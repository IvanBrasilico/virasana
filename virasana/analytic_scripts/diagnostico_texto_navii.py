# coding: utf-8
"""
Diagnóstico para a BUSCA POR TEXTO do NAVI-I (Meilisearch; plano aprovado em 18/10/2026 — `navii/CONTEXTO.md`,
"Busca por texto") — fase 0, SOMENTE LEITURA, antes de instalar qualquer coisa:

  1. CEs POR MÊS (`create_date`): total, exportação (`tipoTrafego` 7, fora do índice) e importação (o índice);
  2. CAMPOS candidatos (CEs de importação dos últimos `--meses-campos` meses): preenchidos, tamanho médio e máximo;
  3. NOTIFY (amostra dos mais recentes): quantos têm e-mail, telefone, palavras de endereço; exemplos MASCARADOS;
  4. EMBARCADOR e CONSIGNATÁRIO (amostra): CNPJ (empresa BR) x nome (estrangeira) e, dos CNPJs, quantos têm nome em
     `laudo_empresas` — o índice guarda os nomes;
  5. ESCALAS e CARDS (amostra): escalas por CE, com escala no porto, SEM escala (sem data de referência), dias entre a
     criação do CE e a previsão de atracação de referência; contêineres (cards) por CE e CEs sem contêiner (o card do
     NAVI-I é o contêiner: CE sem contêiner não aparece);
  6. TAMANHO estimado do índice (3 e 12 meses) — estimativa grosseira (o índice do Meilisearch ocupa algumas vezes o
     texto bruto).

A janela é delimitada pelo `ID` (chave primária) a partir do 1º CE com `create_date` na data (o `create_date` tem
índice). Não imprime CPF/CNPJ nem e-mail completos: dígitos viram '#', e-mail perde a parte antes do @.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    python virasana/analytic_scripts/diagnostico_texto_navii.py --saida texto_navii.log
    python virasana/analytic_scripts/diagnostico_texto_navii.py --meses 12 --meses-campos 3 --amostra 20000
"""
import argparse
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import String, case, cast, create_engine, func, or_, select
from sqlalchemy.orm import Session

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.diagnostico_fichas_repressao import mascara_texto, pct  # noqa: E402
from virasana.analytic_scripts.diagnostico_risco_perdimentos import Diagnostico  # noqa: E402
from virasana.integracao.mercante.mercantealchemy import (  # noqa: E402
    Conhecimento, Escala, EscalaManifesto, Item,
)
from virasana.navii.busca_navii import Empresa  # noqa: E402  (bhadrasana.models.laudo, como o NAVI-I)

TIPO_TRAFEGO_EXPORTACAO = '7'
CAMPOS = ('embarcador', 'consignatario', 'notify', 'cnpjNotify', 'descricao', 'numConhecimento', 'numeroCEMaster',
          'numeroCEmercante', 'codigoEmpresaNavegacao', 'portoOrigemCarga', 'portoDestFinal')
EMAIL = re.compile(r'[\w.+-]+@([\w-]+(?:\.[\w-]+)+)')
TELEFONE = re.compile(r'(?:\bTEL|\bFONE|\bPHONE|\bFAX|\+\d{1,3}[\s.-]?\(?\d)|\d{4,5}[\s.-]\d{4}\b', re.IGNORECASE)
ENDERECO = re.compile(r'\b(?:RUA|AV|AVENIDA|ROAD|RD|STREET|ST|NO|SUITE|FLOOR|FLR|BUILDING|BLDG|DISTRICT|CEP|ZIP|'
                      r'ROOM|TOWER|ESTRADA|ROD|ROD\.)\b', re.IGNORECASE)
CNPJ = re.compile(r'^\d{14}$')
LOTE = 500
FATOR_INDICE = (3, 8)            # o índice ocupa de 3 a 8 vezes o texto bruto (ordem de grandeza)


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


PALAVRAS_DO_FORMATO = {'TEL', 'FONE', 'PHONE', 'FAX', 'EMAIL', 'MAIL', 'RUA', 'AV', 'AVENIDA', 'ROAD', 'RD', 'STREET',
                       'ST', 'NO', 'SUITE', 'FLOOR', 'BUILDING', 'DISTRICT', 'CEP', 'ZIP', 'ROOM', 'TOWER', 'LTD',
                       'LTDA', 'CO', 'INC', 'SA', 'LLC', 'GMBH', 'CORP', 'LIMITED', 'COMPANY'}


def mascara_notify(texto: str) -> str:
    """Notify para exemplo, só com o FORMATO: e-mail sem a parte antes do @ (o domínio fica), dígitos como '#' e as
    palavras com a inicial (nomes de pessoas não são impressos), menos as que mostram o formato (TEL, RUA, LTDA...)."""
    dominios = []

    def guarda(m):
        dominios.append(m.group(1))
        return f'\x00{len(dominios) - 1}\x00'

    texto = EMAIL.sub(guarda, _texto(texto))
    texto = re.sub(r'[^\W\d_]{2,}', lambda m: m.group(0) if m.group(0).upper() in PALAVRAS_DO_FORMATO
                   else m.group(0)[0] + '*' * (len(m.group(0)) - 1), texto)
    texto = re.sub(r'\x00(\d+)\x00', lambda m: '***@' + dominios[int(m.group(1))], texto)
    return mascara_texto(texto, 70)


def percentis(valores: List[float]) -> str:
    if not valores:
        return '-'
    valores = sorted(valores)

    def p(fracao):
        return valores[min(len(valores) - 1, int(round(fracao * (len(valores) - 1))))]
    return f'p10 {p(.1):.0f}, mediana {statistics.median(valores):.0f}, p90 {p(.9):.0f}'


def _data(texto) -> Optional[datetime]:
    try:
        return datetime.strptime(_texto(texto)[:10], '%Y-%m-%d')
    except ValueError:
        return None


def _preenchido(coluna):
    return func.sum(case((func.length(func.trim(func.coalesce(coluna, ''))) > 0, 1), else_=0))


class DiagnosticoTexto(Diagnostico):
    """Lê a base do Mercante (CEs, escalas, itens, empresas) pela sessão e escreve as seções."""

    SECOES = ('secao_meses', 'secao_campos', 'secao_notify', 'secao_empresas', 'secao_escalas_e_cards',
              'secao_tamanho')

    def __init__(self, session, saida, meses: int = 12, meses_campos: int = 3, amostra: int = 20000,
                 porto: str = 'BRSSZ', agora: Optional[datetime] = None):  # noqa: sem a conexão do Diagnostico
        self.session, self.saida = session, saida
        self.meses, self.meses_campos, self.amostra, self.porto = meses, meses_campos, amostra, porto
        self.agora = agora or datetime.now()
        self.por_mes: Dict[str, int] = {}
        self.tamanho_medio = 0.0
        self.recentes: List = []

    def _desde(self, meses: int) -> datetime:
        return self.agora - timedelta(days=round(meses * 30.44))

    def _id_inicial(self, meses: int) -> Optional[int]:
        return self.session.execute(select(func.min(Conhecimento.ID)).where(
            Conhecimento.create_date >= self._desde(meses))).scalar()

    def _importacao(self):
        return or_(Conhecimento.tipoTrafego.is_(None), Conhecimento.tipoTrafego != TIPO_TRAFEGO_EXPORTACAO)

    # ---------------------------------------------------------------- 1. por mês
    def secao_meses(self):
        self.titulo(f'1. CEs POR MÊS DE CRIAÇÃO (últimos {self.meses} meses)')
        mes = func.substr(cast(Conhecimento.create_date, String), 1, 7)
        exportacao = func.sum(case((Conhecimento.tipoTrafego == TIPO_TRAFEGO_EXPORTACAO, 1), else_=0))
        id_inicial = self._id_inicial(self.meses)
        if id_inicial is None:
            self.escreve('Nenhum CE no período.')
            return
        linhas = self.session.execute(select(mes, func.count(), exportacao).where(
            Conhecimento.ID >= id_inicial).group_by(mes).order_by(mes)).all()
        self.por_mes = {m: total - (exp or 0) for m, total, exp in linhas}
        self.tabela(['mês', 'CEs', 'exportação', 'importação (índice)'],
                    [(m, total, exp or 0, total - (exp or 0)) for m, total, exp in linhas])

    # ---------------------------------------------------------------- 2. campos
    def secao_campos(self):
        self.titulo(f'2. CAMPOS CANDIDATOS (CEs de importação dos últimos {self.meses_campos} meses)')
        id_inicial = self._id_inicial(self.meses_campos)
        if id_inicial is None:
            self.escreve('Nenhum CE no período.')
            return
        colunas = [getattr(Conhecimento, nome) for nome in CAMPOS]
        expressoes = [func.count()]
        for coluna in colunas:
            expressoes += [_preenchido(coluna), func.avg(func.length(coluna)), func.max(func.length(coluna))]
        consulta = select(*expressoes).where(Conhecimento.ID >= id_inicial, self._importacao())
        linha = self.session.execute(consulta).one()
        total, linhas, soma_medias = linha[0], [], 0.0
        for n, nome in enumerate(CAMPOS):
            preenchidos, media, maximo = linha[1 + 3 * n: 4 + 3 * n]
            soma_medias += float(media or 0)
            linhas.append((nome, pct(int(preenchidos or 0), total), f'{float(media or 0):.0f}', maximo or 0))
        self.tamanho_medio = soma_medias
        self.escreve(f'CEs de importação na janela: {total}')
        self.tabela(['campo', 'preenchidos', 'tamanho médio', 'máximo'], linhas)
        self.escreve(f'Texto bruto médio por CE (soma das médias): {soma_medias:.0f} caracteres.')

    # ---------------------------------------------------------------- amostra
    def amostra_recente(self) -> List:
        """Os `amostra` CEs de importação mais recentes da janela dos campos (uma consulta, pela chave primária)."""
        if not self.recentes:
            id_inicial = self._id_inicial(self.meses_campos) or 0
            self.recentes = self.session.execute(select(
                Conhecimento.numeroCEmercante, Conhecimento.manifestoCE, Conhecimento.create_date,
                Conhecimento.embarcador, Conhecimento.consignatario, Conhecimento.notify).where(
                Conhecimento.ID >= id_inicial, self._importacao()).order_by(Conhecimento.ID.desc()).limit(
                self.amostra)).all()
        return self.recentes

    # ---------------------------------------------------------------- 3. notify
    def secao_notify(self):
        amostra = [_texto(r.notify) for r in self.amostra_recente() if _texto(r.notify)]
        self.titulo(f'3. NOTIFY (amostra de {len(amostra)} CEs recentes com notify)')
        contagem = Counter()
        for texto in amostra:
            contagem['e-mail'] += bool(EMAIL.search(texto))
            contagem['telefone'] += bool(TELEFONE.search(texto))
            contagem['palavra de endereço'] += bool(ENDERECO.search(texto))
        self.tabela(['contém', 'notify'], [(k, pct(n, len(amostra))) for k, n in contagem.most_common()])
        self.escreve()
        self.escreve('Exemplos (mascarados):')
        for texto in [t for t in amostra if EMAIL.search(t) or TELEFONE.search(t)][:5]:
            self.escreve(f'   {mascara_notify(texto)}')

    # ---------------------------------------------------------------- 4. embarcador e consignatário
    def secao_empresas(self):
        amostra = self.amostra_recente()
        self.titulo(f'4. EMBARCADOR E CONSIGNATÁRIO: CNPJ x nome (amostra de {len(amostra)} CEs)')
        linhas = []
        for campo in ('embarcador', 'consignatario'):
            valores = [_texto(getattr(r, campo)) for r in amostra if _texto(getattr(r, campo))]
            cnpjs = {v for v in valores if CNPJ.match(v)}
            com_nome = self._cnpjs_com_nome(cnpjs)
            de_cnpj = [v for v in valores if v in cnpjs]
            linhas.append((campo, len(valores), pct(len(de_cnpj), len(valores)),
                           pct(sum(1 for v in de_cnpj if v in com_nome), len(de_cnpj)), len(set(valores))))
        self.tabela(['campo', 'preenchidos', 'são CNPJ', 'CNPJ com nome', 'valores distintos'], linhas)

    def _cnpjs_com_nome(self, cnpjs: set) -> set:
        lista, com_nome = sorted(cnpjs), set()
        for n in range(0, len(lista), LOTE):
            com_nome |= {c for (c,) in self.session.execute(select(Empresa.cnpj).where(
                Empresa.cnpj.in_(lista[n:n + LOTE]), func.length(func.trim(func.coalesce(Empresa.nome, ''))) > 0))}
        return com_nome

    # ---------------------------------------------------------------- 5. escalas e cards
    def _escalas(self, manifestos: List[str]) -> Dict[str, list]:
        por_manifesto = defaultdict(list)
        for n in range(0, len(manifestos), LOTE):
            for manifesto, porto, data in self.session.execute(select(
                    EscalaManifesto.manifesto, Escala.portoDaEscala, Escala.dataPrevistaAtracacao).join(
                    Escala, Escala.numeroDaEscala == EscalaManifesto.escala).where(
                    EscalaManifesto.manifesto.in_(manifestos[n:n + LOTE]))):
                por_manifesto[manifesto].append((_texto(porto), _data(data)))
        return por_manifesto

    def _conteineres(self, ces: List[str]) -> Counter:
        por_ce = Counter()
        for n in range(0, len(ces), LOTE):
            for ce, quantos in self.session.execute(select(Item.numeroCEmercante, func.count()).where(
                    Item.numeroCEmercante.in_(ces[n:n + LOTE]),
                    func.length(func.trim(func.coalesce(Item.codigoConteiner, ''))) > 0).group_by(
                    Item.numeroCEmercante)):
                por_ce[ce] = quantos
        return por_ce

    def referencia(self, escalas: list) -> Optional[datetime]:
        """A previsão de atracação de referência: a da escala no porto; sem ela, a primeira (a mais cedo)."""
        no_porto = [d for porto, d in escalas if porto == self.porto and d]
        datas = [d for _, d in escalas if d]
        return min(no_porto) if no_porto else (min(datas) if datas else None)

    def _dias_ate_referencia(self, criacao, referencia: datetime) -> int:
        criado = criacao if isinstance(criacao, datetime) else datetime.fromisoformat(str(criacao)[:19])
        return (referencia - criado.replace(hour=0, minute=0, second=0, microsecond=0)).days

    @staticmethod
    def faixa_de_cards(quantos: int) -> str:
        if quantos <= 1:
            return '0 (sem contêiner)' if quantos == 0 else '1'
        return '2 a 5' if quantos <= 5 else 'mais de 5'

    def secao_escalas_e_cards(self):
        amostra = self.amostra_recente()
        self.titulo(f'5. ESCALAS (data de referência) E CARDS (amostra de {len(amostra)} CEs)')
        escalas = self._escalas(sorted({_texto(r.manifestoCE) for r in amostra if r.manifestoCE}))
        conteineres = self._conteineres(sorted({_texto(r.numeroCEmercante) for r in amostra}))
        n_escalas, dias, faixas, contagem = Counter(), [], Counter(), Counter()
        for r in amostra:
            lista = escalas.get(_texto(r.manifestoCE), [])
            n_escalas['4 ou mais' if len(lista) >= 4 else str(len(lista))] += 1
            contagem['no porto'] += any(porto == self.porto for porto, _ in lista)
            ref = self.referencia(lista)
            contagem['sem referência'] += ref is None
            if ref is not None and r.create_date:
                dias.append(self._dias_ate_referencia(r.create_date, ref))
            faixas[self.faixa_de_cards(conteineres.get(_texto(r.numeroCEmercante), 0))] += 1
        total = len(amostra)
        self.tabela(['escalas do manifesto', 'CEs'], [(k, pct(n, total)) for k, n in sorted(n_escalas.items())])
        self.escreve(f'Com escala em {self.porto}: {pct(contagem["no porto"], total)}; SEM data de referência (sem '
                     f'escala com data): {pct(contagem["sem referência"], total)}')
        self.escreve(f'Dias da criação do CE até a previsão de referência: {percentis(dias)} '
                     f'(negativo: o CE foi criado depois da previsão)')
        self.escreve()
        self.tabela(['contêineres (cards) por CE', 'CEs'], [(k, pct(n, total)) for k, n in faixas.most_common()])

    # ---------------------------------------------------------------- 6. tamanho
    def secao_tamanho(self):
        self.titulo('6. TAMANHO ESTIMADO DO ÍNDICE')
        completos = [n for m, n in sorted(self.por_mes.items())[:-1]][-3:]      # sem o mês em curso
        if not completos or not self.tamanho_medio:
            self.escreve('Sem dados suficientes (rode as seções 1 e 2).')
            return
        por_mes = sum(completos) / len(completos)
        bruto = self.tamanho_medio + 120                                        # + nomes, datas e chaves
        for meses in (3, 12):
            documentos = por_mes * meses
            mb = documentos * bruto / 1024 / 1024
            self.escreve(f'{meses} meses: ~{documentos:,.0f} CEs, texto bruto ~{mb:,.0f} MB, índice ~'
                         f'{mb * FATOR_INDICE[0]:,.0f} a {mb * FATOR_INDICE[1]:,.0f} MB')
        self.escreve(f'Ritmo: ~{por_mes / 30.44:,.0f} CEs novos por dia (a carga contínua).')

    def roda(self):
        self.escreve(f'Diagnóstico da busca por texto — {self.agora:%Y-%m-%d %H:%M:%S}')
        for nome in self.SECOES:
            inicio = time.perf_counter()
            getattr(self, nome)()
            self.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
            self.saida.flush()


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--meses', type=int, default=12, help='CEs por mês dos últimos N meses (padrão 12)')
    parser.add_argument('--meses-campos', type=int, default=3, help='janela dos campos e da amostra (padrão 3)')
    parser.add_argument('--amostra', type=int, default=20000, help='CEs mais recentes na amostra (padrão 20000)')
    parser.add_argument('--porto', default='BRSSZ', help='porto da data de referência (padrão BRSSZ)')
    parser.add_argument('--saida', default=datetime.now().strftime('texto_navii_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    engine = create_engine(uri)
    with Session(engine) as session, open(args.saida, 'w', encoding='utf-8') as saida:
        saida.write(f'Banco: {engine.url.render_as_string(hide_password=True)}\n')
        DiagnosticoTexto(session, saida, args.meses, args.meses_campos, args.amostra, args.porto).roda()
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
