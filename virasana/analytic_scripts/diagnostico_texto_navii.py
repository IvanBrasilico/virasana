# coding: utf-8
"""
Diagnóstico para a BUSCA POR TEXTO do NAVI-I (Meilisearch; plano aprovado em 18/10/2026 — `navii/CONTEXTO.md`,
"Busca por texto") — fase 0, SOMENTE LEITURA, antes de instalar qualquer coisa:

  1. CEs POR MÊS de criação: a faixa de IDs de cada mês e, nela, os de exportação (`tipoTrafego` 7, fora do índice);
  2. AMOSTRA: blocos de IDs espalhados pelos CEs dos últimos `--meses-campos` meses (lacunas, exportação, leitura);
  3. CAMPOS candidatos (amostra, importação): preenchidos, tamanho médio e máximo;
  4. NOTIFY (amostra): quantos têm e-mail, telefone, palavras de endereço; exemplos;
  5. EMBARCADOR, CONSIGNATÁRIO e CNPJ DO NOTIFY (amostra): CNPJ (empresa BR) x nome (estrangeira) e, dos CNPJs,
     quantos têm nome em `laudo_empresas` — o índice guarda os nomes;
  6. ESCALAS e CARDS (amostra): escalas por CE, com escala no porto, SEM escala (sem data de referência), dias entre a
     criação do CE e a previsão de atracação de referência; contêineres (cards) por CE e CEs sem contêiner (o card do
     NAVI-I é o contêiner: CE sem contêiner não aparece);
  7. TAMANHO estimado do índice (3 e 12 meses) e tempo de LEITURA da carga inicial — ordens de grandeza.

Sem varredura da tabela. `conhecimentosresumo.create_date` é a data da ÚLTIMA ALTERAÇÃO (ON UPDATE no banco; ver
`risco_historico/CONTEXTO.md`), não a da criação: a 1ª versão deste script delimitou a janela por ela, pegou um CE
antigo alterado há pouco (ID 842.381) e agrupou a tabela quase inteira por mais de uma hora, até cair a conexão
(produção, 18/10/2026). Agora:
  - a fronteira de cada data é um ID, achado por busca binária na chave primária (`id_na_data`): a criação de um ID
    é a menor `create_date` do bloco de IDs a partir dele (o CE alterado tem data nova; os vizinhos, não);
  - CEs por mês = diferença entre as fronteiras (inclusão = linha nova; a amostra mede as lacunas de ID); exportação
    contada no índice de `tipoTrafego` (que leva o ID) dentro da faixa;
  - as seções 3 a 6 usam a AMOSTRA, lida pela chave primária;
  - `--limite` (padrão 120 s) vira o `max_statement_time` de cada conexão no MariaDB: a consulta mais longa é
    interrompida pelo servidor, a seção registra o erro e o diagnóstico segue.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    python virasana/analytic_scripts/diagnostico_texto_navii.py --saida texto_navii.log
    python virasana/analytic_scripts/diagnostico_texto_navii.py --meses 12 --meses-campos 3 --amostra 20000
"""
import argparse
import math
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.diagnostico_fichas_repressao import pct  # noqa: E402
from virasana.analytic_scripts.diagnostico_risco_perdimentos import Diagnostico  # noqa: E402
from virasana.integracao.mercante.mercantealchemy import (  # noqa: E402
    Conhecimento, Escala, EscalaManifesto, Item,
)
from virasana.navii import texto_navii  # noqa: E402
from virasana.navii.busca_navii import Empresa  # noqa: E402  (bhadrasana.models.laudo, como o NAVI-I)
from virasana.navii.mercante_por_id import (  # noqa: E402
    BLOCO_DATA, _momento, criacao_do_id, id_na_data, limita_tempo,
)

TIPO_TRAFEGO_EXPORTACAO = '7'
CAMPOS = ('embarcador', 'consignatario', 'notify', 'cnpjNotify', 'descricao', 'numConhecimento', 'numeroCEMaster',
          'numeroCEmercante', 'codigoEmpresaNavegacao', 'portoOrigemCarga', 'portoDestFinal')
COLUNAS_AMOSTRA = [Conhecimento.ID, Conhecimento.manifestoCE, Conhecimento.create_date, Conhecimento.tipoTrafego] + [
    getattr(Conhecimento, nome) for nome in CAMPOS]
EMAIL = re.compile(r'[\w.+-]+@([\w-]+(?:\.[\w-]+)+)')
TELEFONE = re.compile(r'(?:\bTEL|\bFONE|\bPHONE|\bFAX|\+\d{1,3}[\s.-]?\(?\d)|\d{4,5}[\s.-]\d{4}\b', re.IGNORECASE)
ENDERECO = re.compile(r'\b(?:RUA|AV|AVENIDA|ROAD|RD|STREET|ST|NO|SUITE|FLOOR|FLR|BUILDING|BLDG|DISTRICT|CEP|ZIP|'
                      r'ROOM|TOWER|ESTRADA|ROD|ROD\.)\b', re.IGNORECASE)
CNPJ = re.compile(r'^\d{14}$')
LOTE = 500
BLOCO_AMOSTRA = 500              # IDs seguidos em cada bloco da amostra (~1 hora de CEs)
LIMITE_CONSULTA = 120            # s: max_statement_time de cada conexão no MariaDB
FATOR_INDICE = (3, 8)            # o índice ocupa de 3 a 8 vezes o texto bruto (ordem de grandeza)
EXEMPLO = 150                    # caracteres de cada exemplo de notify


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _data(texto) -> Optional[datetime]:
    try:
        return datetime.strptime(_texto(texto)[:10], '%Y-%m-%d')
    except ValueError:
        return None


def percentis(valores: List[float]) -> str:
    if not valores:
        return '-'
    valores = sorted(valores)

    def p(fracao):
        return valores[min(len(valores) - 1, int(round(fracao * (len(valores) - 1))))]
    return f'p10 {p(.1):.0f}, mediana {statistics.median(valores):.0f}, p90 {p(.9):.0f}'


class DiagnosticoTexto(Diagnostico):
    """Lê a base do Mercante (CEs, escalas, itens, empresas) pela sessão e escreve as seções."""

    SECOES = ('secao_meses', 'secao_amostra', 'secao_campos', 'secao_notify', 'secao_empresas',
              'secao_escalas_e_cards', 'secao_tamanho')

    def __init__(self, session, saida, meses: int = 12, meses_campos: int = 3, amostra: int = 20000,
                 porto: str = 'BRSSZ', agora: Optional[datetime] = None, bloco_data: int = BLOCO_DATA,
                 bloco_amostra: int = BLOCO_AMOSTRA, progresso: Optional[Callable[[str], None]] = None):
        self.session, self.saida = session, saida               # sem a conexão do Diagnostico (só a saída)
        self.meses, self.meses_campos, self.amostra, self.porto = meses, meses_campos, amostra, porto
        self.agora = agora or datetime.now()
        self.bloco_data, self.bloco_amostra = bloco_data, bloco_amostra
        self.progresso = progresso or (lambda texto: None)
        self.por_mes: Dict[str, Tuple[int, int]] = {}       # mês -> (IDs, exportação)
        self.linhas: List = []                              # a amostra (com a exportação)
        self.criacao: Dict[int, Optional[datetime]] = {}    # ID da amostra -> criação estimada (a do bloco)
        self.ids_lidos = 0
        self.segundos_amostra = 0.0
        self.tamanho_medio = 0.0
        self._ids: Optional[Tuple] = None

    def _desde(self, meses: int) -> datetime:
        return self.agora - timedelta(days=round(meses * 30.44))

    def _limites(self) -> Tuple[Optional[int], Optional[int]]:
        if self._ids is None:
            self._ids = tuple(self.session.execute(select(func.min(Conhecimento.ID), func.max(Conhecimento.ID))).one())
        return self._ids

    # ---------------------------------------------------------------- fronteiras por ID
    def criacao_do_id(self, id_: int) -> Optional[datetime]:
        """Criação estimada do ID (`mercante_por_id.criacao_do_id`, bloco de `bloco_data` IDs)."""
        return criacao_do_id(self.session, id_, self.bloco_data)

    def id_na_data(self, data: datetime, a_partir: Optional[int] = None) -> Optional[int]:
        """O 1º ID criado a partir de `data` (`mercante_por_id.id_na_data`, busca binária na chave primária)."""
        return id_na_data(self.session, data, self.bloco_data, a_partir, self._limites())

    # ---------------------------------------------------------------- 1. por mês
    def _inicios_dos_meses(self) -> List[datetime]:
        ano, mes, inicios = self.agora.year, self.agora.month, []
        for _ in range(self.meses):
            inicios.append(datetime(ano, mes, 1))
            ano, mes = (ano, mes - 1) if mes > 1 else (ano - 1, 12)
        return inicios[::-1]

    def _exportacao(self, de: int, ate: int) -> int:
        return int(self.session.execute(select(func.count()).select_from(Conhecimento).where(
            Conhecimento.tipoTrafego == TIPO_TRAFEGO_EXPORTACAO, Conhecimento.ID.between(de, ate))).scalar() or 0)

    def secao_meses(self):
        self.titulo(f'1. CEs POR MÊS DE CRIAÇÃO (últimos {self.meses} meses, pela faixa de IDs)')
        menor, maior = self._limites()
        if menor is None:
            self.escreve('Nenhum CE.')
            return
        inicios, fronteiras = self._inicios_dos_meses(), []
        for inicio in inicios:
            fronteiras.append(self.id_na_data(inicio, fronteiras[-1] if fronteiras else None))
        fronteiras.append(maior + 1)
        linhas = []
        for n, inicio in enumerate(inicios):
            de, ate = fronteiras[n], fronteiras[n + 1] - 1
            ids = max(ate - de + 1, 0)
            exportacao = self._exportacao(de, ate) if ids else 0
            self.por_mes[f'{inicio:%Y-%m}'] = (ids, exportacao)
            linhas.append((f'{inicio:%Y-%m}', f'{de} a {ate}' if ids else '-', ids, exportacao, ids - exportacao))
        self.tabela(['mês', 'IDs', 'CEs (IDs)', 'exportação', 'importação (índice)'], linhas)
        self.escreve('O último mês está incompleto. CEs = IDs da faixa (veja as lacunas na seção 2).')

    # ---------------------------------------------------------------- 2. amostra
    def _blocos(self, inicio: int, maior: int) -> Tuple[List[int], int]:
        """Inícios dos blocos, espalhados de `inicio` até `maior` (o 1º começa no início e o último termina no fim,
        sem sobreposição), e o tamanho de cada um."""
        faixa = maior - inicio + 1
        quantos = max(1, min(math.ceil(self.amostra / self.bloco_amostra), faixa))
        tamanho = max(1, min(self.bloco_amostra, faixa // quantos))
        if quantos == 1:
            return [inicio], tamanho
        passo = (faixa - tamanho) / (quantos - 1)
        return [inicio + round(k * passo) for k in range(quantos)], tamanho

    def secao_amostra(self):
        desde = self._desde(self.meses_campos)
        self.titulo(f'2. AMOSTRA: blocos de IDs dos CEs criados desde {desde:%d/%m/%Y} ({self.meses_campos} meses)')
        inicio, (_, maior) = self.id_na_data(desde), self._limites()
        if inicio is None or inicio > maior:
            self.escreve('Nenhum CE no período.')
            return
        blocos, tamanho = self._blocos(inicio, maior)
        relogio = time.perf_counter()
        for de in blocos:
            linhas = self.session.execute(select(*COLUNAS_AMOSTRA).where(
                Conhecimento.ID.between(de, de + tamanho - 1))).all()
            datas = [_momento(linha.create_date) for linha in linhas if linha.create_date is not None]
            criacao = min(datas) if datas else None             # a do bloco: vale também para o CE alterado
            self.criacao.update((linha.ID, criacao) for linha in linhas)
            self.linhas.extend(linhas)
            self.ids_lidos += tamanho
        self.segundos_amostra = time.perf_counter() - relogio
        exportacao = len(self.linhas) - len(self.importacao())
        self.escreve(f'Faixa: IDs {inicio} a {maior} ({maior - inicio + 1} IDs); {len(blocos)} blocos de {tamanho} IDs')
        self.escreve(f'CEs lidos: {len(self.linhas)} de {self.ids_lidos} IDs '
                     f'(lacunas de ID: {pct(self.ids_lidos - len(self.linhas), self.ids_lidos)})')
        self.escreve(f'Exportação: {pct(exportacao, len(self.linhas))} (fora do índice); importação: '
                     f'{len(self.importacao())}')
        self.escreve(f'Leitura: {self.segundos_amostra:.1f} s ({self._taxa():,.0f} CEs/s, blocos espalhados)')

    def importacao(self) -> List:
        return [linha for linha in self.linhas if _texto(linha.tipoTrafego) != TIPO_TRAFEGO_EXPORTACAO]

    def _taxa(self) -> float:
        return len(self.linhas) / self.segundos_amostra if self.segundos_amostra else 0.0

    # ---------------------------------------------------------------- 3. campos
    def secao_campos(self):
        ces = self.importacao()
        self.titulo(f'3. CAMPOS CANDIDATOS (amostra de {len(ces)} CEs de importação)')
        if not ces:
            self.escreve('Sem amostra (veja a seção 2).')
            return
        linhas, soma = [], 0.0
        for nome in CAMPOS:
            tamanhos = [len(_texto(getattr(ce, nome))) for ce in ces]
            preenchidos = [t for t in tamanhos if t]
            soma += sum(tamanhos) / len(ces)
            linhas.append((nome, pct(len(preenchidos), len(ces)),
                           f'{statistics.mean(preenchidos):.0f}' if preenchidos else '-', max(tamanhos)))
        self.tamanho_medio = soma
        self.tabela(['campo', 'preenchidos', 'tamanho médio (preenchidos)', 'máximo'], linhas)
        totais = [sum(len(_texto(getattr(ce, nome))) for nome in CAMPOS) for ce in ces]
        self.escreve(f'Texto bruto por CE: média {soma:.0f} caracteres ({percentis(totais)}).')

    # ---------------------------------------------------------------- 4. notify
    def secao_notify(self):
        amostra = [' '.join(_texto(ce.notify).split()) for ce in self.importacao() if _texto(ce.notify)]
        self.titulo(f'4. NOTIFY (amostra: {len(amostra)} CEs de importação com notify)')
        contagem = Counter()
        for texto in amostra:
            contagem['e-mail'] += bool(EMAIL.search(texto))
            contagem['telefone'] += bool(TELEFONE.search(texto))
            contagem['palavra de endereço'] += bool(ENDERECO.search(texto))
        self.tabela(['contém', 'notify'], [(k, pct(n, len(amostra))) for k, n in contagem.most_common()])
        contato = [t for t in amostra if EMAIL.search(t) or TELEFONE.search(t)]
        outros = [t for t in amostra if not (EMAIL.search(t) or TELEFONE.search(t))]
        self.escreve()
        self.escreve('Exemplos (com contato, depois sem):')
        for texto in contato[:5] + outros[:3]:
            self.escreve(f'   {texto[:EXEMPLO]}')

    # ---------------------------------------------------------------- 5. empresas
    def secao_empresas(self):
        ces = self.importacao()
        self.titulo(f'5. EMBARCADOR, CONSIGNATÁRIO E CNPJ DO NOTIFY: CNPJ x nome (amostra de {len(ces)} CEs)')
        linhas = []
        for campo in ('embarcador', 'consignatario', 'cnpjNotify'):
            valores = [_texto(getattr(ce, campo)) for ce in ces if _texto(getattr(ce, campo))]
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

    # ---------------------------------------------------------------- 6. escalas e cards
    def _escalas(self, manifestos: List[str]) -> Dict[str, list]:
        por_manifesto = defaultdict(list)
        for n in range(0, len(manifestos), LOTE):
            for manifesto, porto, data in self.session.execute(select(
                    EscalaManifesto.manifesto, Escala.portoDaEscala, Escala.dataPrevistaAtracacao).join(
                    Escala, Escala.numeroDaEscala == EscalaManifesto.escala).where(
                    EscalaManifesto.manifesto.in_(manifestos[n:n + LOTE]))):
                por_manifesto[_texto(manifesto)].append((_texto(porto), _data(data)))
        return por_manifesto

    def _conteineres(self, ces: List[str]) -> Counter:
        por_ce = Counter()
        for n in range(0, len(ces), LOTE):
            for ce, quantos in self.session.execute(select(Item.numeroCEmercante, func.count()).where(
                    Item.numeroCEmercante.in_(ces[n:n + LOTE]),
                    func.length(func.trim(func.coalesce(Item.codigoConteiner, ''))) > 0).group_by(
                    Item.numeroCEmercante)):
                por_ce[_texto(ce)] = quantos
        return por_ce

    def referencia(self, escalas: list) -> Optional[datetime]:
        """A previsão de atracação de referência (a mesma regra do índice: `texto_navii.referencia`)."""
        return texto_navii.referencia(escalas, self.porto)

    @staticmethod
    def faixa_de_cards(quantos: int) -> str:
        if quantos <= 1:
            return '0 (sem contêiner)' if quantos == 0 else '1'
        return '2 a 5' if quantos <= 5 else 'mais de 5'

    def secao_escalas_e_cards(self):
        ces = self.importacao()
        self.titulo(f'6. ESCALAS (data de referência) E CARDS (amostra de {len(ces)} CEs)')
        if not ces:
            self.escreve('Sem amostra (veja a seção 2).')
            return
        escalas = self._escalas(sorted({_texto(ce.manifestoCE) for ce in ces if _texto(ce.manifestoCE)}))
        conteineres = self._conteineres(sorted({_texto(ce.numeroCEmercante) for ce in ces}))
        n_escalas, dias, faixas, contagem = Counter(), [], Counter(), Counter()
        for ce in ces:
            lista = escalas.get(_texto(ce.manifestoCE), [])
            n_escalas['4 ou mais' if len(lista) >= 4 else str(len(lista))] += 1
            contagem['no porto'] += any(porto == self.porto for porto, _ in lista)
            ref, criado = self.referencia(lista), self.criacao.get(ce.ID)
            contagem['sem referência'] += ref is None
            if ref is not None and criado is not None:
                dias.append((ref - criado.replace(hour=0, minute=0, second=0, microsecond=0)).days)
            faixas[self.faixa_de_cards(conteineres.get(_texto(ce.numeroCEmercante), 0))] += 1
        total = len(ces)
        self.tabela(['escalas do manifesto', 'CEs'], [(k, pct(n, total)) for k, n in sorted(n_escalas.items())])
        self.escreve(f'Com escala em {self.porto}: {pct(contagem["no porto"], total)}; SEM data de referência (sem '
                     f'escala com data): {pct(contagem["sem referência"], total)}')
        self.escreve(f'Dias da criação do CE até a previsão de referência: {percentis(dias)} '
                     f'(negativo: o CE foi criado depois da previsão)')
        self.escreve()
        self.tabela(['contêineres (cards) por CE', 'CEs'], [(k, pct(n, total)) for k, n in faixas.most_common()])

    # ---------------------------------------------------------------- 7. tamanho e carga
    def secao_tamanho(self):
        self.titulo('7. TAMANHO ESTIMADO DO ÍNDICE E LEITURA DA CARGA INICIAL')
        completos = [valores for _, valores in sorted(self.por_mes.items())[:-1]][-3:]     # sem o mês em curso
        if not completos or not self.tamanho_medio:
            self.escreve('Sem dados suficientes (rode as seções 1 a 3).')
            return
        importacao = sum(ids - exportacao for ids, exportacao in completos) / len(completos)
        ids_por_mes = sum(ids for ids, _ in completos) / len(completos)
        bruto = self.tamanho_medio + 120                                        # + nomes, datas e chaves
        for meses in (3, 12):
            documentos = importacao * meses
            mb = documentos * bruto / 1024 / 1024
            self.escreve(f'{meses} meses: ~{documentos:,.0f} CEs, texto bruto ~{mb:,.0f} MB, índice ~'
                         f'{mb * FATOR_INDICE[0]:,.0f} a {mb * FATOR_INDICE[1]:,.0f} MB')
        self.escreve(f'Ritmo: ~{importacao / 30.44:,.0f} CEs de importação novos por dia (a carga contínua).')
        if self._taxa():
            minutos = ', '.join(f'{meses} meses ~{ids_por_mes * meses / self._taxa() / 60:,.0f} min'
                                for meses in (3, 12))
            self.escreve(f'Leitura da carga inicial (todos os IDs, na taxa da amostra): {minutos} — sem contar a '
                         f'indexação no Meilisearch.')

    def roda(self):
        self.escreve(f'Diagnóstico da busca por texto — {self.agora:%Y-%m-%d %H:%M:%S}')
        for nome in self.SECOES:
            self.progresso(f'{datetime.now():%H:%M:%S} {nome}')
            inicio = time.perf_counter()
            try:
                getattr(self, nome)()
            except DBAPIError as erro:      # consulta interrompida (--limite) ou conexão perdida: segue
                self.session.rollback()
                self.escreve(f'!! ERRO na seção (o diagnóstico segue): {getattr(erro, "orig", erro)}')
            self.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
            self.saida.flush()


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--meses', type=int, default=12, help='CEs por mês dos últimos N meses (padrão 12)')
    parser.add_argument('--meses-campos', type=int, default=3, help='janela da amostra (padrão 3)')
    parser.add_argument('--amostra', type=int, default=20000, help='IDs lidos na amostra (padrão 20000)')
    parser.add_argument('--porto', default='BRSSZ', help='porto da data de referência (padrão BRSSZ)')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    parser.add_argument('--saida', default=datetime.now().strftime('texto_navii_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    engine = create_engine(uri, pool_pre_ping=True)
    limitado = limita_tempo(engine, args.limite)
    with Session(engine) as session, open(args.saida, 'w', encoding='utf-8') as saida:
        saida.write(f'Banco: {engine.url.render_as_string(hide_password=True)}; limite por consulta: '
                    f'{f"{args.limite:.0f} s" if limitado else "nenhum"}\n')
        DiagnosticoTexto(session, saida, args.meses, args.meses_campos, args.amostra, args.porto,
                         progresso=lambda texto: print(texto, flush=True)).roda()
    print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
