# coding: utf-8
"""
DIAGNÓSTICO da "FINAL CURTA": passagem de minutos por OUTRO operador (quase sempre o ECOPORTO) sem nenhuma entrada
depois — o contêiner "some" da API Recintos. Pedido do usuário (15/10/2026), antes de mudar o ciclo de vida
(operador em papel de TRA). Somente leitura (SQL e, com --mongo, o Mongo).

Na 1ª medição (`diagnostico_operador_tra.py`, operador_tra.log): 191 de 4.000 contêineres; 98% no Ecoporto; 183 em até
30 min; inspeção com o código do Ecoporto em 2%; gravados quase todos como "entregue pelo operador" do desembarque SEM
escaneamento. Nas passagens que seguem para um TRA, o escaneamento do Ecoporto vem com o código do TRA de destino.

Hipóteses e o que as distingue (cada caso é comparado com as PASSAGENS pelos mesmos recintos, que seguem para um
TRA — o controle mostra como os mesmos indicadores ficam quando o destino é conhecido):
  H1 escaneado e a inspeção não transmitida (ou com outro código) -> imagem no Mongo no horário da passagem; inspeção
     com OUTRO código de recinto perto da passagem;
  H2 já desembaraçado no operador e entregue; a passagem é incidental -> declaração (DI/DUIMP) informada na saída do
     operador; devolução do vazio dias depois; o caminhão não entra em outro recinto da API;
  H3 o destino registrou a entrada com o número lido errado (OCR) -> o MESMO caminhão (placa) entra em outro recinto
     logo depois com um número parecido (até 2 caracteres diferentes);
  H4 o destino não transmite à API -> o caminhão some; agendamento (operação G) para um recinto.

Seções: comparação final curta x passagem; MOMENTO da imagem (11b: o horário da imagem menos a defasagem do recinto
— `imagens_ciclo.DEFASAGEM_HORAS` — cai durante a passagem ou na saída do operador de origem? diz de qual escâner
ela veio); declaração informada nos acessos; para onde foi o caminhão; o que veio depois; CEs e como o ciclo foi
gravado; exemplos. A placa só é usada para seguir o caminhão (não vai ao relatório).

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    DIAG=virasana/analytic_scripts/ciclo_de_vida/diagnostico_final_curta.py
    virasana-venv/bin/python $DIAG --conteineres 20000 --mongo --saida final_curta.log
"""
import argparse
import sys
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida import diagnostico_operador_tra as dot  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_imagens_ciclo import distancia  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import defasagens_do, imagens_do_conteiner  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402

JANELA_CAMINHAO_HORAS = 6         # o caminhão entra noutro recinto até N h depois de sair da passagem
INSPECAO_ANTES_HORAS, INSPECAO_DEPOIS_HORAS = 2, 6
IMAGEM_ANTES_HORAS, IMAGEM_DEPOIS_HORAS = 1, 4   # cobre as defasagens de +0 h e +3 h do Mongo
IMAGEM_PERTO_DIAS = 2
DIFERENCAS_PARECIDO = 2
AGENDAMENTO_DIAS = 3
FOLGA_PASSAGEM_MINUTOS = 15                      # imagem (sem a defasagem) da entrada − 15 min à saída + 15 min
ORIGEM_ANTES_MINUTOS, ORIGEM_DEPOIS_MINUTOS = 60, 15   # ... ou perto da saída do operador de origem (escâner no gate)

DURANTE, NA_ORIGEM = 'durante a passagem', 'na saída do operador de origem'
OUTRO_MOMENTO = f'outro momento (entrada − {IMAGEM_ANTES_HORAS} h a saída + {IMAGEM_DEPOIS_HORAS} h)'
SEM_IMAGEM = 'sem imagem no horário'
MOMENTOS = (DURANTE, NA_ORIGEM, OUTRO_MOMENTO, SEM_IMAGEM)

COM_ESTE, PARECIDO, OUTRO, SEM_CONTEINER = ('entrou com este contêiner', 'entrou com número parecido (OCR?)',
                                            'entrou com outro contêiner', 'entrou sem contêiner informado')
NAO_ENTROU, SEM_PLACA = f'não entrou em recinto da API em {JANELA_CAMINHAO_HORAS} h', 'sem placa no acesso'
DESTINOS = (COM_ESTE, PARECIDO, OUTRO, SEM_CONTEINER, NAO_ENTROU, SEM_PLACA)

SQL_ACESSOS = ('SELECT id, placa, tipoDeclaracao, numeroDeclaracao, ocrNumero '
               'FROM apirecintos_acessosveiculo WHERE id IN :ids')
SQL_CAMINHAO = ('SELECT dataHoraOcorrencia, codigoRecinto, direcao, numeroConteiner, vazioConteiner '
                "FROM apirecintos_acessosveiculo WHERE operacao = 'C' AND placa = :placa "
                'AND dataHoraOcorrencia > :depois AND dataHoraOcorrencia <= :ate ORDER BY dataHoraOcorrencia')
SQL_AGENDAMENTOS = ('SELECT numeroConteiner, dataHoraOcorrencia, codigoRecinto, direcao '
                    "FROM apirecintos_acessosveiculo WHERE operacao = 'G' AND numeroConteiner IN :conteineres "
                    'AND dataHoraOcorrencia >= :desde')


def destino_do_caminhao(acessos: List[tuple], conteiner: str, recinto_estada: str) -> Tuple[str, Optional[str]]:
    """Primeira ENTRADA do caminhão noutro recinto (acessos já na janela, em ordem): (classe, recinto)."""
    for _, recinto, direcao, numero, _ in acessos:
        if direcao != 'E' or recinto == recinto_estada:
            continue
        numero = (numero or '').strip()
        if not numero:
            return SEM_CONTEINER, recinto
        if numero == conteiner:
            return COM_ESTE, recinto
        if distancia(numero, conteiner) <= DIFERENCAS_PARECIDO:
            return PARECIDO, recinto
        return OUTRO, recinto
    return NAO_ENTROU, None


def _sem_defasagem(data: datetime, recinto: str) -> List[datetime]:
    return [data - timedelta(hours=horas) for horas in defasagens_do(recinto)]


def momento_da_imagem(imagens: List[datetime], estada: dot.Estada,
                      origem: dot.Estada) -> Tuple[str, Optional[float]]:
    """De qual momento é a imagem do contêiner (horário menos a defasagem do recinto): durante a passagem, na saída
    do operador de origem, outro momento no horário ou nenhuma. Com DURANTE, também os minutos depois da entrada."""
    fim = estada.saida or estada.entrada
    folga = timedelta(minutes=FOLGA_PASSAGEM_MINUTOS)
    minutos = [(u - estada.entrada).total_seconds() / 60 for t in imagens for u in _sem_defasagem(t, estada.recinto)
               if estada.entrada - folga <= u <= fim + folga]
    if minutos:
        return DURANTE, min(minutos, key=abs)
    if origem.saida and any(origem.saida - timedelta(minutes=ORIGEM_ANTES_MINUTOS) <= u
                            <= origem.saida + timedelta(minutes=ORIGEM_DEPOIS_MINUTOS)
                            for t in imagens for u in _sem_defasagem(t, origem.recinto)):
        return NA_ORIGEM, None
    inicio, ate = estada.entrada - timedelta(hours=IMAGEM_ANTES_HORAS), fim + timedelta(hours=IMAGEM_DEPOIS_HORAS)
    return (OUTRO_MOMENTO if any(inicio <= t <= ate for t in imagens) else SEM_IMAGEM), None


@dataclass
class Caso:
    caminho: dot.Percurso
    indice: int
    estada: dot.Estada
    classe: str
    indicadores: Dict[str, bool] = field(default_factory=dict)
    destino: Tuple[str, Optional[str]] = (SEM_PLACA, None)
    declaracoes: Dict[str, str] = field(default_factory=dict)   # 'origem', 'entrada', 'saida' -> tipo
    agendamentos: List[str] = field(default_factory=list)       # recintos com agendamento (G) perto da passagem
    imagem: Tuple[Optional[str], Optional[float]] = (None, None)  # momento da imagem e minutos depois da entrada

    @property
    def origem(self) -> dot.Estada:
        return self.caminho.estadas[0]

    @property
    def conteiner(self) -> str:
        return self.caminho.conteiner


class DiagnosticoFinalCurta(dot.DiagnosticoOperadorTra):

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, agora: Optional[datetime] = None,
                 amostras: int = 8, conteineres: int = 20000, mongo_db=None):
        super().__init__(conexao, saida, inicio, fim, agora, amostras, conteineres)
        self.mongo_db = mongo_db
        self.curtas: List[Caso] = []
        self.controle: List[Caso] = []
        self.acessos: Dict[int, tuple] = {}

    # ---------------------------------------------------------------- casos e indicadores
    def separa_casos(self):
        """Final curta em outro operador e, como controle, passagens pelos mesmos recintos (mesmo tamanho)."""
        curtas, passagens = [], []
        for caminho, indice, estada, nome_classe in self.estadas(operador=True):
            if nome_classe == dot.FINAL_CURTA:
                curtas.append(Caso(caminho, indice, estada, nome_classe))
            elif nome_classe == dot.PASSAGEM:
                passagens.append(Caso(caminho, indice, estada, nome_classe))
        recintos = {c.estada.recinto for c in curtas}
        passagens = sorted((c for c in passagens if c.estada.recinto in recintos),
                           key=lambda c: zlib.crc32(c.conteiner.encode()))
        self.curtas, self.controle = curtas, passagens[:max(len(curtas), 1)]

    def _le_acessos(self, casos: List[Caso]):
        ids = {i for c in casos for i in (c.origem.saida_id, c.estada.entrada_id, c.estada.saida_id) if i}
        self.acessos = {linha[0]: linha[1:] for linha in self._por_lotes(SQL_ACESSOS, 'ids', ids)}

    def _declaracao(self, id_: Optional[int]) -> str:
        _, tipo, numero, _ = self.acessos.get(id_, (None, None, None, None))
        return (tipo or '').strip() or ('(número sem tipo)' if (numero or '').strip() else '(nenhuma)')

    def _sem_ocr(self, id_: Optional[int]) -> bool:
        """Número do contêiner digitado (ocrNumero falso) no acesso; sem o acesso, False."""
        linha = self.acessos.get(id_)
        return linha is not None and linha[3] is not None and not linha[3]

    def _placa(self, *ids) -> Optional[str]:
        for id_ in ids:
            placa = (self.acessos.get(id_, (None,))[0] or '').strip()
            if placa:
                return placa
        return None

    def _inspecoes(self, caso: Caso) -> Tuple[bool, bool]:
        """(inspeção com o código do recinto da estada, inspeção com OUTRO código) perto da passagem."""
        inicio = caso.estada.entrada - timedelta(hours=INSPECAO_ANTES_HORAS)
        fim = (caso.estada.saida or caso.estada.entrada) + timedelta(hours=INSPECAO_DEPOIS_HORAS)
        perto = [e for e in self.eventos.get(caso.conteiner, []) if e.tipo == ldt.INSPECAO and inicio <= e.data <= fim]
        return (any(e.recinto == caso.estada.recinto for e in perto),
                any(e.recinto != caso.estada.recinto for e in perto))

    def _pesagem(self, caso: Caso) -> bool:
        fim = caso.estada.saida or caso.estada.entrada
        return any(e.tipo == ldt.PESAGEM and e.recinto == caso.estada.recinto and caso.estada.entrada <= e.data <= fim
                   for e in self.eventos.get(caso.conteiner, []))

    def _imagens(self, caso: Caso) -> Dict[str, bool]:
        if self.mongo_db is None:
            return {}
        fim = caso.estada.saida or caso.estada.entrada
        perto = imagens_do_conteiner(self.mongo_db, caso.conteiner,
                                     caso.estada.entrada - timedelta(days=IMAGEM_PERTO_DIAS),
                                     fim + timedelta(days=IMAGEM_PERTO_DIAS))
        inicio = caso.estada.entrada - timedelta(hours=IMAGEM_ANTES_HORAS)
        ate = fim + timedelta(hours=IMAGEM_DEPOIS_HORAS)
        caso.imagem = momento_da_imagem([data for data, _ in perto], caso.estada, caso.origem)
        return {'imagem no Mongo no horário da passagem': any(inicio <= data <= ate for data, _ in perto),
                f'imagem do contêiner em ±{IMAGEM_PERTO_DIAS} dias': bool(perto)}

    def _caminhao(self, caso: Caso) -> Tuple[str, Optional[str]]:
        placa = self._placa(caso.estada.saida_id, caso.estada.entrada_id)
        if not placa or caso.estada.saida is None:
            return SEM_PLACA, None
        acessos = [(_dt(d), r, direcao, n, v) for d, r, direcao, n, v in self.consulta(SQL_CAMINHAO, {
            'placa': placa, 'depois': caso.estada.saida,
            'ate': caso.estada.saida + timedelta(hours=JANELA_CAMINHAO_HORAS)})]
        return destino_do_caminhao(acessos, caso.conteiner, caso.estada.recinto)

    def _agendamentos(self, casos: List[Caso]):
        desde = self.periodo['inicio'] - timedelta(days=AGENDAMENTO_DIAS)
        por_conteiner = defaultdict(list)
        for conteiner, data, recinto, direcao in self._por_lotes(SQL_AGENDAMENTOS, 'conteineres',
                                                                 {c.conteiner for c in casos}, {'desde': desde}):
            por_conteiner[conteiner].append((_dt(data), recinto, direcao))
        for caso in casos:
            inicio = caso.estada.entrada - timedelta(days=AGENDAMENTO_DIAS)
            fim = (caso.estada.saida or caso.estada.entrada) + timedelta(days=AGENDAMENTO_DIAS)
            caso.agendamentos = sorted({r for d, r, direcao in por_conteiner.get(caso.conteiner, [])
                                        if inicio <= d <= fim and r not in (caso.origem.recinto, caso.estada.recinto)})

    def calcula(self):
        casos = self.curtas + self.controle
        self._le_acessos(casos)
        self._agendamentos(casos)
        for caso in casos:
            no_recinto, outro_codigo = self._inspecoes(caso)
            placa_origem = self._placa(caso.origem.saida_id)
            caso.indicadores = {
                'estada em até 30 min': (caso.estada.horas() or 0) <= 0.5,
                'inspeção na origem antes de sair': bool(caso.origem.inspecoes),
                'inspeção com o código do recinto da estada': no_recinto,
                f'inspeção com OUTRO código (−{INSPECAO_ANTES_HORAS} h a +{INSPECAO_DEPOIS_HORAS} h)': outro_codigo,
                'pesagem no recinto da estada': self._pesagem(caso),
                **self._imagens(caso),
                'número digitado na entrada (sem OCR)': self._sem_ocr(caso.estada.entrada_id),
                'mesmo caminhão da saída da origem': bool(placa_origem and placa_origem == self._placa(
                    caso.estada.entrada_id)),
                'agendamento (G) noutro recinto': bool(caso.agendamentos),
            }
            caso.declaracoes = {'origem': self._declaracao(caso.origem.saida_id),
                                'entrada': self._declaracao(caso.estada.entrada_id),
                                'saida': self._declaracao(caso.estada.saida_id)}
            caso.destino = self._caminhao(caso)

    # ---------------------------------------------------------------- seções
    def secao_comparacao(self):
        self.titulo('11. FINAL CURTA x PASSAGEM pelos mesmos recintos (controle: as que seguem para um TRA)')
        recintos = Counter(c.estada.recinto for c in self.curtas)
        self.escreve('Final curta por recinto: ' + ', '.join(f'{self.nome(r)} {n}' for r, n in recintos.most_common()))
        if not self.curtas:
            return
        nomes = list(self.curtas[0].indicadores)

        def celula(casos, chave):
            n = sum(1 for c in casos if c.indicadores.get(chave))
            return f'{n} ({dot._pct(n, len(casos))})'

        def destino(casos, nome):
            n = sum(1 for c in casos if c.destino[0] == nome)
            return f'{n} ({dot._pct(n, len(casos))})'

        linhas = [['casos', len(self.curtas), len(self.controle)]]
        linhas += [[nome, celula(self.curtas, nome), celula(self.controle, nome)] for nome in nomes]
        linhas += [[f'caminhão: {nome}', destino(self.curtas, nome), destino(self.controle, nome)] for nome in DESTINOS]
        self.tabela(['indicador', 'final curta', 'passagem (controle)'], linhas)

    def secao_momento_imagem(self):
        self.titulo('11b. MOMENTO DA IMAGEM no Mongo (horário menos a defasagem do recinto): de qual escâner ela veio?')
        if self.mongo_db is None:
            self.escreve('(sem --mongo)')
            return
        self.escreve(f'"durante": da entrada − {FOLGA_PASSAGEM_MINUTOS} min à saída + {FOLGA_PASSAGEM_MINUTOS} min no '
                     f'recinto da passagem; "na saída do operador de origem": de {ORIGEM_ANTES_MINUTOS} min antes a '
                     f'{ORIGEM_DEPOIS_MINUTOS} min depois da saída de lá.')

        def celula(casos, momento):
            n = sum(1 for c in casos if c.imagem[0] == momento)
            return f'{n} ({dot._pct(n, len(casos))})'

        def mediana(casos):
            valores = sorted(c.imagem[1] for c in casos if c.imagem[0] == DURANTE)
            return f'{valores[len(valores) // 2]:+.0f} min' if valores else '-'

        self.tabela(['momento', 'final curta', 'passagem (controle)'],
                    [[m, celula(self.curtas, m), celula(self.controle, m)] for m in MOMENTOS] +
                    [['"durante": minutos depois da entrada (mediana)', mediana(self.curtas), mediana(self.controle)]])
        por_origem = defaultdict(Counter)
        for caso in self.curtas:
            por_origem[(caso.origem.recinto, caso.estada.recinto)][caso.imagem[0]] += 1
        self.tabela(['final curta: origem → passagem', 'casos'] + list(MOMENTOS), [
            [f'{self.nome(origem)} → {self.nome(recinto)}', sum(contagem.values())] + [contagem[m] for m in MOMENTOS]
            for (origem, recinto), contagem in sorted(por_origem.items(), key=lambda item: -sum(item[1].values()))])

    def secao_declaracoes(self):
        self.titulo('12. DECLARAÇÃO informada nos acessos (tipoDeclaracao)')
        colunas = [('final curta: saída da origem', self.curtas, 'origem'),
                   ('final curta: entrada na passagem', self.curtas, 'entrada'),
                   ('passagem: saída da origem', self.controle, 'origem')]
        contagens = [Counter(c.declaracoes.get(chave) for c in casos) for _, casos, chave in colunas]
        tipos = sorted(set().union(*contagens), key=lambda t: -sum(c[t] for c in contagens))
        self.tabela(['tipo'] + [nome for nome, _, _ in colunas],
                    [[tipo] + [contagem[tipo] for contagem in contagens] for tipo in tipos])

    def secao_caminhao(self, limite: int = 15):
        self.titulo(f'13. PARA ONDE FOI O CAMINHÃO da final curta (primeira entrada noutro recinto em até '
                    f'{JANELA_CAMINHAO_HORAS} h, pela placa)')
        por_recinto = defaultdict(Counter)
        for caso in self.curtas:
            classe_destino, recinto = caso.destino
            por_recinto[recinto][classe_destino] += 1
        linhas = sorted(por_recinto.items(), key=lambda item: -sum(item[1].values()))[:limite]
        self.tabela(['recinto de destino', 'casos'] + list(DESTINOS[:4]), [
            [self.nome(recinto) if recinto else '(nenhum)', sum(contagem.values())] +
            [contagem[d] for d in DESTINOS[:4]] for recinto, contagem in linhas])

    def secao_depois(self):
        self.titulo('14. O QUE VEIO DEPOIS da final curta (próximo evento que corta a operação)')
        dias = defaultdict(list)
        for caso in self.curtas:
            proximo = caso.caminho.eventos[-1] if caso.caminho.corte != dot.CORTE_HOJE else None
            ate = proximo.data if proximo else self.agora
            dias[caso.caminho.corte].append(dot._horas(ate, caso.estada.saida or caso.estada.entrada) / 24)
        self.tabela(['depois', 'casos', 'dias até lá (mediana)'],
                    [[corte, len(valores), f'{sorted(valores)[len(valores) // 2]:.1f}']
                     for corte, valores in sorted(dias.items(), key=lambda item: -len(item[1]))])

    def secao_ces(self, limite: int = 10):
        self.titulo('15. CEs da final curta e como o ciclo foi gravado')
        por_ce = defaultdict(list)
        for caso in self.curtas:
            por_ce[(self._gravado(caso.caminho) or {}).get('ce') or '(sem CE)'].append(caso)
        varios = {ce: casos for ce, casos in por_ce.items() if len(casos) > 1 and ce != '(sem CE)'}
        self.escreve(f'{len(por_ce)} CEs distintos; {len(varios)} com 2 ou mais contêineres na final curta '
                     f'({sum(len(c) for c in varios.values())} contêineres).')

        def percursos(casos):
            return ', '.join(sorted({f'{c.origem.recinto} → {c.estada.recinto}' for c in casos}))

        self.tabela(['CE', 'contêineres', 'percurso', 'datas'], [
            [ce, len(casos), percursos(casos), ', '.join(sorted({f'{c.estada.entrada:%d/%m}' for c in casos}))]
            for ce, casos in sorted(por_ce.items(), key=lambda item: -len(item[1]))[:limite]])
        gravados = [self._gravado(c.caminho) for c in self.curtas]
        motivos = Counter(f'{self._como(g)} — {(g or {}).get("motivo", "")}' for g in gravados)
        self.tabela(['gravado como', 'casos'], [[m, n] for m, n in motivos.most_common(limite)])

    def secao_exemplos_curta(self):
        self.titulo('16. EXEMPLOS da final curta (com o destino do caminhão, a declaração e a imagem)')
        for caso in sorted(self.curtas, key=lambda c: zlib.crc32(c.conteiner.encode()))[:self.amostras]:
            self.escreve()
            sinais = [f'{nome}' for nome, valor in caso.indicadores.items() if valor]
            destino, recinto = caso.destino
            self.escreve(f'--- {caso.conteiner}: caminhão {destino}{" em " + self.nome(recinto) if recinto else ""}; '
                         f'declaração na saída da origem: {caso.declaracoes["origem"]}'
                         + (f'; agendamento em {", ".join(caso.agendamentos)}' if caso.agendamentos else ''))
            self.escreve(f'    sinais: {"; ".join(sinais) or "nenhum"}')
            for linha in self._descreve(caso.caminho):
                self.escreve('   ' + linha)


def roda(diag: DiagnosticoFinalCurta):
    amostra = diag.secao_volume()
    diag.monta_percursos(amostra)
    diag.separa_casos()
    diag.calcula()
    for secao in (diag.secao_comparacao, diag.secao_momento_imagem, diag.secao_declaracoes, diag.secao_caminhao,
                  diag.secao_depois, diag.secao_ces, diag.secao_exemplos_curta):
        secao()
    diag.titulo('TEMPOS DAS CONSULTAS (as 5 mais lentas)')
    for sql, segundos in sorted(diag.tempos, key=lambda t: -t[1])[:5]:
        diag.escreve(f'{segundos:7.1f} s  {sql}')


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--dias', type=int, default=30, help='desembarques dos últimos N dias (padrão 30)')
    parser.add_argument('--ate-dias', type=int, default=7, help='... até N dias atrás (padrão 7)')
    parser.add_argument('--conteineres', type=int, default=20000, help='máximo de contêineres analisados')
    parser.add_argument('--exemplos', type=int, default=8, help='exemplos da final curta')
    parser.add_argument('--mongo', action='store_true', help='procura a imagem do contêiner no Mongo')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    mongo_db = None
    if args.mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    engine = create_engine(uri_do_banco(args))
    agora = datetime.now()
    inicio, fim = agora - timedelta(days=args.dias + args.ate_dias), agora - timedelta(days=args.ate_dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoFinalCurta(conexao, saida, inicio, fim, agora, args.exemplos, args.conteineres, mongo_db)
            diag.escreve(f'Diagnóstico da "final curta" — {agora:%Y-%m-%d %H:%M}; desembarques de {inicio:%Y-%m-%d} a '
                         f'{fim:%Y-%m-%d}; Mongo: {"sim" if mongo_db is not None else "não (--mongo)"}')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
