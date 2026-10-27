# coding: utf-8
"""
D3 — CONTÊINER DO ACESSO PELA PLACA (somente leitura; 20/10/2026, CICLO_DE_VIDA.md "Fora de Santos e pesagens").

Pedido do usuário: incluir o Rio ICTSI (7921302) no ciclo de vida, se não quebrar os outros recintos. Lá o acesso
efetivado nunca traz o contêiner (D1), então a saída do operador não se liga ao ciclo. Para o usuário importam, nesta
ordem, o desembarque, a imagem, o escaneamento e a pesagem. Mede a regra de `acesso_pela_placa` (contêiner do acesso
pela pesagem/inspeção da mesma placa, no mesmo recinto, a até N minutos):

  1. PREENCHIMENTO no recinto: acessos efetivados por direção, pesagens e inspeções — sem contêiner, sem placa;
  2. CONTÊINER DEDUZIDO nos acessos sem contêiner, por direção e motivo, e a cobertura por janela (2 a 30 min);
  3. CONTROLE num operador que informa o contêiner no acesso (padrão BTP 8931359): deduz ignorando o informado e
     confere — a PRECISÃO da regra;
  4. O CICLO no recinto: dos desembarques (D), quantos têm depois (até o próximo D do contêiner) inspeção, pesagem,
     embarque e saída deduzida, por idade; e os embarques (E) sem D antes que parecem desembarque;
  5. exemplos.

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/ciclo_de_vida/diagnostico_acesso_pela_placa.py --saida acesso_placa.log
    python virasana/analytic_scripts/ciclo_de_vida/diagnostico_acesso_pela_placa.py --recinto 7921302 \
        --controle 8931359 --dias 14 --dias-controle 2 --janela 5 --saida acesso_placa.log
"""
import argparse
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida import acesso_pela_placa as ap  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402
from virasana.navii.mercante_por_id import limita_tempo  # noqa: E402

ICTSI, BTP = '7921302', '8931359'
JANELAS = (2, 5, 10, 30)                # minutos
LIMITE_CONSULTA = 300
EXEMPLOS = 5
IDADES = ((3, '< 3 dias'), (7, '3 a 7 dias'), (10 ** 6, '7 dias ou mais'))
SAIDA, EMBARQUE, DESEMBARQUE, NADA = 'saída', 'embarque', 'desembarque', 'nada'
SQL_ACESSOS = ('SELECT dataHoraOcorrencia, direcao, placa, numeroConteiner FROM apirecintos_acessosveiculo '
               "WHERE codigoRecinto = :recinto AND operacao = 'C' AND dataHoraOcorrencia >= :de "
               'AND dataHoraOcorrencia < :ate')
SQL_MARCAS = ((ap.PESAGEM, 'SELECT dataHoraOcorrencia, placa, numeroConteiner FROM apirecintos_pesagensveiculo '
                           'WHERE codigoRecinto = :recinto '
                           'AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia < :ate'),
              (ap.INSPECAO, 'SELECT dataHoraOcorrencia, placa, numeroConteiner '
                            'FROM apirecintos_inspecoesnaoinvasivas WHERE codigoRecinto = :recinto '
                            'AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia < :ate'))
SQL_MOVIMENTOS = ('SELECT numeroConteiner, dataHoraOcorrencia, embarqueDesembarque '
                  'FROM apirecintos_embarquedesembarque WHERE codigoRecinto = :recinto '
                  'AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia < :ate')


def _texto(valor) -> str:
    return str(valor).strip().upper() if valor is not None else ''


def _pct(parte: int, total: int) -> str:
    return f'{parte} ({100 * parte / total:.0f}%)' if total else str(parte)


def _idade(dias: float) -> str:
    return next(nome for limite, nome in IDADES if dias < limite)


def _preenchimento(fonte: str, placa: str, conteiner: str) -> List[Tuple[str, str]]:
    """[(fonte, 'total'), (fonte, 'sem contêiner')?, (fonte, 'sem placa')?] de um evento."""
    faltas = (('sem contêiner', not conteiner), ('sem placa', not placa))
    return [(fonte, 'total')] + [(fonte, chave) for chave, falta in faltas if falta]


def _seguintes(eventos: List[Tuple[datetime, str]], desembarque: datetime) -> set:
    """O que acontece depois do desembarque e ANTES do próximo desembarque do contêiner (outra operação)."""
    proximo = min((quando for quando, o_que in eventos if o_que == DESEMBARQUE and quando > desembarque),
                  default=None)
    return {o_que for quando, o_que in eventos
            if quando > desembarque and (proximo is None or quando < proximo) and o_que != DESEMBARQUE}


def classifica_embarque(eventos: List[Tuple[datetime, str]], embarque: datetime) -> str:
    """Um embarque (E) pelos eventos do contêiner no recinto: com D antes (passagem), sem D antes e com inspeção ou
    pesagem depois (parece desembarque — D1: o ICTSI informa desembarques como E), ou sem nada."""
    if any(o_que == DESEMBARQUE and quando < embarque for quando, o_que in eventos):
        return 'com D antes (passagem)'
    if any(o_que in (ap.INSPECAO, ap.PESAGEM) and quando > embarque for quando, o_que in eventos):
        return 'sem D antes e com inspeção/pesagem depois (parece desembarque)'
    return 'sem D antes, nada depois'


def percentis(valores: List[float]) -> str:
    if not valores:
        return 'sem casos'
    ordenados = sorted(valores)
    return ' | '.join(f'p{p} {ordenados[min(len(ordenados) - 1, len(ordenados) * p // 100)]:.1f}'
                      for p in (10, 50, 90))


class Recinto:
    """Eventos de um recinto num período: acessos efetivados, marcas (pesagens/inspeções) e o preenchimento."""

    def __init__(self, codigo: str):
        self.codigo = codigo
        self.acessos: List[Tuple[datetime, str, str, str]] = []     # (data, direção, placa, contêiner)
        self.marcas: List[ap.Marca] = []
        self.preenchimento: Counter = Counter()                     # (fonte, chave) -> quantidade
        self.por_placa: Dict[str, List[ap.Marca]] = {}

    def deduz(self, janela: float, so_sem_conteiner=True):
        """[(acesso, contêiner deduzido, motivo, minutos)] dos acessos sem contêiner (ou de todos)."""
        return [(acesso, *ap.conteiner_do_acesso(acesso[0], acesso[2], self.por_placa, janela))
                for acesso in self.acessos if not (so_sem_conteiner and acesso[3])]


class DiagnosticoPlaca(Diagnostico):

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, recinto: str = ICTSI, controle: str = BTP,
                 dias_controle: int = 2, janela: float = ap.JANELA_MINUTOS, agora: Optional[datetime] = None):
        super().__init__(conexao, saida, inicio, fim, agora=agora)
        self.alvo, self.controle = Recinto(recinto), Recinto(controle)
        self.inicio_controle = max(inicio, fim - timedelta(days=dias_controle))
        self.janela = janela
        self.nomes: Dict[str, str] = {}

    def _nome(self, codigo: str) -> str:
        return f'{codigo} {self.nomes.get(codigo, "(sem cadastro)")}'

    def carrega(self, recinto: Recinto, inicio: datetime, fim: datetime):
        params = {'recinto': recinto.codigo, 'de': inicio, 'ate': fim}
        for data, direcao, placa, conteiner in self.consulta(SQL_ACESSOS, params):
            acesso = (_dt(data), _texto(direcao) or '?', _texto(placa), _texto(conteiner))
            recinto.acessos.append(acesso)
            recinto.preenchimento.update(_preenchimento(f'acesso {acesso[1]}', acesso[2], acesso[3]))
        folga = timedelta(minutes=max(JANELAS))
        for fonte, sql in SQL_MARCAS:
            for data, placa, conteiner in self.consulta(sql, {**params, 'de': inicio - folga, 'ate': fim + folga}):
                marca = ap.Marca(_dt(data), _texto(placa), _texto(conteiner), fonte)
                recinto.marcas.append(marca)
                recinto.preenchimento.update(_preenchimento(fonte, marca.placa, marca.conteiner))
        recinto.por_placa = ap.indexa(recinto.marcas)

    def carrega_tudo(self):
        self.nomes = {_texto(c): n for c, n in self.consulta(
            "SELECT cod_siscomex, nome FROM ovr_recintos WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''")}
        self.carrega(self.alvo, self.periodo['inicio'], self.periodo['fim'])
        self.carrega(self.controle, self.inicio_controle, self.periodo['fim'])

    # ---------------------------------------------------------------- 1
    def secao_preenchimento(self):
        self.titulo(f'1. PREENCHIMENTO — {self._nome(self.alvo.codigo)} ({self.periodo["inicio"]:%d/%m} a '
                    f'{self.periodo["fim"]:%d/%m}) e controle {self._nome(self.controle.codigo)} '
                    f'(desde {self.inicio_controle:%d/%m})')
        linhas = []
        for recinto in (self.alvo, self.controle):
            for fonte in sorted({fonte for fonte, _ in recinto.preenchimento}):
                contagem = {chave: n for (f, chave), n in recinto.preenchimento.items() if f == fonte}
                total = contagem.get('total', 0)
                linhas.append((recinto.codigo, fonte, total, _pct(contagem.get('sem contêiner', 0), total),
                               _pct(contagem.get('sem placa', 0), total)))
        self.tabela(['recinto', 'evento', 'total', 'sem contêiner', 'sem placa'], linhas)

    # ---------------------------------------------------------------- 2
    def secao_deduzidos(self):
        self.titulo(f'2. CONTÊINER DEDUZIDO pela placa nos acessos SEM contêiner — {self._nome(self.alvo.codigo)}')
        deduzidos = self.alvo.deduz(self.janela)
        por_direcao = Counter(acesso[1] for acesso, *_ in deduzidos)
        contagem = Counter((acesso[1], motivo) for acesso, _, motivo, _ in deduzidos)
        self.tabela([f'direção | motivo (janela {self.janela:g} min)', 'acessos'],
                    [(f'{direcao} | {motivo}', _pct(n, por_direcao[direcao]))
                     for (direcao, motivo), n in sorted(contagem.items())])
        self.escreve()
        self.escreve(f'Minutos até a marca, nos deduzidos: {percentis([m for _, c, _, m in deduzidos if c])}')
        self.escreve()
        linhas = []
        for janela in JANELAS:
            resultado = self.alvo.deduz(janela)
            for direcao in sorted(por_direcao):
                daqui = [(c, motivo) for acesso, c, motivo, _ in resultado if acesso[1] == direcao]
                linhas.append((f'{janela} min', direcao, len(daqui), _pct(sum(1 for c, _ in daqui if c), len(daqui)),
                               _pct(sum(1 for _, motivo in daqui if motivo == ap.AMBIGUO), len(daqui))))
        self.tabela(['janela', 'direção', 'acessos sem contêiner', 'deduzidos', 'ambíguos'], linhas)

    # ---------------------------------------------------------------- 3
    def secao_controle(self):
        self.titulo(f'3. CONTROLE — {self._nome(self.controle.codigo)}: acessos COM contêiner, deduzido ignorando o '
                    'informado (precisão = igual / (igual + diferente))')
        linhas = []
        for janela in JANELAS:
            contagem = Counter()
            for acesso, conteiner, motivo, _ in self.controle.deduz(janela, so_sem_conteiner=False):
                if acesso[3]:
                    contagem[motivo if not conteiner else 'igual' if conteiner == acesso[3] else 'diferente'] += 1
            total = sum(contagem.values())
            decididos = contagem['igual'] + contagem['diferente']
            linhas.append((f'{janela} min', total, _pct(contagem['igual'], total), _pct(contagem['diferente'], total),
                           _pct(contagem[ap.AMBIGUO], total), _pct(contagem[ap.NENHUM], total),
                           f'{100 * contagem["igual"] / decididos:.1f}%' if decididos else '-'))
        self.tabela(['janela', 'acessos com contêiner', 'igual', 'diferente', 'ambíguo', 'nenhuma marca', 'precisão'],
                    linhas)

    # ---------------------------------------------------------------- 4
    def movimentos(self) -> List[Tuple[str, datetime, str]]:
        params = {'recinto': self.alvo.codigo, 'de': self.periodo['inicio'], 'ate': self.periodo['fim']}
        return [(_texto(c), _dt(data), _texto(tipo)) for c, data, tipo in self.consulta(SQL_MOVIMENTOS, params) if c]

    def eventos_do_alvo(self, movimentos) -> Dict[str, List[Tuple[datetime, str]]]:
        """{contêiner: [(data, o quê)]} no alvo: inspeções, pesagens, saídas (informadas ou deduzidas), D e E."""
        eventos = defaultdict(list)
        for marca in self.alvo.marcas:
            if marca.conteiner:
                eventos[marca.conteiner].append((marca.data, marca.fonte))
        for acesso, conteiner, _, _ in self.alvo.deduz(self.janela, so_sem_conteiner=False):
            if acesso[1] == 'S' and (acesso[3] or conteiner):
                eventos[acesso[3] or conteiner].append((acesso[0], SAIDA))
        for conteiner, data, tipo in movimentos:
            eventos[conteiner].append((data, EMBARQUE if tipo == 'E' else DESEMBARQUE))
        return eventos

    def secao_ciclo(self):
        self.titulo(f'4. O CICLO no {self._nome(self.alvo.codigo)} — o que vem depois de cada desembarque (D), até o '
                    'próximo D do contêiner, por idade; e os embarques (E)')
        movimentos = self.movimentos()
        eventos = self.eventos_do_alvo(movimentos)
        contagem, totais, embarques = Counter(), Counter(), Counter()
        for conteiner, data, tipo in movimentos:
            if tipo == 'E':
                embarques[classifica_embarque(eventos[conteiner], data)] += 1
                continue
            idade = _idade((self.periodo['fim'] - data).total_seconds() / 86400)
            totais[idade] += 1
            contagem.update((idade, o_que) for o_que in _seguintes(eventos[conteiner], data) or {NADA})
        colunas = (ap.INSPECAO, ap.PESAGEM, EMBARQUE, SAIDA, NADA)
        self.tabela(['idade do D', 'desembarques', 'com inspeção', 'com pesagem', 'com embarque', 'com saída',
                     'nada depois'],
                    [(nome, totais[nome], *(_pct(contagem[(nome, c)], totais[nome]) for c in colunas))
                     for _, nome in IDADES if totais[nome]])
        self.escreve()
        self.tabela(['embarques (E) no período', 'quantidade'], sorted(embarques.items()))

    # ---------------------------------------------------------------- 5
    def secao_exemplos(self):
        self.titulo('5. EXEMPLOS — acessos sem contêiner deduzidos e ambíguos')
        deduzidos = self.alvo.deduz(self.janela)
        for motivo in (ap.PELAS_DUAS, ap.PELA_PESAGEM, ap.PELA_INSPECAO, ap.AMBIGUO):
            casos = [(a, c, m) for a, c, mot, m in deduzidos if mot == motivo][:EXEMPLOS]
            if casos:
                self.escreve(f'--- {motivo}')
            for acesso, conteiner, minutos in casos:
                self.escreve(f'   {acesso[0]:%d/%m %H:%M} {acesso[1]} placa {acesso[2]} -> {conteiner or "?"} '
                             f'({minutos:.1f} min)')


def roda(diag: DiagnosticoPlaca):
    diag.carrega_tudo()
    diag.secao_preenchimento()
    diag.secao_deduzidos()
    diag.secao_controle()
    diag.secao_ciclo()
    diag.secao_exemplos()
    diag.resumo(6)


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--recinto', default=ICTSI, help=f'recinto sem contêiner no acesso (padrão {ICTSI}, ICTSI)')
    parser.add_argument('--controle', default=BTP, help=f'operador que informa o contêiner (padrão {BTP}, BTP)')
    parser.add_argument('--dias', type=int, default=14, help='eventos dos últimos N dias no recinto (padrão 14)')
    parser.add_argument('--dias-controle', type=int, default=2, help='... e no controle (padrão 2)')
    parser.add_argument('--janela', type=float, default=ap.JANELA_MINUTOS,
                        help=f'minutos entre o acesso e a pesagem/inspeção (padrão {ap.JANELA_MINUTOS})')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    engine = create_engine(uri_do_banco(args), pool_pre_ping=True)
    limita_tempo(engine, args.limite)
    agora = datetime.now()
    inicio = agora - timedelta(days=args.dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoPlaca(conexao, saida, inicio, agora, args.recinto, args.controle, args.dias_controle,
                                    args.janela, agora)
            diag.escreve(f'Contêiner do acesso pela placa (D3) — {agora:%Y-%m-%d %H:%M}; recinto {args.recinto}, '
                         f'controle {args.controle}, janela {args.janela:g} min')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
