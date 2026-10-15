# coding: utf-8
"""
DIAGNÓSTICO: recinto OPERADOR fazendo papel de recinto seco (TRA). Somente leitura.

Observação do usuário (15/10/2026), que faltou na especificação inicial: os operadores às vezes armazenam carga
desembarcada em OUTRO operador, como um TRA — desembarca na ECOPORTO, sai, entra na DPW/EMBRAPORT, é escaneada e
fica armazenada lá; ou desembarca na BTP, Santos Brasil ou DPW, sai, entra na ECOPORTO, é escaneada e fica lá. Isso
se confunde com o ESCANEAMENTO TERCEIRIZADO, já tratado: desembarca na BTP, sai, entra na ECOPORTO só para
escanear e segue para um TRA (recinto não operador da 0817800), onde ficam a entrada e a saída do ciclo.

Hoje o núcleo (`linha_do_tempo.py`) trata toda estada noutro operador como PASSAGEM: a saída de lá volta o ciclo ao
trânsito, o escaneamento feito lá é marcado "fora" e, sem nova entrada em 48 h, o ciclo fecha como "entregue pelo
operador" (o do desembarque). A completude, então, cobra o escaneamento do operador do desembarque e não conta o
outro operador como TRA. Este diagnóstico mede o tamanho disso e dá os números para a regra nova:

  1. volume: ciclos por operador do desembarque e quantos entraram (cheios) noutro operador de Santos em até
     30 dias — esses contêineres são os analisados (amostra espalhada, `--conteineres`);
  2. o PERCURSO de cada desembarque, estada por estada (entrada, saída, inspeções no recinto), até a primeira
     fronteira (novo desembarque, embarque, entrada vazia, volta ao operador do desembarque, entrada mais de
     48 h depois da saída) — e a classe de cada estada depois do operador do desembarque:
        passagem      estada de até 6 h seguida de nova entrada (escaneamento terceirizado)
        longa+segue   estada de mais de 6 h seguida de nova entrada
        final curta   estada de até 6 h sem nova entrada
        final (TRA?)  estada de mais de 6 h sem nova entrada — candidato a operador em papel de TRA
        em aberto     ainda no recinto (sem saída até hoje)
        saiu <48h     saiu há menos de 48 h (ainda pode entrar noutro recinto)
        baldeação     embarque no recinto
        saída omit.   sem saída informada e nova entrada noutro recinto
        cortada       sem saída e a operação acabou numa fronteira
        sem entrada   só a saída informada (duração desconhecida)
     por tipo de recinto (operador x não operador), por faixa de duração e por par de operadores;
  3. como o ciclo GRAVADO (v2) tratou cada classe (fluxo/situação, armazenagem, escaneamento gravado no recinto);
  4. efeito na COMPLETUDE: estadas "final (TRA?)" contadas hoje como entrega pelo operador do desembarque, com e
     sem escaneamento gravado;
  5. referência: duração das estadas nos TRA (não operadores) já gravadas;
  6. DTA dos CEs por classe (se existir `navii_dtas`); TRÁFEGO do CE do ciclo por classe (a baldeação também fica
     dias no outro operador antes de embarcar: o tráfego/origem do CE separa os dois enquanto o contêiner está lá?);
     percursos mais comuns e exemplos para conferir à mão.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    DIAG=virasana/analytic_scripts/ciclo_de_vida/diagnostico_operador_tra.py
    virasana-venv/bin/python $DIAG --saida operador_tra.log
    virasana-venv/bin/python $DIAG --dias 60 --ate-dias 7 --conteineres 6000 --exemplos 8 --saida operador_tra.log
"""
import argparse
import sys
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set

from sqlalchemy import create_engine, inspect

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import CargaCiclo  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402

_PARAMETROS = ldt.Parametros()
ESTADA_CURTA_HORAS = _PARAMETROS.passagem_ate_entrada_horas   # 6 h: passagem (escanear) x armazenagem
PRAZO_TRANSITO_HORAS = _PARAMETROS.prazo_transito_horas       # 48 h: entrada depois disso é outra operação
TOLERANCIA_GATE_HORAS = _PARAMETROS.tolerancia_gate_horas     # inspeção até 2 h antes da entrada (gate)
JANELA_ENTRADA_DIAS = 30        # entrada no outro operador até N dias depois do desembarque (candidatos)
LIMITE_EVENTOS_EXEMPLO = 24

PASSAGEM, LONGA_E_SEGUE, FINAL_CURTA, FINAL = 'passagem', 'longa+segue', 'final curta', 'final (TRA?)'
EM_ABERTO, SAIU_AGORA, BALDEACAO = 'em aberto', 'saiu <48h', 'baldeação'
SAIDA_OMITIDA, CORTADA, SEM_ENTRADA = 'saída omit.', 'cortada', 'sem entrada'
CLASSES = (PASSAGEM, LONGA_E_SEGUE, FINAL_CURTA, FINAL, EM_ABERTO, SAIU_AGORA, BALDEACAO, SAIDA_OMITIDA, CORTADA,
           SEM_ENTRADA)
CORTE_HOJE = 'eventos até hoje'

FAIXAS = ((0.5, '≤ 30 min'), (3, '30 min–3 h'), (6, '3–6 h'), (24, '6–24 h'), (72, '1–3 dias'),
          (240, '3–10 dias'), (None, '> 10 dias'))
SEM_SAIDA, SEM_ENTRADA_INFORMADA = 'sem saída', 'entrada não informada'

SQL_CICLOS_POR_OPERADOR = (
    'SELECT codigoRecintoDesembarque, COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao '
    'WHERE data_desembarque >= :inicio AND data_desembarque < :fim AND codigoRecintoDesembarque IN :escopo '
    'GROUP BY codigoRecintoDesembarque')
# entrada CHEIA (acesso efetivado) noutro operador de Santos depois do desembarque; o limite de 30 dias por
# ciclo é aplicado aqui no Python (a soma de datas no SQL muda entre MariaDB e SQLite)
SQL_CANDIDATOS = (
    'SELECT c.codigoRecintoDesembarque, c.numeroConteiner, c.data_desembarque, a.dataHoraOcorrencia '
    'FROM ciclodevida_ciclo_de_vida_importacao c '
    'JOIN apirecintos_acessosveiculo a ON a.numeroConteiner = c.numeroConteiner '
    'WHERE c.data_desembarque >= :inicio AND c.data_desembarque < :fim AND c.codigoRecintoDesembarque IN :escopo '
    "AND a.operacao = 'C' AND a.direcao = 'E' AND a.codigoRecinto IN :operadores_escopo "
    'AND a.codigoRecinto <> c.codigoRecintoDesembarque AND (a.vazioConteiner IS NULL OR a.vazioConteiner = 0) '
    'AND a.dataHoraOcorrencia > c.data_desembarque AND a.dataHoraOcorrencia < :ate')
SQL_GRAVADOS = (
    'SELECT ID, desembarque_id, fluxo, situacao, motivo, codigoRecintoArmazenagem, concluido, numeroCEmercante, '
    'versao_heuristica FROM ciclodevida_ciclo_de_vida_importacao WHERE desembarque_id IN :ids')
SQL_ESCANEAMENTOS = ('SELECT ciclo_de_vida_id, codigoRecinto, fora '
                     'FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos WHERE ciclo_de_vida_id IN :ids')
SQL_ESTADAS_TRA = (
    'SELECT data_entrada_seco, data_saida_seco FROM ciclodevida_ciclo_de_vida_importacao '
    'WHERE data_desembarque >= :inicio AND data_desembarque < :fim AND codigoRecintoDesembarque IN :escopo '
    'AND data_entrada_seco IS NOT NULL AND data_saida_seco IS NOT NULL')
SQL_DTAS = 'SELECT nr_ce, descricao_tipo_declaracao_transito FROM navii_dtas WHERE nr_ce IN :ces'
SQL_TRAFEGO = 'SELECT numeroCEmercante, tipoTrafego FROM conhecimentosresumo WHERE numeroCEmercante IN :ces'
TRAFEGOS = (('5', 'importação (5)'), ('9', 'passagem (9)'), ('3', 'cabotagem (3)'), ('7', 'exportação (7)'))
PREFIXO_SANTOS = '15'   # CE emitido em Santos (os de outros portos: transbordo/cabotagem — sombra de 01/10)


def _horas(depois: datetime, antes: datetime) -> float:
    return (depois - antes).total_seconds() / 3600


def faixa(horas: Optional[float]) -> str:
    for limite, rotulo in FAIXAS:
        if limite is None or horas <= limite:
            return rotulo
    return FAIXAS[-1][1]  # pragma: no cover


def _pct(parte: int, total: int) -> str:
    return f'{100 * parte / total:.0f}%' if total else '-'


def _mediana(valores: List[float]) -> str:
    if not valores:
        return '-'
    ordenados = sorted(valores)
    horas = ordenados[len(ordenados) // 2]
    return f'{horas:.1f} h' if horas < 48 else f'{horas / 24:.1f} d'


# ---------------------------------------------------------------------- percurso (função pura)

@dataclass
class Estada:
    recinto: str
    operador: bool
    entrada: Optional[datetime]                     # None: entrada não informada (só a saída)
    saida: Optional[datetime] = None
    embarque: Optional[datetime] = None
    inspecoes: List[ldt.Evento] = field(default_factory=list)   # no recinto, durante a estada (ou no gate)
    soltas: List[ldt.Evento] = field(default_factory=list)      # em outros recintos, depois da entrada
    entrada_id: Optional[int] = None                            # ids dos acessos (para ler placa/declaração)
    saida_id: Optional[int] = None

    @property
    def fim(self) -> Optional[datetime]:
        return self.embarque or self.saida

    def horas(self) -> Optional[float]:
        return _horas(self.fim, self.entrada) if self.entrada and self.fim else None

    def faixa(self) -> str:
        if self.entrada is None:
            return SEM_ENTRADA_INFORMADA
        return SEM_SAIDA if self.fim is None else faixa(self.horas())


@dataclass
class Percurso:
    conteiner: str
    desembarque: ldt.Evento
    estadas: List[Estada]
    corte: str
    eventos: List[ldt.Evento] = field(default_factory=list)   # os do percurso (para os exemplos)


def _nova(estadas: List[Estada], recinto: str, operadores: Set[str], entrada: Optional[datetime]) -> Estada:
    """Abre a estada seguinte; as inspeções soltas no recinto dela passam para ela (scanner no gate até 2 h antes
    da entrada, ou qualquer uma quando a entrada não foi informada)."""
    anterior, nova = estadas[-1], Estada(recinto, recinto in operadores, entrada)
    for inspecao in list(anterior.soltas):
        if inspecao.recinto == recinto and (entrada is None or
                                            _horas(entrada, inspecao.data) <= TOLERANCIA_GATE_HORAS):
            anterior.soltas.remove(inspecao)
            nova.inspecoes.append(inspecao)
    estadas.append(nova)
    return nova


def _depois_do_prazo(estada: Estada, data: datetime) -> bool:
    return estada.saida is not None and _horas(data, estada.saida) > PRAZO_TRANSITO_HORAS


def _entrada(estadas: List[Estada], evento: ldt.Evento, desembarque: ldt.Evento, operadores: Set[str]):
    atual = estadas[-1]
    if evento.recinto == atual.recinto and atual.saida is None:
        return 'reentrada sem saída informada'
    if evento.recinto == desembarque.recinto:
        return 'voltou ao operador do desembarque'
    if evento.vazio:
        return 'entrada com o contêiner vazio'
    if _depois_do_prazo(atual, evento.data):
        return f'entrada mais de {PRAZO_TRANSITO_HORAS:.0f} h depois da saída'
    _nova(estadas, evento.recinto, operadores, evento.data).entrada_id = evento.id
    return None


def _saida(estadas: List[Estada], evento: ldt.Evento, operadores: Set[str]):
    atual = estadas[-1]
    if evento.recinto == atual.recinto:
        if atual.saida is None:
            atual.saida, atual.saida_id = evento.data, evento.id
        return None
    if _depois_do_prazo(atual, evento.data):
        return f'saída noutro recinto mais de {PRAZO_TRANSITO_HORAS:.0f} h depois'
    nova = _nova(estadas, evento.recinto, operadores, None)   # entrada não informada
    nova.saida, nova.saida_id = evento.data, evento.id
    return None


def _passo(estadas: List[Estada], evento: ldt.Evento, desembarque: ldt.Evento, operadores: Set[str]):
    """Aplica um evento ao percurso; devolve o motivo do corte (fronteira) ou None."""
    atual = estadas[-1]
    if evento.tipo == ldt.DESEMBARQUE:
        return 'novo desembarque'
    if evento.tipo == ldt.EMBARQUE:
        if evento.recinto != atual.recinto:
            atual = _nova(estadas, evento.recinto, operadores, None)
        atual.embarque = evento.data
        return 'embarque'
    if evento.tipo == ldt.ENTRADA:
        return _entrada(estadas, evento, desembarque, operadores)
    if evento.tipo == ldt.SAIDA:
        return _saida(estadas, evento, operadores)
    if evento.tipo == ldt.INSPECAO:
        no_recinto = evento.recinto == atual.recinto and atual.saida is None
        (atual.inspecoes if no_recinto else atual.soltas).append(evento)
    return None


def percurso(conteiner: str, seguintes: Iterable[ldt.Evento], desembarque: ldt.Evento,
             operadores: Set[str]) -> Percurso:
    """Estadas do contêiner depois do desembarque (`seguintes`: os eventos ordenados que vêm depois dele), até a
    primeira fronteira. A estada 0 é o operador do desembarque. Pesagens são ignoradas."""
    estadas, usados = [Estada(desembarque.recinto, True, desembarque.data)], [desembarque]
    for evento in seguintes:
        corte = _passo(estadas, evento, desembarque, operadores)
        usados.append(evento)
        if corte:
            return Percurso(conteiner, desembarque, estadas, corte, usados)
    return Percurso(conteiner, desembarque, estadas, CORTE_HOJE, usados)


def classe(caminho: Percurso, indice: int, agora: datetime) -> str:
    """Classe da estada `indice` (>= 1) do percurso — ver a docstring do módulo."""
    estada = caminho.estadas[indice]
    seguinte = caminho.estadas[indice + 1] if indice + 1 < len(caminho.estadas) else None
    if estada.embarque:
        return BALDEACAO
    if estada.entrada is None:
        return SEM_ENTRADA
    if estada.saida is None:
        if seguinte:
            return SAIDA_OMITIDA
        return EM_ABERTO if caminho.corte == CORTE_HOJE else CORTADA
    curta = estada.horas() <= ESTADA_CURTA_HORAS
    if seguinte:
        return PASSAGEM if curta else LONGA_E_SEGUE
    if caminho.corte == CORTE_HOJE and _horas(agora, estada.saida) < PRAZO_TRANSITO_HORAS:
        return SAIU_AGORA
    return FINAL_CURTA if curta else FINAL


def padrao(caminho: Percurso) -> str:
    """Percurso genérico (OP = operador do desembarque, OP2 = outro operador, SECO = não operador)."""
    partes = ['OP']
    for estada in caminho.estadas[1:]:
        insp = ', insp' if estada.inspecoes else ''
        partes.append(f'{"OP2" if estada.operador else "SECO"}({estada.faixa()}{insp})')
    return ' → '.join(partes)


# ---------------------------------------------------------------------- diagnóstico (banco)

class DiagnosticoOperadorTra(Diagnostico):

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, agora: Optional[datetime] = None,
                 amostras: int = 5, conteineres: int = 4000):
        super().__init__(conexao, saida, inicio, fim, agora, amostras)
        self.max_conteineres = conteineres
        self.carga = CargaCiclo(self, inicio, fim)
        self.escopo, self.operadores, self.nomes = self.carga.operadores()
        self.percursos: List[Percurso] = []
        self.gravados: Dict[int, dict] = {}
        self.eventos: Dict[str, List[ldt.Evento]] = {}   # contêiner -> todos os eventos (sem retificações)

    def nome(self, recinto: Optional[str]) -> str:
        return f'{recinto} {(self.nomes.get(recinto) or "")[:22]}'.strip() if recinto else '-'

    def estadas(self, operador: Optional[bool] = None):
        """(percurso, índice, estada, classe) de cada estada depois do operador do desembarque."""
        for caminho in self.percursos:
            for indice in range(1, len(caminho.estadas)):
                estada = caminho.estadas[indice]
                if operador is None or estada.operador == operador:
                    yield caminho, indice, estada, classe(caminho, indice, self.agora)

    # ---------------------------------------------------------------- 1
    def candidatos(self) -> Dict[str, Set[str]]:
        """{operador do desembarque: contêineres com entrada cheia noutro operador de Santos em até 30 dias}."""
        escopo = sorted(self.escopo)
        linhas = self.consulta(SQL_CANDIDATOS, {
            'escopo': escopo, 'operadores_escopo': escopo,
            'ate': self.periodo['fim'] + timedelta(days=JANELA_ENTRADA_DIAS)}, listas=('escopo', 'operadores_escopo'))
        por_operador = defaultdict(set)
        for origem, conteiner, desembarque, entrada in linhas:
            if _dt(entrada) - _dt(desembarque) <= timedelta(days=JANELA_ENTRADA_DIAS):
                por_operador[origem].add(conteiner)
        return por_operador

    def secao_volume(self) -> List[str]:
        self.titulo('1. VOLUME — ciclos por operador do desembarque; quantos entraram CHEIOS noutro operador de '
                    f'Santos em até {JANELA_ENTRADA_DIAS} dias')
        self.escreve('Operadores de Santos (escopo): ' + ', '.join(self.nome(op) for op in sorted(self.escopo)))
        ciclos = dict(self.consulta(SQL_CICLOS_POR_OPERADOR, {'escopo': sorted(self.escopo)}, listas=('escopo',)))
        candidatos = self.candidatos()
        self.tabela(['operador do desembarque', 'ciclos', 'com entrada noutro operador', '%'], [
            [self.nome(op), ciclos.get(op, 0), len(candidatos.get(op, ())),
             _pct(len(candidatos.get(op, ())), ciclos.get(op, 0))] for op in sorted(set(ciclos) | set(candidatos))])
        todos = sorted(set().union(*candidatos.values())) if candidatos else []
        amostra = sorted(todos, key=lambda c: zlib.crc32(c.encode()))[:self.max_conteineres]
        self.escreve(f'Contêineres candidatos: {len(todos)}; analisados: {len(amostra)} (amostra espalhada; '
                     'limite em --conteineres).')
        return amostra

    def monta_percursos(self, conteineres: List[str]):
        eventos, _ = self.carga.eventos(conteineres)
        inicio, fim = self.periodo['inicio'], self.periodo['fim']
        for conteiner, lista in eventos.items():
            ordenados = self.eventos[conteiner] = ldt.sem_retificacoes(lista)
            for posicao, evento in enumerate(ordenados):
                if evento.tipo == ldt.DESEMBARQUE and evento.recinto in self.escopo and inicio <= evento.data < fim:
                    self.percursos.append(percurso(conteiner, ordenados[posicao + 1:], evento, self.operadores))
        self.gravados = self._gravados([c.desembarque.id for c in self.percursos])

    def _gravados(self, ids: List[int]) -> Dict[int, dict]:
        gravados = {}
        for id_, desembarque_id, fluxo, situacao, motivo, armazenagem, concluido, ce, versao in self._por_lotes(
                SQL_GRAVADOS, 'ids', ids):
            gravados[desembarque_id] = {'id': id_, 'fluxo': fluxo, 'situacao': situacao, 'motivo': motivo or '',
                                        'armazenagem': armazenagem, 'concluido': bool(concluido), 'ce': ce,
                                        'versao': versao, 'escaneamentos': []}
        por_ciclo = {g['id']: g for g in gravados.values()}
        for ciclo_id, recinto, fora in self._por_lotes(SQL_ESCANEAMENTOS, 'ids', list(por_ciclo)):
            por_ciclo[ciclo_id]['escaneamentos'].append((recinto, bool(fora)))
        return gravados

    # ---------------------------------------------------------------- 2
    def secao_classes(self):
        self.titulo('2. ESTADAS DEPOIS DO OPERADOR DO DESEMBARQUE, por classe e tipo de recinto')
        self.escreve(f'{len(self.percursos)} desembarques no período com percurso montado. Classes: ver o cabeçalho '
                     f'do script (curta = até {ESTADA_CURTA_HORAS:.0f} h; nova entrada = até '
                     f'{PRAZO_TRANSITO_HORAS:.0f} h depois da saída).')
        dados = defaultdict(lambda: {True: [], False: []})
        for _, _, estada, nome_classe in self.estadas():
            dados[nome_classe][estada.operador].append(estada)
        linhas = []
        for nome_classe in CLASSES:
            if nome_classe in dados:
                op, nao = dados[nome_classe][True], dados[nome_classe][False]
                linhas.append([nome_classe, len(op), _pct(sum(1 for e in op if e.inspecoes), len(op)),
                               _mediana([e.horas() for e in op if e.horas() is not None]), len(nao),
                               _pct(sum(1 for e in nao if e.inspecoes), len(nao)),
                               _mediana([e.horas() for e in nao if e.horas() is not None])])
        self.tabela(['classe', 'OPERADOR: estadas', 'com inspeção lá', 'duração (mediana)', 'NÃO OPERADOR: estadas',
                     'com inspeção lá', 'duração (mediana)'], linhas)

    def secao_faixas(self):
        self.titulo('3. ESTADAS EM OUTRO OPERADOR: faixa de duração x classe')
        contagem, classes = defaultdict(Counter), Counter()
        for _, _, estada, nome_classe in self.estadas(operador=True):
            contagem[estada.faixa()][nome_classe] += 1
            classes[nome_classe] += 1
        colunas = [c for c in CLASSES if classes[c]]
        ordem = [rotulo for _, rotulo in FAIXAS] + [SEM_SAIDA, SEM_ENTRADA_INFORMADA]
        self.tabela(['duração'] + colunas + ['total'], [
            [rotulo] + [contagem[rotulo][c] for c in colunas] + [sum(contagem[rotulo].values())]
            for rotulo in ordem if contagem[rotulo]])

    def secao_pares(self):
        self.titulo('4. ESTADAS EM OUTRO OPERADOR por par (operador do desembarque → operador da estada)')
        pares, inspecao = defaultdict(Counter), Counter()
        for caminho, _, estada, nome_classe in self.estadas(operador=True):
            par = (caminho.desembarque.recinto, estada.recinto)
            pares[par][nome_classe] += 1
            inspecao[par] += bool(estada.inspecoes)
        principais = (PASSAGEM, FINAL, FINAL_CURTA, EM_ABERTO, LONGA_E_SEGUE, BALDEACAO)
        linhas = [[f'{self.nome(origem)} → {self.nome(destino)}', sum(contagem.values()),
                   _pct(inspecao[(origem, destino)], sum(contagem.values()))] +
                  [contagem[c] for c in principais] + [sum(contagem.values()) - sum(contagem[c] for c in principais)]
                  for (origem, destino), contagem in sorted(pares.items(), key=lambda p: -sum(p[1].values()))]
        self.tabela(['par', 'estadas', 'com inspeção lá'] + list(principais) + ['outras'], linhas)

    # ---------------------------------------------------------------- 3 e 4
    def _gravado(self, caminho: Percurso) -> Optional[dict]:
        return self.gravados.get(caminho.desembarque.id)

    @staticmethod
    def _como(gravado: Optional[dict]) -> str:
        if not gravado:
            return 'sem ciclo gravado'
        situacao = gravado['situacao'] or ('concluído' if gravado['concluido'] else 'aberto')
        return f'{gravado["fluxo"] or "-"}/{situacao}' + ('' if gravado['versao'] == 'v2' else ' (loop antigo)')

    def secao_gravado(self):
        self.titulo('5. COMO O CICLO GRAVADO TRATOU AS ESTADAS EM OUTRO OPERADOR (fluxo/situação)')
        contagem = defaultdict(Counter)
        for caminho, _, estada, nome_classe in self.estadas(operador=True):
            gravado = self._gravado(caminho)
            chave = (nome_classe, self._como(gravado))
            escaneamentos = gravado['escaneamentos'] if gravado else []
            contagem[chave]['estadas'] += 1
            contagem[chave]['armazenagem lá'] += bool(gravado and gravado['armazenagem'] == estada.recinto)
            contagem[chave]['esc. lá "fora"'] += any(r == estada.recinto and fora for r, fora in escaneamentos)
            contagem[chave]['esc. lá'] += any(r == estada.recinto and not fora for r, fora in escaneamentos)
            contagem[chave]['nenhum esc.'] += not escaneamentos
        colunas = ['estadas', 'armazenagem lá', 'esc. lá "fora"', 'esc. lá', 'nenhum esc.']
        self.tabela(['classe', 'gravado como'] + colunas, [
            [nome_classe, como] + [contagem[(nome_classe, como)][c] for c in colunas]
            for nome_classe, como in sorted(contagem, key=lambda k: (CLASSES.index(k[0]), -contagem[k]['estadas']))])

    def secao_completude(self):
        self.titulo('6. EFEITO NA COMPLETUDE — estadas "final (TRA?)" e "em aberto" em outro operador')
        self.escreve('"entrega pelo operador" = o ciclo conta hoje na completude do OPERADOR DO DESEMBARQUE '
                     '(fluxo operador, concluído, sem entrada no seco); o operador da estada não aparece como TRA.')
        por_recinto = defaultdict(Counter)
        for caminho, _, estada, nome_classe in self.estadas(operador=True):
            if nome_classe not in (FINAL, EM_ABERTO):
                continue
            gravado, contagem = self._gravado(caminho), por_recinto[(estada.recinto, nome_classe)]
            entrega = bool(gravado and gravado['fluxo'] in (ldt.OPERADOR, None) and gravado['concluido']
                           and not gravado['armazenagem'])
            contagem['estadas'] += 1
            contagem['inspeção lá (API)'] += bool(estada.inspecoes)
            contagem['entrega pelo op. do desembarque'] += entrega
            contagem['... sem escaneamento gravado'] += entrega and not gravado['escaneamentos']
            contagem['TRA gravado lá'] += bool(gravado and gravado['armazenagem'] == estada.recinto)
            contagem['aberto'] += bool(gravado and not gravado['concluido'])
        colunas = ['estadas', 'inspeção lá (API)', 'entrega pelo op. do desembarque', '... sem escaneamento gravado',
                   'TRA gravado lá', 'aberto']
        self.tabela(['recinto da estada', 'classe'] + colunas, [
            [self.nome(recinto), nome_classe] + [por_recinto[(recinto, nome_classe)][c] for c in colunas]
            for recinto, nome_classe in sorted(por_recinto)])

    # ---------------------------------------------------------------- 5 e 6
    def secao_referencia_tra(self):
        self.titulo('7. REFERÊNCIA — duração das estadas no seco (TRA) já gravadas, mesmos desembarques '
                    '(desde 15/10/2026 inclui o operador em papel de TRA)')
        faixas = Counter()
        for entrada, saida in self.consulta(SQL_ESTADAS_TRA, {'escopo': sorted(self.escopo)}, listas=('escopo',)):
            faixas[faixa(_horas(_dt(saida), _dt(entrada)))] += 1
        total = sum(faixas.values())
        self.tabela(['duração', 'estadas no TRA', '%'],
                    [[rotulo, faixas[rotulo], _pct(faixas[rotulo], total)] for _, rotulo in FAIXAS])

    def secao_dtas(self):
        self.titulo('8. DTA dos CEs das estadas em outro operador, por classe (tabela navii_dtas)')
        if not inspect(self.conexao).has_table('navii_dtas'):
            self.escreve('(tabela navii_dtas não existe neste banco)')
            return
        ces_por_classe = defaultdict(set)
        for caminho, _, _, nome_classe in self.estadas(operador=True):
            gravado = self._gravado(caminho)
            if gravado and gravado['ce']:
                ces_por_classe[nome_classe].add(gravado['ce'])
        tipos = defaultdict(Counter)
        for ce, tipo in self._por_lotes(SQL_DTAS, 'ces', set().union(*ces_por_classe.values()) if ces_por_classe
                                        else []):
            tipos[ce][(tipo or '').strip() or '(sem tipo)'] += 1
        linhas = []
        for nome_classe in CLASSES:
            ces = ces_por_classe.get(nome_classe)
            if ces:
                com = [ce for ce in ces if ce in tipos]
                principais = sum((tipos[ce] for ce in com), Counter()).most_common(3)
                linhas.append([nome_classe, len(ces), len(com), _pct(len(com), len(ces)),
                               '; '.join(f'{t} ({n})' for t, n in principais)])
        self.tabela(['classe', 'CEs', 'com DTA', '%', 'tipos mais comuns'], linhas)

    def secao_trafego(self):
        self.titulo('8b. TRÁFEGO do CE do ciclo nas estadas em outro operador, por classe (conhecimentosresumo)')
        ces = {ce for c, _, _, _ in self.estadas(operador=True) for ce in [(self._gravado(c) or {}).get('ce')] if ce}
        trafego = {ce: (t or '').strip() for ce, t in self._por_lotes(SQL_TRAFEGO, 'ces', ces)}
        contagem, rotulos = defaultdict(Counter), dict(TRAFEGOS)
        for caminho, _, _, nome_classe in self.estadas(operador=True):
            ce = (self._gravado(caminho) or {}).get('ce')
            contagem[nome_classe]['estadas'] += 1
            if not ce:
                contagem[nome_classe]['sem CE no ciclo'] += 1
                continue
            contagem[nome_classe][rotulos.get(trafego.get(ce), 'outro/fora do Mercante')] += 1
            contagem[nome_classe][f'CE emitido fora (≠ {PREFIXO_SANTOS})'] += not ce.startswith(PREFIXO_SANTOS)
        colunas = (['estadas', 'sem CE no ciclo'] + [r for _, r in TRAFEGOS] +
                   ['outro/fora do Mercante', f'CE emitido fora (≠ {PREFIXO_SANTOS})'])
        self.tabela(['classe'] + colunas, [[c] + [contagem[c][col] for col in colunas] for c in CLASSES if contagem[c]])

    def secao_padroes(self, limite: int = 25):
        self.titulo('9. PERCURSOS MAIS COMUNS (OP = operador do desembarque, OP2 = outro operador, SECO = não '
                    'operador)')
        padroes = Counter(padrao(c) for c in self.percursos if any(e.operador for e in c.estadas[1:]))
        self.tabela(['percurso', 'desembarques'], [[p, n] for p, n in padroes.most_common(limite)])

    def _descreve(self, caminho: Percurso) -> List[str]:
        gravado = self._gravado(caminho)
        linhas = [f'{caminho.conteiner} — desembarque {caminho.desembarque.data:%d/%m %H:%M} em '
                  f'{self.nome(caminho.desembarque.recinto)}; corte: {caminho.corte}; gravado: {self._como(gravado)}'
                  + (f' — {gravado["motivo"]}' if gravado and gravado['motivo'] else '')]
        for evento in caminho.eventos[:LIMITE_EVENTOS_EXEMPLO]:
            extras = (' vazio' if evento.vazio else '') + (f' CE {evento.ce_declarado}' if evento.ce_declarado else '')
            linhas.append(f'    {evento.data:%d/%m %H:%M}  {evento.tipo:<11} {self.nome(evento.recinto)}{extras}')
        return linhas

    def secao_exemplos(self):
        self.titulo('10. EXEMPLOS de estadas em outro operador, por classe (para conferir à mão)')
        vistos = Counter()
        for caminho, _, estada, nome_classe in sorted(self.estadas(operador=True),
                                                      key=lambda x: (CLASSES.index(x[3]), x[0].conteiner)):
            if vistos[nome_classe] >= self.amostras:
                continue
            vistos[nome_classe] += 1
            self.escreve()
            self.escreve(f'--- {nome_classe} em {self.nome(estada.recinto)} ({estada.faixa()})')
            for linha in self._descreve(caminho):
                self.escreve('   ' + linha)


def roda(diag: DiagnosticoOperadorTra):
    amostra = diag.secao_volume()
    diag.monta_percursos(amostra)
    for secao in (diag.secao_classes, diag.secao_faixas, diag.secao_pares, diag.secao_gravado, diag.secao_completude,
                  diag.secao_referencia_tra, diag.secao_dtas, diag.secao_trafego, diag.secao_padroes,
                  diag.secao_exemplos):
        secao()
    diag.titulo('TEMPOS DAS CONSULTAS (as 5 mais lentas)')
    for sql, segundos in sorted(diag.tempos, key=lambda t: -t[1])[:5]:
        diag.escreve(f'{segundos:7.1f} s  {sql}')


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--dias', type=int, default=30, help='desembarques dos últimos N dias (padrão 30)')
    parser.add_argument('--ate-dias', type=int, default=7,
                        help='... até N dias atrás: as estadas já tiveram tempo de terminar (padrão 7)')
    parser.add_argument('--conteineres', type=int, default=4000, help='máximo de contêineres analisados')
    parser.add_argument('--exemplos', type=int, default=5, help='exemplos por classe')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    engine = create_engine(uri_do_banco(args))
    agora = datetime.now()
    inicio, fim = agora - timedelta(days=args.dias + args.ate_dias), agora - timedelta(days=args.ate_dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoOperadorTra(conexao, saida, inicio, fim, agora, args.exemplos, args.conteineres)
            diag.escreve(f'Diagnóstico: operador em papel de TRA — {agora:%Y-%m-%d %H:%M}; desembarques de '
                         f'{inicio:%Y-%m-%d} a {fim:%Y-%m-%d}')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
