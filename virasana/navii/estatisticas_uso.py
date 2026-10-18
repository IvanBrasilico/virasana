# coding: utf-8
"""
ESTATÍSTICAS DE USO do NAVI-I (aba gerencial; plano aprovado em 17/10/2026 — `CONTEXTO.md`, "Estatísticas de uso").
F2a: o CÁLCULO, sem tela — períodos, métricas, acesso e o universo (com cache). As pesquisas vêm da coleta
(`coleta_uso.py`, Mongo `navii_uso_pesquisas`).

- Períodos civis (mês, trimestre, semestre, ano). O período em curso compara com o MESMO TRECHO do anterior
  (1–17/out x 1–17/set), não com o anterior inteiro.
- Unidade dos cards: o ITEM do Mercante (`Item.ID` — o card: um contêiner num CE). É estável: a carga do Mercante
  insere uma vez e atualiza no lugar. Refinar e rever o mesmo card não conta duas vezes (distintos por item).
- Dois relógios: o USO conta pela data da pesquisa; a COBERTURA, pela data do contêiner — os itens vistos (nas
  pesquisas de MARGEM_COBERTURA antes a MARGEM_COBERTURA depois do período) que estão no universo do período, pela
  mesma regra da contagem da tela (restrita aos CEs vistos: barata).
- Universo: porto da escala (BRSSZ por padrão), janela efetiva, SOMA DE DIAS (medição de 17/10/2026: um mês "frio"
  leva ~130 s numa consulta — mais que os 30 s do gunicorn —, um dia frio até ~7,5 s; a soma dos dias fica ~0,1-0,8%
  acima do período contado de uma vez: a carga do Mercante trata a atualização de escala como inclusão, e o manifesto
  entra nos dias das duas escalas). Guardado em `navii_uso_universo`, um documento por dia — definitivo
  DIAS_ATE_DEFINITIVO dias depois do dia (o ciclo de vida chega atrasado), antes disso recalculado com mais de
  VALIDADE_UNIVERSO. Os mesmos dias servem à seletividade de cada pesquisa. A tela pede os dias que faltam, UM por
  pedaço (um pedaço maior seria uma consulta longa de propósito).
- A cobertura começa no INÍCIO DO REGISTRO do uso (a primeira pesquisa gravada): antes dele ela não é zero, é
  desconhecida — e não se calcula universo de dias sem uso.
- Seletividade: total da pesquisa / universo dos dias da pesquisa, na mesma janela — só das pesquisas na base (com
  período e com `porto_escala` = o porto) e com algum critério além do porto; as sem critério são "navegação".
- Acesso: cada um vê a sua linha; o Supervisor do Fichas (`ovr_perfis_usuarios`) vê os usuários dos seus setores (o
  dele e os subordinados), pelo setor gravado NA PESQUISA (quem mudou de setor fica com o da época); todos veem os
  totais e os setores. Por usuário, ordem alfabética, sem ranking.
"""
import io
import re
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy import CHAR, Column, Integer, MetaData, Table, select

from virasana.navii import acompanhamento_fichas, busca_navii, coleta_uso, tabelas_opcionais

PORTO_PADRAO = busca_navii.PORTO_ESCALA_PADRAO
TIPOS = ('mes', 'trimestre', 'semestre', 'ano')
MESES_DO_TIPO = {'mes': 1, 'trimestre': 3, 'semestre': 6, 'ano': 12}
NOMES_MES = ('jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez')
CODIGOS = {'mes': re.compile(r'^(\d{4})-(\d{2})$'), 'trimestre': re.compile(r'^(\d{4})-T([1-4])$'),
           'semestre': re.compile(r'^(\d{4})-S([12])$'), 'ano': re.compile(r'^(\d{4})$')}
PAUSA_SESSAO = timedelta(minutes=30)
MARGEM_COBERTURA = timedelta(days=30)
JANELA_COBERTURA = 'efetiva'
COLECAO_UNIVERSO = 'navii_uso_universo'
DIAS_ATE_DEFINITIVO = 15
VALIDADE_UNIVERSO = timedelta(hours=6)
MAX_DIAS_SELETIVIDADE = 92        # pesquisa com período mais longo fica fora da seletividade
LOTE_ITENS = 500
PERFIL_SUPERVISOR = 2             # bhadrasana.models.perfilAcesso: Consulta, Operador, Supervisor, Cadastrador
SEM_SETOR = '-'

_metadata = MetaData()
PERFIS = Table('ovr_perfis_usuarios', _metadata, Column('cpf', CHAR(15)), Column('perfil', Integer))


# ---------------------------------------------------------------- períodos

def _fim_do_mes(dia: date) -> date:
    return date(dia.year + dia.month // 12, dia.month % 12 + 1, 1) - timedelta(days=1)


@dataclass(frozen=True)
class Periodo:
    tipo: str
    inicio: date
    fim: date                     # último dia (inclusive)

    @property
    def ordem(self) -> int:
        return (self.inicio.month - 1) // MESES_DO_TIPO[self.tipo] + 1

    @property
    def codigo(self) -> str:
        ano = self.inicio.year
        return {'mes': f'{ano}-{self.inicio.month:02d}', 'trimestre': f'{ano}-T{self.ordem}',
                'semestre': f'{ano}-S{self.ordem}', 'ano': str(ano)}[self.tipo]

    @property
    def rotulo(self) -> str:
        ano = self.inicio.year
        return {'mes': f'{NOMES_MES[self.inicio.month - 1]}/{ano}', 'trimestre': f'{self.ordem}º trimestre de {ano}',
                'semestre': f'{self.ordem}º semestre de {ano}', 'ano': str(ano)}[self.tipo]

    def anterior(self) -> 'Periodo':
        return periodo_que_contem(self.tipo, self.inicio - timedelta(days=1))

    def seguinte(self) -> 'Periodo':
        return periodo_que_contem(self.tipo, self.fim + timedelta(days=1))


def periodo_que_contem(tipo: str, dia: date) -> Periodo:
    meses = MESES_DO_TIPO[tipo]
    primeiro = (dia.month - 1) // meses * meses + 1
    return Periodo(tipo, date(dia.year, primeiro, 1), _fim_do_mes(date(dia.year, primeiro + meses - 1, 1)))


def le_periodo(tipo: str, codigo: Optional[str], hoje: date) -> Periodo:
    """O período do tipo pelo código ('2026-10', '2026-T4', '2026-S2', '2026'); sem código, o que contém hoje."""
    if tipo not in TIPOS:
        raise ValueError(f'Tipo de período inválido: {tipo!r} (use {", ".join(TIPOS)}).')
    if not codigo:
        return periodo_que_contem(tipo, hoje)
    achado = CODIGOS[tipo].match(codigo)
    ordem = int(achado.group(2)) if achado and tipo != 'ano' else 1
    if not achado or (tipo == 'mes' and not 1 <= ordem <= 12) or int(achado.group(1)) < 2000:
        raise ValueError(f'Período inválido: {codigo!r}.')
    mes = ordem if tipo == 'mes' else (ordem - 1) * MESES_DO_TIPO[tipo] + 1
    return periodo_que_contem(tipo, date(int(achado.group(1)), mes, 1))


@dataclass(frozen=True)
class Comparacao:
    atual: Tuple[date, date]
    anterior: Tuple[date, date]
    parcial: bool


def comparacao(periodo: Periodo, hoje: date) -> Comparacao:
    """O trecho do período a contar e o do anterior: em curso, até hoje x o mesmo número de dias do anterior."""
    anterior = periodo.anterior()
    if periodo.inicio <= hoje <= periodo.fim:
        mesmo_trecho = min(anterior.inicio + (hoje - periodo.inicio), anterior.fim)
        return Comparacao((periodo.inicio, hoje), (anterior.inicio, mesmo_trecho), True)
    return Comparacao((periodo.inicio, periodo.fim), (anterior.inicio, anterior.fim), False)


def dias_entre(inicio: date, fim: date) -> List[date]:
    return [inicio + timedelta(days=n) for n in range((fim - inicio).days + 1)]


# ---------------------------------------------------------------- por pesquisa

def _itens(doc: dict, regra) -> Set[int]:
    return {int(item) for item, visto in (doc.get('vistos') or {}).items() if str(item).isdigit() and regra(visto)}


def vistos(doc: dict) -> Set[int]:
    """Itens vistos: com segundos visíveis OU interação (quem clicou viu: examinados ficam dentro dos vistos)."""
    return _itens(doc, lambda v: v.get('s') or v.get('x'))


def examinados(doc: dict) -> Set[int]:
    return _itens(doc, lambda v: v.get('x'))


def sessoes(docs: List[dict]) -> List[List[dict]]:
    """Pesquisas do mesmo usuário e aba, em ordem, cortadas a cada pausa maior que PAUSA_SESSAO."""
    por_aba: Dict[Tuple, List[dict]] = defaultdict(list)
    for doc in docs:
        por_aba[(doc['usuario'], doc.get('aba') or str(doc['_id']))].append(doc)
    resultado = []
    for lista in por_aba.values():
        lista.sort(key=lambda d: d['inicio'])
        atual = [lista[0]]
        for doc in lista[1:]:
            if doc['inicio'] - max(d.get('atualizado') or d['inicio'] for d in atual) > PAUSA_SESSAO:
                resultado.append(atual)
                atual = []
            atual.append(doc)
        resultado.append(atual)
    return resultado


def minutos(sessao: List[dict]) -> float:
    fim = max(d.get('atualizado') or d['inicio'] for d in sessao)
    return max((fim - min(d['inicio'] for d in sessao)).total_seconds() / 60, 0)


def _criterios(filtros: Optional[dict]) -> Set[Tuple[str, str]]:
    criterios = set()
    for campo, valor in (filtros or {}).items():
        for v in (valor if isinstance(valor, list) else [valor]):
            criterios.add((campo, str(v)))
    return criterios


def transicao(antes: dict, depois: dict) -> str:
    """Como a pesquisa mudou em relação à anterior da mesma aba (os critérios são os pares campo/valor)."""
    if (antes.get('janela'), antes.get('periodo')) != (depois.get('janela'), depois.get('periodo')):
        return 'outra janela ou período'
    a, b = _criterios(antes.get('filtros')), _criterios(depois.get('filtros'))
    if a == b:
        return 'repetida'
    if a < b:
        return 'mais critérios'
    return 'menos critérios' if b < a else 'mudança de critérios'


def mediana(valores: List[float]) -> Optional[float]:
    return round(statistics.median(valores), 1) if valores else None


def _pct(parte: float, total: float) -> Optional[float]:
    return round(100 * parte / total, 1) if total else None


# ---------------------------------------------------------------- agregação

@dataclass
class Agregado:
    pesquisas: int = 0
    usuarios: Set[str] = field(default_factory=set)
    sessoes: int = 0
    minutos: float = 0.0
    paginas: int = 0
    enviados: int = 0
    vistos: Set[int] = field(default_factory=set)
    examinados: Set[int] = field(default_factory=set)
    vistos_somados: int = 0
    acionados: int = 0
    fichas_novas: Set = field(default_factory=set)
    profundidades: List[float] = field(default_factory=list)
    origens: Counter = field(default_factory=Counter)
    janelas: Counter = field(default_factory=Counter)
    campos: Counter = field(default_factory=Counter)
    transicoes: Counter = field(default_factory=Counter)

    def numeros(self) -> dict:
        horas = self.minutos / 60
        return {
            'pesquisas': self.pesquisas, 'usuarios': len(self.usuarios), 'sessoes': self.sessoes,
            'horas': round(horas, 1), 'paginas': self.paginas, 'enviados': self.enviados,
            'vistos': len(self.vistos), 'examinados': len(self.examinados), 'repetidos': self.vistos_somados - len(
                self.vistos), 'acionados': self.acionados, 'fichasNovas': len(self.fichas_novas),
            'vistosPorHora': round(len(self.vistos) / horas, 1) if horas else None,
            'examinadosPorVistos': _pct(len(self.examinados), len(self.vistos)),
            'acionadosPorExaminados': _pct(self.acionados, len(self.examinados)),
            'profundidade': mediana(self.profundidades),
        }


def _soma_pesquisa(agregado: Agregado, doc: dict):
    agregado.pesquisas += 1
    agregado.usuarios.add(doc['usuario'])
    agregado.paginas += doc.get('paginas') or 0
    agregado.enviados += doc.get('n_cards') or 0
    itens_vistos = vistos(doc)
    agregado.vistos |= itens_vistos
    agregado.examinados |= examinados(doc)
    agregado.vistos_somados += len(itens_vistos)
    for acao in doc.get('acoes') or []:
        if acao.get('t') == 'ficha' and acao.get('resultado') in ('nova', 'acrescentada'):
            agregado.acionados += 1
            if acao.get('resultado') == 'nova':
                agregado.fichas_novas.add(acao.get('ficha'))
    if doc.get('total'):
        agregado.profundidades.append(100 * len(itens_vistos) / doc['total'])
    agregado.origens[doc.get('origem')] += 1
    agregado.janelas[doc.get('janela')] += 1
    agregado.campos.update((doc.get('filtros') or {}).keys())


def agrega(docs: List[dict]) -> Agregado:
    agregado = Agregado()
    if not docs:
        return agregado
    for sessao in sessoes(docs):
        agregado.sessoes += 1
        agregado.minutos += minutos(sessao)
    por_id = {doc['_id']: doc for doc in docs}
    for doc in docs:
        _soma_pesquisa(agregado, doc)
        anterior = por_id.get(doc.get('anterior'))
        if anterior is not None:
            agregado.transicoes[transicao(anterior, doc)] += 1
    return agregado


def por_dia(docs: List[dict]) -> List[dict]:
    """Série diária (data da pesquisa): pesquisas, usuários e itens vistos distintos no dia."""
    dias: Dict[date, List[dict]] = defaultdict(list)
    for doc in docs:
        dias[doc['inicio'].date()].append(doc)
    return [{'dia': dia.isoformat(), 'pesquisas': len(lista), 'usuarios': len({d['usuario'] for d in lista}),
             'vistos': len(set().union(*(vistos(d) for d in lista)))} for dia, lista in sorted(dias.items())]


# ---------------------------------------------------------------- seletividade

def na_base(doc: dict, porto: str) -> bool:
    periodo = doc.get('periodo') or {}
    return bool(periodo.get('inicio') and periodo.get('fim')) and (doc.get('filtros') or {}).get(
        'porto_escala') == [porto]


def com_criterio(doc: dict) -> bool:
    return any(campo != 'porto_escala' for campo in (doc.get('filtros') or {}))


def dias_da_pesquisa(doc: dict) -> List[date]:
    """Os dias do período da pesquisa; vazio se inválido ou mais longo que MAX_DIAS_SELETIVIDADE."""
    try:
        inicio, fim = (date.fromisoformat(doc['periodo'][k][:10]) for k in ('inicio', 'fim'))
    except (KeyError, TypeError, ValueError):
        return []
    dias = (fim - inicio).days
    return [] if dias < 0 or dias >= MAX_DIAS_SELETIVIDADE else [inicio + timedelta(days=n) for n in range(dias + 1)]


def dias_necessarios(docs: List[dict], porto: str) -> Set[Tuple[str, date]]:
    """(janela, dia) cujo universo diário a seletividade das pesquisas precisa."""
    return {(doc.get('janela'), dia) for doc in docs if na_base(doc, porto) and com_criterio(doc)
            for dia in dias_da_pesquisa(doc)}


def _seletividade_da_pesquisa(doc: dict, diario: Dict[Tuple[str, date], int]) -> Optional[float]:
    """Total / universo dos dias da pesquisa (%); None se faltar o universo de algum dia (ou for zero)."""
    chaves = [(doc.get('janela'), dia) for dia in dias_da_pesquisa(doc)]
    if not chaves or any(chave not in diario for chave in chaves):
        return None
    universo_dos_dias = sum(diario[chave] for chave in chaves)
    return 100 * (doc.get('total') or 0) / universo_dos_dias if universo_dos_dias else None


def seletividade(docs: List[dict], porto: str, diario: Dict[Tuple[str, date], int]) -> dict:
    """Mediana de total / universo dos dias da pesquisa (%), das pesquisas na base com critério."""
    valores, contagem = [], Counter()
    for doc in docs:
        if not na_base(doc, porto):
            contagem['foraDaBase'] += 1
        elif not com_criterio(doc):
            contagem['navegacao'] += 1
        else:
            valor = _seletividade_da_pesquisa(doc, diario)
            if valor is None:
                contagem['semUniverso'] += 1
            else:
                valores.append(valor)
    return {'mediana': mediana(valores), 'pesquisas': len(valores), 'navegacao': contagem['navegacao'],
            'foraDaBase': contagem['foraDaBase'], 'semUniverso': contagem['semUniverso']}


# ---------------------------------------------------------------- universo

def colecao_universo(db):
    return db[COLECAO_UNIVERSO]


def _chave(porto: str, janela: str, inicio: date, fim: date) -> str:
    return f'{porto}|{janela}|{inicio.isoformat()}|{fim.isoformat()}'


@dataclass(frozen=True)
class Pedaco:
    """Um pedaço do universo a calcular: janela e dia (`inicio` = `fim`)."""
    janela: str
    inicio: date
    fim: date

    def como_dict(self) -> dict:
        return {'janela': self.janela, 'inicio': self.inicio.isoformat(), 'fim': self.fim.isoformat()}

    @classmethod
    def de_dict(cls, dados: dict) -> 'Pedaco':
        """O pedaço devolvido pela tela (`pendentes`); janela ou datas inválidas: ValueError."""
        if not isinstance(dados, dict) or dados.get('janela') not in busca_navii.TIPOS_JANELA_VALIDOS:
            raise ValueError('Pedaço do universo inválido.')
        inicio, fim = date.fromisoformat(str(dados.get('inicio'))), date.fromisoformat(str(dados.get('fim')))
        if fim != inicio:
            raise ValueError('Pedaço do universo inválido: um dia por vez.')
        return cls(dados['janela'], inicio, fim)


def _vencido(doc: dict, fim: date, agora: datetime) -> bool:
    """Calculado depois de DIAS_ATE_DEFINITIVO dias do fim: definitivo; antes, vale por VALIDADE_UNIVERSO."""
    calculado = doc['calculado']
    if calculado.date() > fim + timedelta(days=DIAS_ATE_DEFINITIVO):
        return False
    return agora - calculado > VALIDADE_UNIVERSO


def universo_dos_dias(col, porto: str, chaves: Iterable[Tuple[str, date]],
                      agora: datetime) -> Tuple[Dict[Tuple[str, date], int], List[Pedaco]]:
    """({(janela, dia): total} dos dias guardados e válidos, os dias que faltam) — numa consulta só."""
    chaves = sorted(set(chaves), key=lambda c: (str(c[0]), c[1]))
    ids = [_chave(porto, janela, dia, dia) for janela, dia in chaves]
    guardados = {doc['_id']: doc for doc in col.find({'_id': {'$in': ids}})} if ids else {}
    diario, faltam = {}, []
    for (janela, dia), id_ in zip(chaves, ids):
        doc = guardados.get(id_)
        if doc is None or _vencido(doc, dia, agora):
            faltam.append(Pedaco(janela, dia, dia))
        else:
            diario[(janela, dia)] = doc['total']
    return diario, faltam


def universo_guardado(col, porto: str, pedaco: Pedaco, agora: datetime) -> Optional[int]:
    chave = (pedaco.janela, pedaco.inicio)
    return universo_dos_dias(col, porto, [chave], agora)[0].get(chave)


def calcula_universo(session, col, porto: str, pedaco: Pedaco, agora: datetime) -> int:
    """A contagem da tela, sem filtros além do período e do porto da escala, guardada."""
    total = busca_navii.conta_conteineres_navii(session, pedaco.janela, {
        'data_inicio': pedaco.inicio.isoformat(), 'data_fim': pedaco.fim.isoformat(), 'porto_escala': [porto]})
    col.replace_one({'_id': _chave(porto, pedaco.janela, pedaco.inicio, pedaco.fim)},
                    {'porto': porto, **pedaco.como_dict(), 'total': total, 'calculado': agora}, upsert=True)
    return total


def universo(col, porto: str, inicio: date, fim: date, agora: datetime,
             janela: str = JANELA_COBERTURA) -> Tuple[Optional[int], List[Pedaco]]:
    """(soma dos dias guardados, dias que faltam) — a soma é None enquanto faltar algum."""
    diario, faltam = universo_dos_dias(col, porto, [(janela, dia) for dia in dias_entre(inicio, fim)], agora)
    return (None if faltam else sum(diario.values())), faltam


def calcula_pendencias(session, col, porto: str, pendentes: List[Pedaco], agora: datetime,
                       orcamento: float = 5.0, relogio=time.monotonic) -> List[Pedaco]:
    """Calcula os pedaços até estourar o orçamento de segundos (ao menos um); devolve os que faltam — a tela chama de
    novo até zerar (cada chamada cabe no tempo do proxy)."""
    comeco = relogio()
    for n, pedaco in enumerate(pendentes):
        if n and relogio() - comeco > orcamento:
            return pendentes[n:]
        calcula_universo(session, col, porto, pedaco, agora)
    return []


def itens_no_universo(session, porto: str, inicio: date, fim: date, itens: Set[int],
                      janela: str = JANELA_COBERTURA) -> Set[int]:
    """Os itens (vistos) que estão no universo do intervalo — pela mesma regra da contagem da tela, verificada por
    candidato (`busca_navii.itens_na_janela`: barata), em lotes."""
    ids, achados = sorted(itens), set()
    for n in range(0, len(ids), LOTE_ITENS):
        achados |= busca_navii.itens_na_janela(session, janela, inicio.isoformat(), fim.isoformat(), [porto],
                                               ids[n:n + LOTE_ITENS])
    return achados


# ---------------------------------------------------------------- acesso

@dataclass(frozen=True)
class Acesso:
    cpf: str
    supervisor: bool = False
    setores: frozenset = frozenset()          # o dele e os subordinados

    def ve(self, usuario: str, setor: Optional[str]) -> bool:
        return usuario == self.cpf or (self.supervisor and setor in self.setores)


def acesso_do_usuario(session, cpf: str) -> Acesso:
    """Setores (o dele e os subordinados) e se é Supervisor no Fichas; sem as tabelas ou sem cadastro, só ele."""
    if not all(tabelas_opcionais.existe(session, t) for t in ('ovr_usuarios', 'ovr_setores')):
        return Acesso(cpf)
    conexao = session.connection()
    _, setores, _ = acompanhamento_fichas.usuario_e_setores(conexao, cpf)
    supervisor = bool(setores) and tabelas_opcionais.existe(session, PERFIS.name) and conexao.execute(
        select(PERFIS.c.cpf).where(PERFIS.c.cpf == cpf, PERFIS.c.perfil == PERFIL_SUPERVISOR)).first() is not None
    return Acesso(cpf, supervisor, frozenset(setores))


def _nomes(session, cpfs: Set[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """({CPF: nome}, {setor: nome}) do Fichas; vazios sem as tabelas."""
    pessoas, setores = {}, {}
    conexao = session.connection()
    if tabelas_opcionais.existe(session, 'ovr_usuarios') and cpfs:
        pessoas = acompanhamento_fichas._nomes(conexao, cpfs)
    if tabelas_opcionais.existe(session, 'ovr_setores'):
        tabela = acompanhamento_fichas.SETORES
        setores = {str(i).strip(): str(n or '').strip() for i, n in conexao.execute(select(tabela.c.id, tabela.c.nome))}
    return pessoas, setores


# ---------------------------------------------------------------- montagem

PROJECAO = {'cards': 0}                     # a lista dos enviados não entra na conta (os vistos já têm o item)


def pesquisas(col, inicio: date, fim: date) -> List[dict]:
    """As pesquisas feitas entre os dias (inclusive)."""
    return list(col.find({'inicio': {'$gte': datetime.combine(inicio, datetime.min.time()),
                                     '$lt': datetime.combine(fim + timedelta(days=1), datetime.min.time())}},
                         PROJECAO))


def inicio_do_registro(col) -> Optional[date]:
    """O dia da primeira pesquisa gravada (antes dele, a cobertura é desconhecida, não zero)."""
    primeira = next(iter(col.find({}, {'inicio': 1}).sort('inicio', 1).limit(1)), None)
    return primeira['inicio'].date() if primeira else None


def _cobertura(session, col, col_universo, porto: str, intervalo: Tuple[date, date], agora: datetime,
               desde: Optional[date]):
    """({universo, vistos, percentual, examinados, examinadosPorVistos, desde} ou None, pedaços que faltam) — o funil
    todo na base da data do contêiner, a partir do início do registro do uso (`desde`; o intervalo todo antes dele:
    None, sem pendências). Os vistos vêm das pesquisas de MARGEM_COBERTURA antes a MARGEM_COBERTURA depois."""
    inicio, fim = intervalo
    if desde is None or fim < desde:
        return None, []
    inicio = max(inicio, desde)
    total, faltam = universo(col_universo, porto, inicio, fim, agora)
    if total is None:
        return None, faltam
    docs = pesquisas(col, inicio - MARGEM_COBERTURA, min(fim + MARGEM_COBERTURA, agora.date()))
    itens = set().union(*(vistos(d) for d in docs))
    no_universo = itens_no_universo(session, porto, inicio, fim, itens) if itens else set()
    examinados_no_universo = no_universo & set().union(*(examinados(d) for d in docs))
    return {'universo': total, 'vistos': len(no_universo), 'percentual': _pct(len(no_universo), total),
            'examinados': len(examinados_no_universo),
            'examinadosPorVistos': _pct(len(examinados_no_universo), len(no_universo)), 'desde': inicio.isoformat()}, []


def _linhas(docs: List[dict], docs_anteriores: List[dict], chave, nomes: Dict[str, str]) -> List[dict]:
    grupos, anteriores = defaultdict(list), defaultdict(list)
    for doc in docs:
        grupos[chave(doc)].append(doc)
    for doc in docs_anteriores:
        anteriores[chave(doc)].append(doc)
    linhas = [{'codigo': codigo, 'nome': nomes.get(codigo) or codigo, 'atual': agrega(grupos[codigo]).numeros(),
               'anterior': agrega(anteriores[codigo]).numeros()} for codigo in set(grupos) | set(anteriores)]
    return sorted(linhas, key=lambda linha: (linha['nome'] or '').lower())


def _setor(doc: dict) -> str:
    return doc.get('setor') or SEM_SETOR


def _como(agregado: Agregado) -> dict:
    return {'origens': agregado.origens.most_common(), 'janelas': agregado.janelas.most_common(),
            'campos': agregado.campos.most_common(), 'transicoes': agregado.transicoes.most_common()}


def _descreve(periodo: Periodo, comp: Comparacao, hoje: date) -> dict:
    seguinte = periodo.seguinte()
    return {'tipo': periodo.tipo, 'codigo': periodo.codigo, 'rotulo': periodo.rotulo,
            'inicio': comp.atual[0].isoformat(), 'fim': comp.atual[1].isoformat(), 'parcial': comp.parcial,
            'anterior': {'codigo': periodo.anterior().codigo, 'rotulo': periodo.anterior().rotulo,
                         'inicio': comp.anterior[0].isoformat(), 'fim': comp.anterior[1].isoformat()},
            'seguinte': seguinte.codigo if seguinte.inicio <= hoje else None}


def estatisticas(session, db, cpf: str, tipo: str = 'mes', codigo: Optional[str] = None, porto: str = PORTO_PADRAO,
                 agora: Optional[datetime] = None) -> dict:
    """Tudo o que a aba mostra para o usuário `cpf` no período; `pendentes` = pedaços do universo a calcular
    (`calcula_pendencias`) — enquanto houver, a cobertura e a seletividade vêm incompletas."""
    agora = agora or datetime.now()
    periodo = le_periodo(tipo, codigo, agora.date())
    comp = comparacao(periodo, agora.date())
    col, col_universo = coleta_uso.colecao(db), colecao_universo(db)
    docs, docs_anteriores = pesquisas(col, *comp.atual), pesquisas(col, *comp.anterior)
    acesso = acesso_do_usuario(session, cpf)
    desde = inicio_do_registro(col)
    cobertura, faltam = _cobertura(session, col, col_universo, porto, comp.atual, agora, desde)
    cobertura_anterior, faltam_anterior = _cobertura(session, col, col_universo, porto, comp.anterior, agora, desde)
    diario, faltam_dias = universo_dos_dias(col_universo, porto, dias_necessarios(docs, porto), agora)
    visiveis = [d for d in docs if acesso.ve(d['usuario'], d.get('setor'))]
    visiveis_anteriores = [d for d in docs_anteriores if acesso.ve(d['usuario'], d.get('setor'))]
    pessoas, setores = _nomes(session, {d['usuario'] for d in visiveis + visiveis_anteriores})
    atual = agrega(docs)
    pendentes = list(dict.fromkeys(faltam + faltam_anterior + faltam_dias))
    return {
        'periodo': _descreve(periodo, comp, agora.date()), 'porto': porto,
        'uso': {'atual': atual.numeros(), 'anterior': agrega(docs_anteriores).numeros()},
        'cobertura': {'atual': cobertura, 'anterior': cobertura_anterior,
                      'inicioDoRegistro': desde.isoformat() if desde else None},
        'seletividade': seletividade(docs, porto, diario), 'como': _como(atual),
        'setores': _linhas(docs, docs_anteriores, _setor, setores),
        'usuarios': _linhas(visiveis, visiveis_anteriores, lambda d: d['usuario'], pessoas),
        'dias': por_dia(docs), 'acesso': {'supervisor': acesso.supervisor, 'setores': [
            {'codigo': s, 'nome': setores.get(s) or s} for s in sorted(acesso.setores)]},
        'pendentes': [p.como_dict() for p in pendentes],
    }


# ---------------------------------------------------------------- planilha

INDICADORES = (('usuarios', 'Usuários ativos'), ('sessoes', 'Sessões de análise'), ('pesquisas', 'Pesquisas'),
               ('horas', 'Horas de sessão'), ('vistos', 'Cards vistos (distintos)'),
               ('vistosPorHora', 'Cards vistos por hora de sessão'), ('examinados', 'Cards examinados'),
               ('examinadosPorVistos', 'Examinados / vistos (%)'), ('acionados', 'Cards acionados (Abrir Ficha)'),
               ('fichasNovas', 'Fichas novas pelo card'), ('profundidade', 'Profundidade mediana (vistos / total, %)'),
               ('repetidos', 'Cards vistos de novo (refinamento)'))
COLUNAS_LINHA = (('pesquisas', 'Pesquisas'), ('sessoes', 'Sessões'), ('horas', 'Horas'), ('vistos', 'Vistos'),
                 ('vistosPorHora', 'Vistos por hora'), ('examinadosPorVistos', 'Examinados / vistos (%)'),
                 ('acionados', 'Acionados'), ('fichasNovas', 'Fichas novas'))


def variacao(atual, anterior) -> Optional[float]:
    """Variação % do anterior para o atual; None sem um dos dois ou com o anterior zero (a tela diz "novo")."""
    if atual is None or anterior is None or not anterior:
        return None
    return round(100 * (atual - anterior) / anterior, 1)


def _resumo(dados: dict) -> pd.DataFrame:
    atual, anterior = dados['uso']['atual'], dados['uso']['anterior']
    linhas = [(rotulo, atual[chave], anterior[chave], variacao(atual[chave], anterior[chave]))
              for chave, rotulo in INDICADORES]
    cobertura, cobertura_anterior = dados['cobertura']['atual'] or {}, dados['cobertura']['anterior'] or {}
    for chave, rotulo in (('universo', 'Universo do porto (contêineres que chegaram)'),
                          ('vistos', 'Contêineres do universo vistos'), ('percentual', 'Cobertura (%)'),
                          ('examinados', 'Contêineres do universo examinados')):
        valor, valor_anterior = cobertura.get(chave), cobertura_anterior.get(chave)
        linhas.append((rotulo, valor, valor_anterior, variacao(valor, valor_anterior)))
    linhas.append(('Seletividade mediana das pesquisas com critério (%)', dados['seletividade']['mediana'], None, None))
    return pd.DataFrame(linhas, columns=['Indicador', 'Atual', 'Anterior', 'Variação (%)'])


def _tabela(linhas: List[dict], primeira: str) -> pd.DataFrame:
    return pd.DataFrame([[linha['nome']] + [linha['atual'][chave] for chave, _ in COLUNAS_LINHA]
                         + [linha['anterior']['vistos']] for linha in linhas],
                        columns=[primeira] + [rotulo for _, rotulo in COLUNAS_LINHA] + ['Vistos (anterior)'])


def planilha_xlsx(dados: dict) -> bytes:
    """A aba em planilha: Resumo (atual, anterior, variação), Por setor, Por usuário (só os que quem baixa vê) e Por
    dia; a primeira linha do resumo diz o período, o trecho comparado e o porto."""
    periodo = dados['periodo']
    sobre = pd.DataFrame([
        ('Período', f"{periodo['rotulo']} ({periodo['inicio']} a {periodo['fim']})"
                    + (' — em curso' if periodo['parcial'] else '')),
        ('Comparado com', f"{periodo['anterior']['rotulo']} ({periodo['anterior']['inicio']} a "
                          f"{periodo['anterior']['fim']})"),
        ('Porto do universo', dados['porto']),
        ('Universo', 'em cálculo' if dados['pendentes'] else 'completo')], columns=['Item', 'Valor'])
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as escritor:
        sobre.to_excel(escritor, sheet_name='Resumo', index=False)
        _resumo(dados).to_excel(escritor, sheet_name='Resumo', index=False, startrow=len(sobre) + 2)
        _tabela(dados['setores'], 'Setor').to_excel(escritor, sheet_name='Por setor', index=False)
        _tabela(dados['usuarios'], 'Usuário').to_excel(escritor, sheet_name='Por usuário', index=False)
        pd.DataFrame(dados['dias'], columns=['dia', 'pesquisas', 'usuarios', 'vistos']).rename(columns={
            'dia': 'Dia', 'pesquisas': 'Pesquisas', 'usuarios': 'Usuários', 'vistos': 'Cards vistos'}).to_excel(
            escritor, sheet_name='Por dia', index=False)
    return buffer.getvalue()
