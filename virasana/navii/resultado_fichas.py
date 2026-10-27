# coding: utf-8
"""
FICHAS E RESULTADO no período — F3 das estatísticas de uso (plano aprovado em 17/10/2026, `CONTEXTO.md`,
"Estatísticas de uso"). Somente leitura das tabelas do Fichas (declaradas aqui com as colunas lidas, como em
`acompanhamento_fichas`; `test_navii_resultado_fichas` confere contra os models).

Fichas de Repressão na importação (`tipooperacao = 1`) ABERTAS no período (`datahora`; sem ela, `create_date`), por
SETOR e por ORIGEM:
- 'navii': abertas pelo card do NAVI-I (flag NAVI-I — só a Ficha nova; o contêiner acrescentado a uma Ficha de outra
  origem não a torna do NAVI-I);
- 'provavel': as demais cujo CE foi VISTO no NAVI-I por QUEM ABRIU a Ficha até MARGEM_ATRIBUICAO antes da abertura
  (coleta de uso: card visto = segundos ou clique, a mesma regra de `estatisticas_uso.vistos`);
- 'demais'.
A comparação NAVI-I x demais é DENTRO do setor: na medição de 17/10/2026, 75% das Fichas tipo 1 eram da conferência
de DTA da SACIT (sem verificação nem resultado); a repressão de fato (EQRIMP) tinha verificação em 85% e TG ou
resultado em 56% em um ano.

Por grupo: abertas; VERIFICAÇÃO física informada (evento especial 2 não excluído) em até PRAZO_VERIFICACAO dias e TG
ou RESULTADO (`ovr_resultados`) em até PRAZO_RESULTADO dias — só entre as Fichas abertas há esse prazo ou mais (as
mais novas estão "em maturação": a verificação estabiliza em ~30 dias; o resultado ainda cresce em um ano, e 180 dias
pegam ~80% do que chega em um ano); e, até hoje: com resultado, encerradas sem resultado (fase >= 3), em andamento,
valor do TG e do perdimento.
"""
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set

from sqlalchemy import BigInteger, CHAR, Column, DateTime, Integer, MetaData, Numeric, Table, VARCHAR, func, select

from virasana.navii import acompanhamento_fichas, tabelas_opcionais

TIPO_REPRESSAO_IMPORTACAO = 1
FASE_ENCERRADA = 3
VERIFICACAO_INFORMADA = 2         # EventoEspecial.RVF
PERDIMENTO = 2                    # TipoResultado.Perdimento
FLAG_NAVII = 'NAVI-I'
PRAZO_VERIFICACAO = 30
PRAZO_RESULTADO = 180
MARGEM_ATRIBUICAO = timedelta(days=30)
ORIGENS = ('navii', 'provavel', 'demais')

_metadata = MetaData()
OVRS = Table('ovr_ovrs', _metadata,
             Column('id', BigInteger, primary_key=True), Column('tipooperacao', Integer), Column('fase', Integer),
             Column('setor_id', CHAR(15)), Column('user_name', VARCHAR(14)), Column('numeroCEmercante', VARCHAR(15)),
             Column('datahora', DateTime), Column('create_date', DateTime))
TGS = Table('ovr_tgovr', _metadata, Column('id', BigInteger, primary_key=True), Column('ovr_id', BigInteger),
            Column('create_date', DateTime), Column('valor', Numeric(12, 2)))
RESULTADOS = Table('ovr_resultados', _metadata, Column('id', BigInteger, primary_key=True),
                   Column('ovr_id', BigInteger), Column('create_date', DateTime), Column('tipo_resultado', Integer),
                   Column('valor', Numeric(12, 2)))
EVENTOS, TIPOS_EVENTO = acompanhamento_fichas.EVENTOS, acompanhamento_fichas.TIPOS_EVENTO
FLAGS, FLAGS_OVR = acompanhamento_fichas.FLAGS, acompanhamento_fichas.FLAGS_OVR
TABELAS = ('ovr_ovrs', 'ovr_eventos', 'ovr_tiposevento', 'ovr_flags', 'ovr_flags_ovr', 'ovr_tgovr', 'ovr_resultados')


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


@dataclass
class Ficha:
    id: int
    setor: str
    usuario: str
    ce: str
    aberta: datetime
    fase: int
    navii: bool = False
    verificacao: Optional[datetime] = None
    tg: Optional[datetime] = None
    resultado: Optional[datetime] = None
    valor_tg: float = 0.0
    valor_perdimento: float = 0.0

    @property
    def com_resultado(self) -> bool:
        return bool(self.tg or self.resultado)

    @property
    def primeiro_resultado(self) -> Optional[datetime]:
        return min((d for d in (self.tg, self.resultado) if d), default=None)


# ---------------------------------------------------------------- leitura

def _fichas_abertas(conexao, inicio: date, fim: date) -> Dict[int, Ficha]:
    aberta = func.coalesce(OVRS.c.datahora, OVRS.c.create_date)
    consulta = select(OVRS.c.id, OVRS.c.setor_id, OVRS.c.user_name, OVRS.c.numeroCEmercante, OVRS.c.fase,
                      aberta.label('aberta')).where(
        OVRS.c.tipooperacao == TIPO_REPRESSAO_IMPORTACAO, aberta >= datetime.combine(inicio, datetime.min.time()),
        aberta < datetime.combine(fim + timedelta(days=1), datetime.min.time()))
    return {linha.id: Ficha(linha.id, _texto(linha.setor_id), _texto(linha.user_name), _texto(linha.numeroCEmercante),
                            _data(linha.aberta), linha.fase or 0) for linha in conexao.execute(consulta)}


def _completa(conexao, fichas: Dict[int, Ficha]):
    """Flag NAVI-I, a 1ª verificação física informada não excluída, o 1º TG e o 1º resultado, com os valores."""
    ids = sorted(fichas)
    for (ovr_id,) in conexao.execute(select(FLAGS_OVR.c.rvf_id).join(FLAGS, FLAGS.c.id == FLAGS_OVR.c.flag_id).where(
            FLAGS.c.nome == FLAG_NAVII, FLAGS_OVR.c.rvf_id.in_(ids))):
        fichas[ovr_id].navii = True
    verificacoes = select(EVENTOS.c.ovr_id, func.min(EVENTOS.c.create_date)).join(
        TIPOS_EVENTO, TIPOS_EVENTO.c.id == EVENTOS.c.tipoevento_id).where(
        EVENTOS.c.ovr_id.in_(ids), TIPOS_EVENTO.c.eventoespecial == VERIFICACAO_INFORMADA,
        func.coalesce(EVENTOS.c.excluido, 0) == 0).group_by(EVENTOS.c.ovr_id)
    for ovr_id, quando in conexao.execute(verificacoes):
        fichas[ovr_id].verificacao = _data(quando)
    for ovr_id, quando, valor in conexao.execute(select(
            TGS.c.ovr_id, func.min(TGS.c.create_date), func.sum(TGS.c.valor)).where(
            TGS.c.ovr_id.in_(ids)).group_by(TGS.c.ovr_id)):
        fichas[ovr_id].tg, fichas[ovr_id].valor_tg = _data(quando), float(valor or 0)
    for ovr_id, quando, tipo, valor in conexao.execute(select(
            RESULTADOS.c.ovr_id, RESULTADOS.c.create_date, RESULTADOS.c.tipo_resultado, RESULTADOS.c.valor).where(
            RESULTADOS.c.ovr_id.in_(ids))):
        ficha = fichas[ovr_id]
        ficha.resultado = min(d for d in (ficha.resultado, _data(quando)) if d)
        if tipo == PERDIMENTO:
            ficha.valor_perdimento += float(valor or 0)


def fichas_do_intervalo(session, inicio: date, fim: date) -> List[Ficha]:
    """As Fichas de repressão na importação abertas entre os dias (inclusive), com flag, marcos e valores."""
    conexao = session.connection()
    fichas = _fichas_abertas(conexao, inicio, fim)
    if fichas:
        _completa(conexao, fichas)
    return sorted(fichas.values(), key=lambda f: f.id)


# ---------------------------------------------------------------- atribuição provável

def _ces_vistos(doc: dict) -> Set[str]:
    """CEs dos cards vistos na pesquisa (segundos ou clique — a regra de `estatisticas_uso.vistos`)."""
    itens = {str(item) for item, visto in (doc.get('vistos') or {}).items() if visto.get('s') or visto.get('x')}
    return {_texto(card.get('ce')) for card in doc.get('cards') or [] if str(card.get('i')) in itens and card.get('ce')}


def _vistos_por_usuario(col_coleta, candidatas: List[Ficha]) -> Dict[str, list]:
    """{usuário: [(quando, CEs vistos)]} das pesquisas de quem abriu as Fichas, na janela da atribuição."""
    vistos: Dict[str, list] = defaultdict(list)
    for doc in col_coleta.find({'usuario': {'$in': sorted({f.usuario for f in candidatas})},
                                'inicio': {'$gte': min(f.aberta for f in candidatas) - MARGEM_ATRIBUICAO,
                                           '$lte': max(f.aberta for f in candidatas)}},
                               {'usuario': 1, 'inicio': 1, 'vistos': 1, 'cards.i': 1, 'cards.ce': 1}):
        vistos[doc['usuario']].append((doc['inicio'], _ces_vistos(doc)))
    return vistos


def atribui(fichas: List[Ficha], col_coleta) -> Dict[int, str]:
    """{Ficha: origem}: 'navii' pela flag; 'provavel' se quem abriu viu o CE no NAVI-I até MARGEM_ATRIBUICAO antes."""
    origem = {f.id: ('navii' if f.navii else 'demais') for f in fichas}
    candidatas = [f for f in fichas if not f.navii and f.ce and f.usuario and f.aberta]
    if not candidatas or col_coleta is None:
        return origem
    vistos = _vistos_por_usuario(col_coleta, candidatas)
    for ficha in candidatas:
        if any(ficha.aberta - MARGEM_ATRIBUICAO <= quando <= ficha.aberta and ficha.ce in ces
               for quando, ces in vistos.get(ficha.usuario, ())):
            origem[ficha.id] = 'provavel'
    return origem


# ---------------------------------------------------------------- indicadores

def coorte(fichas: List[Ficha], marco, prazo: int, agora: datetime) -> dict:
    """Entre as Fichas abertas há `prazo` dias ou mais, quantas chegaram ao marco em até `prazo` dias."""
    elegiveis = [f for f in fichas if agora - f.aberta >= timedelta(days=prazo)]
    chegaram = sum(1 for f in elegiveis if marco(f) and marco(f) - f.aberta <= timedelta(days=prazo))
    return {'chegaram': chegaram, 'elegiveis': len(elegiveis),
            'percentual': round(100 * chegaram / len(elegiveis), 1) if elegiveis else None}


def numeros(fichas: List[Ficha], agora: datetime) -> dict:
    sem_resultado = [f for f in fichas if not f.com_resultado]
    return {
        'abertas': len(fichas),
        'verificacao': coorte(fichas, lambda f: f.verificacao, PRAZO_VERIFICACAO, agora),
        'resultado': coorte(fichas, lambda f: f.primeiro_resultado, PRAZO_RESULTADO, agora),
        'comVerificacao': sum(1 for f in fichas if f.verificacao),
        'comResultado': len(fichas) - len(sem_resultado),
        'encerradasSemResultado': sum(1 for f in sem_resultado if f.fase >= FASE_ENCERRADA),
        'emAndamento': sum(1 for f in sem_resultado if f.fase < FASE_ENCERRADA),
        'valorTG': round(sum(f.valor_tg for f in fichas), 2),
        'valorPerdimento': round(sum(f.valor_perdimento for f in fichas), 2),
    }


def _bloco(fichas: List[Ficha], origem: Dict[int, str], agora: datetime) -> dict:
    return {'origens': {o: numeros([f for f in fichas if origem[f.id] == o], agora) for o in ORIGENS},
            'total': numeros(fichas, agora)}


def resumo(session, col_coleta, inicio: date, fim: date, agora: datetime,
           nomes_setores: Optional[Dict[str, str]] = None) -> Optional[dict]:
    """{'setores': [{codigo, nome, origens: {navii, provavel, demais}, total}], 'total': {...}} das Fichas abertas no
    intervalo (setores com mais Fichas primeiro); None sem as tabelas do Fichas."""
    if not all(tabelas_opcionais.existe(session, tabela) for tabela in TABELAS):
        return None
    fichas = fichas_do_intervalo(session, inicio, fim)
    origem = atribui(fichas, col_coleta)
    por_setor: Dict[str, List[Ficha]] = defaultdict(list)
    for ficha in fichas:
        por_setor[ficha.setor].append(ficha)
    nomes_setores = nomes_setores or {}
    setores = [{'codigo': setor, 'nome': nomes_setores.get(setor) or setor or '-', **_bloco(lista, origem, agora)}
               for setor, lista in por_setor.items()]
    setores.sort(key=lambda s: (-s['total']['abertas'], s['nome'].lower()))
    return {'setores': setores, **_bloco(fichas, origem, agora)}
