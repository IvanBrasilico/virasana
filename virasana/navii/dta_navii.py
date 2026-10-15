# coding: utf-8
"""
DTA (Declaração de Trânsito Aduaneiro) no NAVI-I (pedido do usuário, 15/10/2026).

A tabela `navii_dtas` é gravada pela integração com o Contagil
(`virasana/integracao/dta/dta_manager.py`); uma linha por (DTA, CE) — `nr_ce` é o
`numeroCEmercante` do Conhecimento. Ela também traz dados do CE, da escala e
agregados históricos; aqui só se usam os campos DA DECLARAÇÃO (definição Core
parcial, só leitura):

  - filtro "Tem DTA" (`tem_dta`: 'sim'/'nao'): verificado por candidato (índice de `nr_ce`);
  - filtro "Nº DTA" (`dta`): vira a lista dos CEs da DTA (índice da chave única), que dirige a busca como o
    filtro de CE (sem datas, sem limite de período);
  - card: os dados básicos de cada DTA do CE.

Sem a tabela no banco, o card fica sem DTA e os filtros respondem com erro claro.
"""
from collections import defaultdict
from typing import Callable, Dict, Iterable, List

from sqlalchemy import (
    CHAR, VARCHAR, BigInteger, Boolean, Column, DateTime, Integer, MetaData, Numeric, Table, Text, not_, select,
)

from virasana.integracao.mercante.mercantealchemy import Conhecimento
from virasana.navii import tabelas_opcionais

DTAS = Table(
    'navii_dtas', MetaData(),
    Column('id', BigInteger().with_variant(Integer, 'sqlite'), primary_key=True),
    Column('nr_ce', CHAR(15), nullable=False),
    Column('codigo_declaracao_transito', CHAR(10), nullable=False),          # Nº DTA
    Column('descricao_mercadoria_transito', Text),                            # descrição da mercadoria na DTA
    Column('codigo_moeda_negociada', CHAR(3)),                                # moeda
    Column('max_vlr_fob_carga_fatura', Numeric(20, 2)),                       # valor declarado
    Column('descricao_tipo_declaracao_transito', VARCHAR(100)),
    Column('ni_transportador_declaracao', VARCHAR(20)),                       # CNPJ do transportador
    Column('data_registro_declaracao', DateTime),
    Column('data_completa_desembaraco_dh', DateTime),
    Column('indicador_transito_concluido', Boolean),
    Column('data_previsao_chegada_transito', DateTime),
    Column('nome_transportadora', VARCHAR(255)),
)
OPCOES_TEM_DTA = {'sim': 'Com DTA', 'nao': 'Sem DTA'}


def disponivel(session) -> bool:
    return tabelas_opcionais.existe(session, DTAS.name)


def _exige(session):
    if not disponivel(session):
        raise ValueError('DTA indisponível neste banco (tabela navii_dtas ausente).')


def criterio_tem_dta(session, valor: str, existe: Callable):
    """Condição do filtro `tem_dta` para a query do NAVI-I (`existe` = `busca_navii._existe`)."""
    _exige(session)
    tem = existe(DTAS, DTAS.c.nr_ce == Conhecimento.numeroCEmercante)
    return tem if valor == 'sim' else not_(tem)


def ces_das_dtas(session, numeros: Iterable[str]) -> List[str]:
    """CEs das DTAs informadas (filtro `dta`)."""
    _exige(session)
    numeros = sorted(set(numeros))
    if not numeros:
        return []
    linhas = session.connection().execute(
        select(DTAS.c.nr_ce).where(DTAS.c.codigo_declaracao_transito.in_(numeros)).distinct())
    return sorted({(ce or '').strip() for ce, in linhas if ce})


def _dados(linha) -> dict:
    valor = linha.max_vlr_fob_carga_fatura
    return {
        'numero': (linha.codigo_declaracao_transito or '').strip(),
        'tipo': linha.descricao_tipo_declaracao_transito,
        'registro': linha.data_registro_declaracao,
        'desembaraco': linha.data_completa_desembaraco_dh,
        'previsaoChegada': linha.data_previsao_chegada_transito,
        'concluido': bool(linha.indicador_transito_concluido),
        'transportador': linha.nome_transportadora,
        'cnpjTransportador': linha.ni_transportador_declaracao,
        'valor': float(valor) if valor is not None else None,
        'moeda': linha.codigo_moeda_negociada,
        'descricao': linha.descricao_mercadoria_transito,
    }


def dtas_dos_ces(session, ces: Iterable[str]) -> Dict[str, List[dict]]:
    """{CE: [DTA, ...]} (mais recentes primeiro) para os CEs da página; {} sem a tabela."""
    ces = sorted({ce for ce in ces if ce})
    if not ces or not disponivel(session):
        return {}
    resultado: Dict[str, List[dict]] = defaultdict(list)
    for linha in session.connection().execute(
            select(DTAS).where(DTAS.c.nr_ce.in_(ces)).order_by(DTAS.c.data_registro_declaracao.desc())):
        resultado[(linha.nr_ce or '').strip()].append(_dados(linha))
    return dict(resultado)
