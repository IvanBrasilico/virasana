# coding: utf-8
"""
Tabelas do risco histórico por CE (SQLAlchemy Core), as mesmas de
`ddl_risco_historico.sql` — a DDL é executada pelo usuário/DBA; o teste
`test_risco_historico_lotes` confere que batem. Os testes criam as tabelas com
`metadata.create_all` no SQLite.

  risco_historico_ce         um CE (não exportação) desde a marca inicial: sinais (JSON) e, no F3, pontos
  risco_historico_versao     uma versão dos parâmetros (constantes, K, N, fichas)
  risco_historico_parametro  peso de cada valor numa versão, com a memória de cálculo
  risco_historico_marca      marcas d'água da extração (ID do CE, alterações, ID das NCMs)
"""
from sqlalchemy import (
    CHAR, VARCHAR, BigInteger, Column, DateTime, Index, Integer, MetaData, Numeric, Table, Text, UniqueConstraint,
)

metadata = MetaData()
_ID = BigInteger().with_variant(Integer, 'sqlite')

CES = Table(
    'risco_historico_ce', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('numeroCEmercante', CHAR(15), nullable=False, unique=True),
    Column('ce_id', BigInteger, nullable=False, index=True),          # conhecimentosresumo.ID
    Column('dataEmissao', VARCHAR(10)),
    Column('sinais', Text, nullable=False),                           # JSON: {dimensão: [valores]}
    Column('ce_alterado_em', DateTime),                               # create_date do CE lido (última alteração)
    Column('ncm_id', BigInteger, nullable=False),                     # maior ncmitemresumo.ID usado (0 = sem NCM)
    Column('criado_em', DateTime, nullable=False, index=True),
    Column('sinais_em', DateTime, nullable=False),
    # F3 (pontuação)
    Column('pontos', Numeric(7, 3)),
    Column('faixa', VARCHAR(10)),
    Column('versao_id', BigInteger, index=True),
    Column('calculado_em', DateTime),
    Index('ix_risco_ce_faixa_pontos', 'faixa', 'pontos'),
)

VERSOES = Table(
    'risco_historico_versao', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('criado_em', DateTime, nullable=False),
    Column('motivo', VARCHAR(60), nullable=False),
    Column('assinatura', CHAR(64), nullable=False),                   # das fichas positivas (id e peso)
    Column('fichas', Integer, nullable=False),
    Column('total_k', Numeric(10, 2), nullable=False),
    Column('total_n', Integer, nullable=False),
    Column('populacao_desde', DateTime, nullable=False),
    Column('constantes', Text, nullable=False),                       # JSON (calculo.constantes)
)

PARAMETROS = Table(
    'risco_historico_parametro', metadata,
    Column('ID', _ID, primary_key=True, autoincrement=True),
    Column('versao_id', BigInteger, nullable=False),
    Column('dimensao', VARCHAR(20), nullable=False),
    Column('valor', VARCHAR(255), nullable=False),
    Column('descricao', VARCHAR(200)),
    Column('fichas', Integer, nullable=False),
    Column('importadores', Integer, nullable=False),
    Column('k', Numeric(10, 2), nullable=False),
    Column('n', Integer, nullable=False),
    Column('vezes', Numeric(14, 2), nullable=False),
    Column('peso', Numeric(6, 3), nullable=False),
    Column('motivo', VARCHAR(60)),                                    # por que o peso é zero
    Column('fichas_ids', Text, nullable=False),
    UniqueConstraint('versao_id', 'dimensao', 'valor', name='uq_risco_parametro'),
)

MARCAS = Table(
    'risco_historico_marca', metadata,
    Column('nome', VARCHAR(60), primary_key=True),
    Column('ultimo_id', BigInteger),
    Column('ultima_data', DateTime),
    Column('atualizado_em', DateTime, nullable=False),
)
