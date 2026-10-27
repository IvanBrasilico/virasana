# coding: utf-8
import sys

from sqlalchemy import Column, DateTime, func, Integer, BigInteger, CHAR, ForeignKey, Boolean, Numeric, VARCHAR
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import deferred, relationship

sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from ajna_commons.flask.conf import SQL_URI
from bhadrasana.models.apirecintos import EmbarqueDesembarque, AcessoVeiculo
from virasana.integracao.mercante.mercantealchemy import Conhecimento

import time
import logging

logger = logging.getLogger(__name__)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    # logger.addHandler(handler)

logger.setLevel(logging.INFO)

Base = declarative_base()
metadata = Base.metadata


class CicloDeVidaImportacao(Base):
    __tablename__ = 'ciclodevida_ciclo_de_vida_importacao'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    codigoRecintoDesembarque = Column(CHAR(7), index=True)
    numeroConteiner = Column(CHAR(11), index=True)
    numeroCEmercante = Column(CHAR(15), index=True)
    portoDestFinal = Column(CHAR(7), index=True)
    desembarque_id = Column(BigInteger(), unique=True)
    data_desembarque = Column(DateTime(), index=True)
    saida_id = Column(BigInteger(), index=True)
    data_saida = Column(DateTime(), index=True)
    codigoRecintoArmazenagem = Column(CHAR(7), index=True)
    entrada_seco_id = Column(BigInteger(), index=True)
    data_entrada_seco = Column(DateTime(), index=True)
    saida_seco_id = Column(BigInteger(), index=True)
    data_saida_seco = Column(DateTime(), index=True)
    concluido = Column(Boolean(), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())

    # Ciclo de Vida v2 (ddl_ciclo_de_vida_v2.sql; gravadas pelo servico_ciclo_de_vida). Carregadas sob demanda
    # (grupo 'v2'): as telas e o loop antigo não as selecionam; quem as mostra usa undefer_group('v2').
    situacao = deferred(Column(VARCHAR(12), index=True), group='v2')
    fluxo = deferred(Column(VARCHAR(12), index=True), group='v2')
    motivo = deferred(Column(VARCHAR(200)), group='v2')
    confianca_ce = deferred(Column(VARCHAR(5)), group='v2')
    vazio = deferred(Column(Boolean(), index=True), group='v2')
    numeroManifesto = deferred(Column(CHAR(15), index=True), group='v2')
    escala = deferred(Column(CHAR(15)), group='v2')
    tipoConteiner = deferred(Column(CHAR(4)), group='v2')
    oog = deferred(Column(Boolean(), index=True), group='v2')
    peso_balanca = deferred(Column(Numeric(9, 2)), group='v2')
    peso_manifesto = deferred(Column(Numeric(11, 2)), group='v2')
    tara = deferred(Column(Numeric(9, 2)), group='v2')
    peso_menos_tara = deferred(Column(Numeric(11, 2)), group='v2')
    embarque_id = deferred(Column(BigInteger()), group='v2')
    data_embarque = deferred(Column(DateTime()), group='v2')
    entrada_seco_implicita_de = deferred(Column(VARCHAR(10)), group='v2')
    versao_heuristica = deferred(Column(VARCHAR(10)), group='v2')
    processado_em = deferred(Column(DateTime(), index=True), group='v2')

    escaneamentos = relationship(
        'CicloDeVidaImportacaoEscaneamentos',
        back_populates="ciclo_de_vida",
        cascade="all, delete-orphan",
        lazy="selectin"
    )

    def set_cemercante(self, cemercante: Conhecimento):
        if cemercante is None:
            return
        self.numeroCEmercante = cemercante.numeroCEmercante
        self.portoDestFinal = cemercante.portoDestFinal

    def set_desembarque(self, desembarque: EmbarqueDesembarque):
        if desembarque is None:
            return
        self.desembarque_id = desembarque.id
        self.data_desembarque = desembarque.dataHoraOcorrencia
        self.codigoRecintoDesembarque = desembarque.codigoRecinto
        if self.numeroConteiner is None:
            self.numeroConteiner = desembarque.numeroConteiner

    def set_saida(self, saida: AcessoVeiculo):
        if saida is None:
            return
        self.saida_id = saida.id
        self.data_saida = saida.dataHoraOcorrencia

    def set_entrada_seco(self, entrada_seco: AcessoVeiculo):
        if entrada_seco is None:
            return
        self.entrada_seco_id = entrada_seco.id
        self.data_entrada_seco = entrada_seco.dataHoraOcorrencia
        self.codigoRecintoArmazenagem = entrada_seco.codigoRecinto

    def set_saida_seco(self, saida_seco: AcessoVeiculo):
        if saida_seco is None:
            return
        self.saida_seco_id = saida_seco.id
        self.data_saida_seco = saida_seco.dataHoraOcorrencia


class CicloDeVidaImportacaoEscaneamentos(Base):
    __tablename__ = 'ciclodevida_ciclo_de_vida_importacao_escaneamentos'

    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    ciclo_de_vida_id = Column(
        BigInteger(),
        ForeignKey('ciclodevida_ciclo_de_vida_importacao.ID', ondelete='CASCADE'),
        index=True,
        nullable=False
    )
    escaneamento_id = Column(BigInteger(), index=True, nullable=False)
    data_escaneamento = Column(DateTime(), index=True, nullable=False)
    imagem_id = Column(CHAR(24), index=True)
    # Foi retirado unique de escaneamento_id e imagem_id porque alguns casos o mesmo contêiner
    # tem vários desembarques informados... ????
    # v2 (sob demanda, ver CicloDeVidaImportacao)
    confianca = deferred(Column(VARCHAR(5)), group='v2')
    motivo = deferred(Column(VARCHAR(200)), group='v2')
    codigoRecinto = deferred(Column(CHAR(7)), group='v2')
    fora = deferred(Column(Boolean()), group='v2')

    ciclo_de_vida = relationship(
        "CicloDeVidaImportacao",
        back_populates="escaneamentos"
    )


class CicloDeVidaCE(Base):
    """v2: todos os CEs da família (master/filhotes) com o contêiner nos itens, por ciclo — o NAVI-I acha o
    ciclo pelo card de qualquer um deles. Gravada pelo servico_ciclo_de_vida (o loop antigo não a preenche)."""
    __tablename__ = 'ciclodevida_ciclo_ce'

    ID = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    ciclo_de_vida_id = Column(BigInteger(), ForeignKey('ciclodevida_ciclo_de_vida_importacao.ID', ondelete='CASCADE'),
                              nullable=False)
    numeroCEmercante = Column(CHAR(15), index=True, nullable=False)
    papel = Column(VARCHAR(8), nullable=False)
    principal = Column(Boolean(), nullable=False)
    confianca = Column(VARCHAR(5))
    create_date = Column(TIMESTAMP, server_default=func.current_timestamp())


TABLES = [CicloDeVidaImportacaoEscaneamentos.__table__,
          CicloDeVidaImportacao.__table__]

if __name__ == '__main__':
    confirma = input('Recriar todas as tabelas ** APAGA TODOS OS DADOS ** (S/N)')
    if confirma != 'S':
        exit('Saindo... (só recrio se digitar "S", digitou %s)' % confirma)
    print('Recriando tabelas, aguarde...')
    # engine = create_engine('mysql+pymysql://ivan@localhost:3306/mercante')
    banco = input('Escolha a opção de Banco (1 - MySQL/ 2 - Sqlite)')
    if banco == '1':
        engine = create_engine(SQL_URI)
        metadata.drop_all(engine, tables=TABLES)
        metadata.create_all(engine, tables=TABLES)
    if banco == '2':
        engine = create_engine('sqlite:///teste.db')
        metadata.drop_all(engine, tables=TABLES)
        metadata.create_all(engine, tables=TABLES)
