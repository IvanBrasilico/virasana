# coding: utf-8
import sys

from sqlalchemy import Column, DateTime, func, Integer, BigInteger, CHAR
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from ajna_commons.flask.conf import SQL_URI
from bhadrasana.models.apirecintos import InspecaoNaoInvasiva, EmbarqueDesembarque, AcessoVeiculo
from virasana.integracao.mercante.mercantealchemy import Conhecimento

Base = declarative_base()
metadata = Base.metadata


class CicloDeVidaImportacao(Base):
    __tablename__ = 'ciclodevida_ciclo_de_vida_importacao'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    codRecinto = Column(CHAR(7), index=True)
    numeroConteiner = Column(CHAR(11), index=True)
    numeroCEmercante = Column(CHAR(15), unique=True)
    desembarque_id = Column(BigInteger(), index=True)
    data_desembarque = Column(DateTime(), index=True)
    escaneamento_id = Column(BigInteger(), index=True)
    data_escaneamento = Column(DateTime(), index=True)
    saida_id = Column(BigInteger(), index=True)
    data_saida = Column(DateTime(), index=True)
    entrada_seco_id = Column(BigInteger(), index=True)
    data_entrada_seco = Column(DateTime(), index=True)
    saida_seco_id = Column(BigInteger(), index=True)
    data_saida_seco = Column(DateTime(), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())

    def set_cemercante(self, cemercante: Conhecimento):
        self.numeroCEmercante = cemercante.numeroCEmercante

    def set_desembarque(self, desembarque: EmbarqueDesembarque):
        self.desembarque_id = desembarque.id
        self.data_desembarque = desembarque.dataHoraOcorrencia

    def set_escaneamento(self, escaneamento: InspecaoNaoInvasiva):
        self.escaneamento_id = escaneamento.id
        self.data_escaneamento = escaneamento.dataHoraOcorrencia

    def set_saida(self, saida: AcessoVeiculo):
        self.saida_id = saida.id
        self.data_saida = saida.dataHoraOcorrencia

    def set_entrada_seco(self, entrada_seco: AcessoVeiculo):
        self.entrada_seco_id = entrada_seco.id
        self.data_entrada_seco = entrada_seco.dataHoraOcorrencia

    def set_saida_seco(self, saida_seco: AcessoVeiculo):
        self.saida_seco_id = saida_seco.id
        self.data_saida_seco = saida_seco.dataHoraOcorrencia


TABLES = [CicloDeVidaImportacao.__table__]

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
