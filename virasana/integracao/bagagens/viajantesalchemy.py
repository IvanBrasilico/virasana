import sys
from enum import Enum

from sqlalchemy import Column, CHAR, \
    DateTime, Integer, BigInteger, String, func
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.conf import SQL_URI

Base = declarative_base()
metadata = Base.metadata


class Viagem(Base):
    __tablename__ = 'bagagens_viagens'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    cpf = Column(CHAR(11), index=True)
    data_chegada = Column(DateTime(), index=True)
    origem = Column(CHAR(3), index=True)
    destino = Column(CHAR(3), index=True)
    localizador = Column(CHAR(10), index=True)
    voo = Column(CHAR(10), index=True)
    codigo_vu = Column(CHAR(17), index=True)


class Pessoa(Base):
    __tablename__ = 'bagagens_pessoas'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    cpf = Column(CHAR(11), index=True)
    data_nascimento = Column(DateTime(), index=True)
    nome = Column(String(50), index=True)
    endereco = Column(String(200), index=True)
    cep = Column(CHAR(9), index=True)


class DSI(Base):
    __tablename__ = 'bagagens_dsi'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    numero = Column(CHAR(11), unique=True)
    data_registro = Column(DateTime, index=True)
    consignatario = Column(CHAR(11), index=True)
    despachante = Column(CHAR(11), index=True)
    numeroCEmercante = Column(CHAR(15), index=True)
    descricao = Column(String(1000), index=True)


class ClasseRisco(Enum):
    NAO_CLASSIFICADO = 0
    VERDE = 1
    AMARELO = 2
    VERMELHO = 3
    SAFIA = 4
    REPRESSAO = 5


class ClassificacaoRisco(Base):
    __tablename__ = 'risco_classificacao'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    numeroCEmercante = Column(CHAR(15), unique=True)
    classerisco = Column(Integer(), index=True)
    descricao = Column(CHAR(200), index=True)
    user_name = Column(CHAR(14), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())

    @property
    def classerisco_name(self):
        return ClasseRisco(self.classerisco).name


if __name__ == '__main__':
    confirma = input('Recriar todas as tabelas ** APAGA TODOS OS DADOS ** (S/N)')
    if confirma != 'S':
        exit('Saindo... (só recrio se digitar "S", digitou %s)' % confirma)
    print('Recriando tabelas, aguarde...')
    # engine = create_engine('mysql+pymysql://ivan@localhost:3306/mercante')
    banco = input('Escolha a opção de Banco (1 - MySQL/ 2 - Sqlite)')
    if banco == '1':
        engine = create_engine(SQL_URI)
        metadata.drop_all(engine, [metadata.tables['risco_classificacao']])
        metadata.create_all(engine)
    if banco == '2':
        engine = create_engine('sqlite:///teste.db')
        metadata.drop_all(engine)
        metadata.create_all(engine)
