import sys

from sqlalchemy import Column, CHAR, \
    DateTime, Integer, BigInteger, String
from sqlalchemy import create_engine
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


class Pessoa(Base):
    __tablename__ = 'bagagens_pessoas'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    cpf = Column(CHAR(11), index=True)
    data_nascimento = Column(DateTime(), index=True)
    nome = Column(String(50), index=True)
    endereco = Column(String(200), index=True)

class DSI(Base):
    __tablename__ = 'bagagens_dsi'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    numero = Column(CHAR(11), index=True)
    consignatario = Column(CHAR(11), index=True)
    despachante = Column(CHAR(11), index=True)
    descricao = Column(String(1000), index=True)


class Descartado(Base):
    __tablename__ = 'risco_descartados'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    numeroCEmercante = Column(CHAR(15), index=True)


if __name__ == '__main__':
    confirma = input('Recriar todas as tabelas ** APAGA TODOS OS DADOS ** (S/N)')
    if confirma != 'S':
        exit('Saindo... (só recrio se digitar "S", digitou %s)' % confirma)
    print('Recriando tabelas, aguarde...')
    # engine = create_engine('mysql+pymysql://ivan@localhost:3306/mercante')
    banco = input('Escolha a opção de Banco (1 - MySQL/ 2 - Sqlite)')
    if banco == '1':
        engine = create_engine(SQL_URI)
        # metadata.drop_all(engine)
        metadata.create_all(engine)
    if banco == '2':
        engine = create_engine('sqlite:///teste.db')
        metadata.drop_all(engine)
        metadata.create_all(engine)
