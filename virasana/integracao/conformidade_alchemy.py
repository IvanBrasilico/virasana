# coding: utf-8
import sys
sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import Column, DateTime, func, Integer, VARCHAR, BigInteger, \
    Numeric
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Conformidade(Base):
    __tablename__ = 'ajna_conformidade'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    cod_recinto = Column(Integer(), index=True)
    id_imagem = Column(VARCHAR(40), index=True)
    width = Column(Integer(), unique=True)
    height = Column(Integer(), unique=True)
    ratio = Column(Numeric(10, 2), index=True)
    num_gmci = Column(Integer(), unique=True)
    datahora = Column(DateTime, index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())

    def set_size(self, size):
        self.width = size[0]
        self.height = size[1]
        self.ratio = self.width + self.ratio

if __name__ == '__main__':
    confirma = input('Recriar todas as tabelas ** APAGA TODOS OS DADOS ** (S/N)')
    if confirma != 'S':
        exit('Saindo... (só recrio se digitar "S", digitou %s)' % confirma)
    print('Recriando tabelas, aguarde...')
    # engine = create_engine('mysql+pymysql://ivan@localhost:3306/mercante')
    banco = input('Escolha a opção de Banco (1 - MySQL/ 2 - Sqlite)')
    if banco == '1':
        engine = create_engine(SQL_URI)
        metadata.drop_all(engine)
        metadata.create_all(engine)
    if banco == '2':
        engine = create_engine('sqlite:///teste.db')
        metadata.drop_all(engine)
        metadata.create_all(engine)
