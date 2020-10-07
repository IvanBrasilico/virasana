# coding: utf-8
from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import Column, DateTime, func, Integer, VARCHAR, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

# from sqlalchemy.orm import relationship
# from sqlachemy import ForeignKey

Base = declarative_base()
metadata = Base.metadata


class GMCI(Base):
    __tablename__ = 'dte_gmcis'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    cod_recinto = Column(Integer(), index=True)
    num_conteiner = Column(VARCHAR(20), index=True)
    num_gmci = Column(Integer(), unique=True)
    data_dt = Column(DateTime, index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())


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
