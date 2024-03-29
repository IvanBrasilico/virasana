"""Lógicas para tratar conformidade/qualidade das imagens de escaneamento

SQLs para relatórios:

elect cod_recinto, avg(height), std(height), avg(ratio), stddev(ratio),
 min(ratio), max(ratio), count(ratio)
 from ajna_conformidade group by cod_recinto


"""

import sys

sys.path.append('..')
sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import Column, DateTime, func, Integer, VARCHAR, BigInteger, \
    Numeric, Boolean
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Conformidade(Base):
    __tablename__ = 'ajna_conformidade'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    cod_recinto = Column(VARCHAR(20), index=True)
    id_imagem = Column(VARCHAR(40), unique=True)
    width = Column(Integer(), index=True)
    height = Column(Integer(), index=True)
    ratio = Column(Numeric(10, 2), index=True)
    num_gmci = Column(Integer(), index=True)
    uploadDate = Column(DateTime, index=True)
    dataescaneamento = Column(DateTime, index=True)
    numeroinformado = Column(VARCHAR(11), index=True)  # Número contêiner
    laplacian = Column(Integer(), index=True)
    bbox_classe = Column(Integer(), index=True)
    bbox_score = Column(Numeric(4, 2), index=True)
    tipotrafego = Column(VARCHAR(3), index=True)  # Importação ou exportação
    vazio = Column(Boolean(), index=True)
    isocode_size = Column(VARCHAR(2), index=True)
    isocode_group = Column(VARCHAR(2), index=True)
    paisdestino = Column(VARCHAR(2), index=True)
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())

    def set_size(self, size):
        self.width = size[0]
        self.height = size[1]
        self.ratio = self.width / self.height


def get_isocode_sizes(session):
    sql_sizes = 'SELECT distinct(isocode_size) FROM dbmercante.ajna_conformidade'
    return session.execute(sql_sizes)


def get_isocode_sizes_choices(session):
    rs = get_isocode_sizes(session)
    lista = sorted([(str(row[0]), str(row[0])) for row in rs], key=lambda x: x[1])
    return [('', '-'), *lista]


def get_isocode_groups(session):
    sql_groups = 'SELECT distinct(isocode_group) FROM dbmercante.ajna_conformidade'
    return session.execute(sql_groups)


def get_isocode_groups_choices(session):
    rs = get_isocode_groups(session)
    lista = sorted([(row[0], str(row[0])) for row in rs], key=lambda x: x[1])
    return [('', '-'), *lista]


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
