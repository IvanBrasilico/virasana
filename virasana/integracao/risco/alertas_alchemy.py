"""Lógicas para tratar alertas de fontes diversas

"""

import sys
from enum import Enum

sys.path.append('..')
sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import Column, DateTime, func, Integer, VARCHAR, BigInteger
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class NivelAlerta(Enum):
    Baixo = 1
    Medio = 2
    Moderado = 3
    Alto = 5
    Critico = 8


cor_nivel = {
    NivelAlerta.Baixo: "#00ff00",
    NivelAlerta.Medio: "#66ffff",
    NivelAlerta.Moderado: "#ffff00",
    NivelAlerta.Alto: "#ff3300",
    NivelAlerta.Critico: "#660066"
}


class Alerta(Base):
    """Caso a imagem incida em um apontamento, é gerado um alerta

    """
    __tablename__ = 'ajna_alertas'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    origem = Column(BigInteger().with_variant(Integer, 'sqlite'), index=True)
    nivel = Column(Integer(), index=True)
    cod_recinto = Column(VARCHAR(20), index=True)
    id_imagem = Column(VARCHAR(40), unique=True)
    dataescaneamento = Column(DateTime, index=True)
    numeroinformado = Column(VARCHAR(11), index=True)  # Número contêiner
    informacoes = Column(VARCHAR(1000), index=True)  # Texto resumindo o alerta
    create_date = Column(TIMESTAMP, index=True,
                         server_default=func.current_timestamp())
    last_modified = Column(DateTime, index=True,
                           onupdate=func.current_timestamp())


class Apontamentos(Base):
    """Os apontamentos são rodados periodicamente na base. Se encontrados, geram um alerta.

    """
    __tablename__ = 'ajna_apontamentos'
    ID = Column(BigInteger().with_variant(Integer, 'sqlite'),
                primary_key=True, autoincrement=True)
    nome = Column(VARCHAR(50), index=True)
    nome_classe = Column(VARCHAR(50), index=True)
    nome_campo = Column(VARCHAR(50), index=True)
    valor_campo = Column(VARCHAR(50), index=True)
    nivel = Column(Integer(), index=True)
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
