import sys

from sqlalchemy import BigInteger, Column, String, UniqueConstraint, Numeric, DateTime

sys.path.append('../bhadrasana2')
from bhadrasana.models import Base


class OrganizacaoSivana(Base):
    __tablename__ = 'sivana_organizacao'
    codigoOrganizacao = Column(String(20), primary_key=True)
    descricao = Column(String(100))
    url = Column(String(100))
    username = Column(String(20))
    password = Column(String(20))
    auth_type = Column(String(4))  # auth = Simple Authentication
    ultima_transmissao = Column(DateTime())
    ultimo_id_transmitido = Column(BigInteger())


class PontoSivana(Base):
    __tablename__ = 'sivana_pontos'
    __table_args__ = (UniqueConstraint('codigoOrganizacao', 'codigoPonto'),)
    id = Column(BigInteger(), primary_key=True)
    codigoOrganizacao = Column(String(10), index=True)
    nomeOrganizacao = Column(String(40), index=True)
    codigoPonto = Column(String(20), index=True)
    codigoUF = Column(String(2), index=True)
    nomeCidade = Column(String(40), index=True)
    endereco = Column(String(100), index=True)
    sentido = Column(String(1), index=True)
    numeroFaixa = Column(String(1), index=True)
    latitude = Column(Numeric(8, 6), index=True)
    longitude = Column(Numeric(9, 6), index=True)


def traduz_chaves_aps(umdict_sivana_ponto):
    ponto_sivana = PontoSivana()
    ponto_sivana.nomeCidade = umdict_sivana_ponto['cidade']
    ponto_sivana.codigoUF = umdict_sivana_ponto['uf']
    ponto_sivana.sentido = umdict_sivana_ponto['sentido']
    ponto_sivana.latitude = umdict_sivana_ponto['latitude'].replace(',', '.')
    ponto_sivana.longitude = umdict_sivana_ponto['longitude'].replace(',', '.')
    ponto_sivana.codigoPonto = umdict_sivana_ponto['Código do equipamento']
    ponto_sivana.numeroFaixa = umdict_sivana_ponto['Número da faixa']
    ponto_sivana.endereco = umdict_sivana_ponto['endereço']
    ponto_sivana.codigoOrganizacao = 'APS'
    ponto_sivana.nomeOrganizacao = umdict_sivana_ponto['empresa']
    return ponto_sivana


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import pandas as pd

    confirma = input('Revisar o código... Esta ação pode apagar TODAS as tabelas. Confirma??')
    if confirma == 'S':
        pass
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    # Sair por segurança. Comentar linha abaixo para funcionar
    # sys.exit(0)
    # Cria tabelas para salvar os pontos de controle do Sivana
    Base.metadata.create_all(engine, [Base.metadata.tables['sivana_pontos'],
                                      Base.metadata.tables['sivana_organizacao'], ])
    # Código pandas para inserir pontos de controle da *APS* na tabela sivana_pontos
    df = pd.read_csv('C:/Users/25052288840/Downloads/EquipamentosAPS.csv',
                     sep=';', header=None)
    dict_pontos = df.set_index(0).to_dict()
    for dict_sivana_ponto in dict_pontos.values():
        ponto = traduz_chaves_aps(dict_sivana_ponto)
        session.add(ponto)
    session.commit()
