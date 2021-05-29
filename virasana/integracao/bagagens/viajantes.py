import os

import pandas as pd
from ajna_commons.flask.conf import SQL_URI
from integracao.bagagens.viajantesalchemy import Viagem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def exporta_viajantes(con):
    sql_novos_viajantes = '''SELECT DISTINCT c.consignatario as cpf FROM itensresumo i
    INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
    WHERE NCM LIKE '9797%' AND c.tipoBLConhecimento IN(10, 12, 15) AND c.tipoTrafego = 5
    AND c.dataEmissao >= '2021-04-01
    and c.consignatario not in (select distinct cpf from bagagens_viagens)'''
    df = pd.read_sql(sql_novos_viajantes, con)
    df.to_csv('viajantes.csv')


def importa_viagens(session):
    df = pd.read_csv('viagens.csv', session)
    for index, row in df.itertuples():
        viagem = Viagem()
        viagem.cpf = row['cpf']
        viagem.data_chegada = row['apvj_apva_apvo_dt_chegada']
        viagem.origem = row['apvj_cd_local_embarque']
        viagem.destino = row['apvj_cd_local_destino']
        viagem.localizador = row['apvj_cd_loc_reserva']
        viagem.voo = row['apvj_apva_apvo_nr_voo']
        session.add(viagem)
    session.commit()


if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    if os.path.exists('viagens.csv'):
        print('Importando viagens...')
        importa_viagens(session)
    else:
        print('Exportando viajantes...')
        exporta_viajantes(session)

