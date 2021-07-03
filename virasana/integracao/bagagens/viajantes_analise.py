import pandas as pd
import sys

sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sql_ultima_viagem = '''
select b1.cpf, p.nome, b1.data_chegada, b1.destino from bagagens_viagens b1 
inner join (
select cpf, max(data_chegada) as data_chegada  from bagagens_viagens
group by cpf ) b2
on b1.cpf = b2.cpf and b1.data_chegada = b2.data_chegada
inner join bagagens_pessoas p on p.cpf = b1.cpf
'''

AEROPORTOS_BRASILEIROS = [
    'CGH', 'GRU', 'RAO', 'SJP', 'MII', 'BAU', 'CPQ', 'GIG', 'LDB', 'BPS',
    'SDU', 'BHZ', 'UDI', 'GYN', 'BSB', 'CWB', 'FLN', 'JOI', 'BNU', 'ITJ', 'CXJ',
    'POA', 'FOR', 'CNF', 'VCP', 'VIX', 'IGU', 'SSA', 'MGF', 'CGB', 'REC', 'IOS'
]

if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    df = pd.read_sql(sql_ultima_viagem, engine)
    df_brasil = df[df.destino.isin(AEROPORTOS_BRASILEIROS)]
    df_estrangeiro = df[~ df.destino.isin(AEROPORTOS_BRASILEIROS)]
    print('Análise de viagens finais de viajantes com CEs de 2021.')
    print('Foram considerados aeroportos brasileiros:')
    print(AEROPORTOS_BRASILEIROS)
    print('Lista de passageiros com último vôo para o estrangeiro:')
    print(df_estrangeiro.to_string())
    print('Aeroportos brasileiros frequentes:')
    print(df_brasil.destino.value_counts()[:10])
    print('Aeroportos estrangeiros frequentes:')
    print(df_estrangeiro.destino.value_counts()[:10])
    print(f'Total de {len(df_brasil)} viagens finais para aeroportos brasileiros')
    print(f'Total de {len(df_estrangeiro)} viagens finais para aeroportos estrangeiros '
          f'({int(len(df_estrangeiro)/len(df)*100)}%)')
    print(f'Total de {len(df)} viagens finais')
