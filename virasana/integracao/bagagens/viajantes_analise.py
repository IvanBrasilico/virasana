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


sql_bagagens_e_viajante = """
SELECT DISTINCT c.consignatario as cpf_cnpj, c.portoDestFinal, b.codigo_vu, 
c.portoOrigemCarga FROM itensresumo i 
INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
LEFT JOIN bagagens_viagens b ON b.cpf = c.consignatario 
WHERE NCM LIKE '9797%%' AND c.tipoTrafego = 5 
AND c.dataEmissao >= '2021-03-01' 
"""

AEROPORTOS_BRASILEIROS = [
    'CGH', 'GRU', 'RAO', 'SJP', 'MII', 'BAU', 'CPQ', 'GIG', 'LDB', 'BPS',
    'SDU', 'BHZ', 'UDI', 'GYN', 'BSB', 'CWB', 'FLN', 'JOI', 'BNU', 'ITJ', 'CXJ',
    'POA', 'FOR', 'CNF', 'VCP', 'VIX', 'IGU', 'SSA', 'MGF', 'CGB', 'REC', 'IOS'
]

if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    df = pd.read_sql(sql_ultima_viagem, engine)
    df_ces_vu = pd.read_sql(sql_bagagens_e_viajante, engine)
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

    print('Considerando os CEs com data de emissão a partir de 01/03/2021:')
    sum_ces = len(df_ces_vu)
    sum_ces_sem = sum(df_ces_vu.codigo_vu.isna())
    df_ces_vu_us = df_ces_vu[df_ces_vu.portoOrigemCarga.str.startswith('US')]
    sum_ces_us = len(df_ces_vu_us)
    sum_ces_sem_us = sum(df_ces_vu_us.codigo_vu.isna())
    print(f'Total de CEs de bagagens: {sum_ces}')
    print(f'Total de CEs de bagagens com viajantes sem viagens: {sum_ces_sem} '
          f'({sum_ces_sem*100/sum_ces:0.0f}%)')
    print(f'Total de CEs de bagagens US: {sum_ces_us} ({sum_ces_us*100/sum_ces:0.0f}%)')
    print(f'Total de CEs de bagagens US com viajantes sem viagens: {sum_ces_sem_us} '
          f'({sum_ces_sem_us*100/sum_ces_us:0.0f}%)')

