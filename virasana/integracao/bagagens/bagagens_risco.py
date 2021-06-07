import sys

import pandas as pd
from bhadrasana.models.laudo import Empresa, get_empresa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.conf import SQL_URI
from virasana.integracao.bagagens.viajantesalchemy import Viagem


def pessoa_bagagens_sem_info(con, opcao:str):
    adicoes_sql = {
        'viagem': 'AND c.consignatario NOT IN (SELECT DISTINCT cpf from bagagens_viagens)',
        'pessoa': 'AND c.consignatario NOT IN (SELECT DISTINCT cnpj from laudo_empresas)'
    }
    sql_novos_viajantes = """SELECT DISTINCT c.consignatario as cpf_cnpj FROM itensresumo i
    INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
    WHERE NCM LIKE '9797%' AND c.tipoTrafego = 5
    AND c.dataEmissao >= '2021-04-01'"""
    sql_novos_viajantes += adicoes_sql[opcao.lower()]
    df = pd.read_sql(sql_novos_viajantes, con)
    df.to_csv(f'{opcao}.csv')


def importa_viagens(session):
    df = pd.read_csv('viagens.csv')
    df = df.fillna('')
    for index, row in df.iterrows():
        viagem = Viagem()
        viagem.cpf = str(row['cpf']).zfill(11)
        viagem.data_chegada = row['apvj_apva_apvo_dt_chegada']
        viagem.origem = row['apvj_cd_local_embarque']
        viagem.destino = row['apvj_cd_local_destino']
        viagem.localizador = row['apvj_cd_loc_reserva']
        viagem.voo = row['apvj_apva_apvo_nr_voo']
        session.add(viagem)
    session.commit()


def importa_pessoas(session, fisica=True):
    if fisica:
        df = pd.read_csv('pessoas.csv')
    else:
        df = pd.read_csv('empresas.csv')
    for index, row in df.iterrows():
        if fisica:
            cnpj = row['cpf']
        else:
            cnpj = row['cnpj']
        try:
            empresa = get_empresa(session, cnpj)
        except ValueError:
            empresa = None
        if empresa is None:
            empresa = Empresa()
        empresa.cnpj = cnpj
        empresa.nome = row['nome']
        session.add(empresa)
    session.commit()


if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    opcao = input('Escola (E)xportar ou (I)mportar dados.'
                  'Os arquivos serão criados/procurados no diretório atual.')
    if opcao.lower() == 'i':
        print('Importando viagens...')
        importa_viagens(session)
        print('Importando pessoas físicas...')
        importa_pessoas(session)
        print('Importando pessoas juridicas...')
        importa_pessoas(session, fisica=False)
    else:
        print('Exportando listas de CFPs de CEs de bagagem sem informação...')
        print('Exportando CPFs/CNPJs sem viagens...')
        pessoa_bagagens_sem_info(engine, 'viagem')
        print('Exportando CPFs/CNPJs sem nomes...')
        pessoa_bagagens_sem_info(engine, 'pessoa')
        print('Exportando CPFs sem DIRFs...')
        print('Exportando CEs sem DSIs...')
        print('Listas devem ser fornecidas ao notebook que faz as consultas no RD.')
