import os
import sys
from datetime import datetime, timedelta, time

import pandas as pd

sys.path.append('.')
from virasana.integracao.bagagens.bagagens_risco import importa_cnpjs
from virasana.integracao.due.manager_conteineres_dues import (get_conteineres_escaneados_sem_due,
                                                              set_conteineres_escaneados_sem_due,
                                                              integra_dues, integra_dues_itens)
from virasana.integracao.due.rd_baixa_dues_novas import dues_rd


def exporta_ctrs(session, inicio: datetime, fim: datetime, codigos_recintos: list):
    lista_final = get_conteineres_escaneados_sem_due(session, inicio,
                                                     fim, codigos_recintos)
    df = pd.DataFrame(lista_final[1:], columns=lista_final[0])
    df.to_csv('escaneamentos_sem_due.csv', index=False)


def insert_from_dataframe(session, classe, df):
    try:
        df = df.fillna('').drop_duplicates()
        _list = [classe(**row.to_dict()) for _, row in df.iterrows()]
        session.bulk_save_objects(_list)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Erro ao inserir dados: {e}")
    finally:
        session.close()

def atualiza_acesso_e_mongo(db, session):
    # Passo 5: Atualizar metadata do Mongo por _id e Acesso por id com número da DUE
    # Não tratar erro, se houver, interromper aqui porque senão o passo mais importante ficará para trás
    # Se der erro, operador precisa tratar e identificar, porque esta amarração é a parte mais importante
    df_dues = pd.read_csv('dues.csv')
    df_escaneamentos_sem_due = pd.read_csv('escaneamentos_sem_due.csv')
    set_conteineres_escaneados_sem_due(db, session, df_escaneamentos_sem_due, df_dues)


def importa_dues(session):
    if not os.path.exists('dues.csv'):
        print('Pulando dues - arquivo não existe')
    else:
        # Passo 6: Inserir DUEs a partir do df
        df_dues = pd.read_csv('dues.csv')
        df_dues = df_dues.fillna('').drop_duplicates()
        if integra_dues(session, df_dues):
            os.remove('dues.csv')

    # Passo 6c: Inserir DUEItens a partir do df
    if not os.path.exists('itens_dues.csv'):
        print('Pulando itens_dues - arquivo não existe')
    else:
        df_itens_dues = pd.read_csv('itens_dues.csv')
        df_itens_dues = df_itens_dues.fillna('').drop_duplicates()
        if integra_dues_itens(session, df_itens_dues):
            os.remove('itens_dues.csv')


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from virasana.db import mongodb

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define o fim às 00h de hoje e início 5 dias antes
    fim = datetime.combine(datetime.now(), time.min)
    inicio = fim - timedelta(days=5)

    operacao = input('Qual a operação desejada? (E)exportar lista, (I)mportar DUEs, (R)eceita Data?')

    if operacao == 'E':
        # Passo 1, 2 e 3
        exporta_ctrs(session, inicio, fim, ['8931359'])
    elif operacao == 'I':
        # Passo 5
        atualiza_acesso_e_mongo(mongodb, session)
        # Passo 6
        importa_dues(session)
        # Passo 7: Inserir nomes das empresas na tabela
        importa_cnpjs(session, 'cnpjs_nomes.csv')

    elif operacao == 'R':
        # Passo 4: Recuperar do Receita Data CÓDIGO FAKE DE EXEMPLO!!!
        # Abaixo apenas exemplo, hoje é preciso rodar no Jupyter Notebook do RD na mão
        df = pd.read_csv('escaneamentos_sem_due.csv')
        dues_rd(None, inicio, fim, df['numero_conteiner'].unique(), ['8931359'])
