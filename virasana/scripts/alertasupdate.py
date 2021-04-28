"""Processamento de alertas cadastrados

Script de linha de comando para processar alertas (tabela para janela única
de tratamento de resultados de seleções [internas ou externas] ou algoritmos)

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import datetime
import os
import sys

import click
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append('.')
from ajna_commons.flask.conf import MONGODB_URI, SQL_URI, DATABASE
from virasana.integracao.risco.alertas_manager import processa_apontamentos

today = datetime.date.today()
str_today = datetime.datetime.strftime(today, '%d/%m/%Y')
lastweek = today - datetime.timedelta(days=7)
str_lastweek = datetime.datetime.strftime(lastweek, '%d/%m/%Y')


@click.command()
def update():
    """Script de linha de comando para integração dos alertas."""
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = MongoClient(host=MONGODB_URI)
    db = conn[DATABASE]
    MONGODB_RISCO = os.environ.get('MONGODB_RISCO')
    conn_risco = MongoClient(host=MONGODB_RISCO)
    db_fichas = conn_risco['risco']
    print('Começando a integração de alertas...')
    processa_apontamentos(session, db, db_fichas)


if __name__ == '__main__':
    update()
