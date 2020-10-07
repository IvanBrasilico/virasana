"""Funções para leitura e tratamento dos dados de pesagem e gate dos recintos.
"""
import csv
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta

import requests
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, mongo_sanitizar


DTE_USERNAME = os.environ.get('DTE_USERNAME')
DTE_PASSWORD = os.environ.get('DTE_PASSWORD')

if DTE_PASSWORD is None:
    dte_file = os.path.join(os.path.dirname(__file__), 'jupapi.info')
    with open(dte_file) as dte_info:
        linha = dte_info.readline()
    DTE_USERNAME = linha.split(',')[0]
    DTE_PASSWORD = linha.split(',')[1]

try:
    recintos_file = os.path.join(os.path.dirname(__file__), 'recintos.csv')
    with open(recintos_file, encoding='utf-8') as csv_in:
        reader = csv.reader(csv_in)
        recintos_list = [row for row in reader]
except FileNotFoundError:
    recintos_list = []

API_TOKEN = 'https://jupapi.org.br/api/jupgmcialf/autenticar'
GMCI_URL = 'https://jupapi.org.br/api/sepes/PesagemMovimentacao'
FIELDS = ()

# Fields to be converted to ISODate
DATE_FIELDS = ('Date', 'UpdateDateTime', 'LastStateDateTime',
               'SCANTIME', 'ScanTime')

def get_token_api(username=DTE_USERNAME, password=DTE_PASSWORD):
    data = {'username': username, 'password': password, 'grant_type': 'password'}
    r = requests.post(API_TOKEN, data=data, verify=False)
    print(r.url)
    print(r.text)
    print(r.status_code)
    token = r.json().get('access_token')
    return token




if __name__ == '__main__':  # pragma: no cover
    start = datetime.combine(date.today(), datetime.min.time()) - timedelta(days=1)
    end = start
    print(get_token_api())
