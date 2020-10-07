"""Funções para leitura e tratamento dos dados de pesagem e gate dos recintos.
"""
import csv
import os
from datetime import date, datetime

import requests
from ajna_commons.flask.log import logger

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
    print(data)
    print(r.status_code)
    token = r.json().get('access_token')
    return token


def get_gmci(datainicial, datafinal, token):
    payload = {'DataInicial': datetime.strftime(datainicial, '%d/%m/%Y %H%:M:%S'),
               'DataFinal': datetime.strftime(datafinal, '%d/%m/%Y %H%:M:%S')}
    headers = {'Authorization': 'Bearer ' + token}
    print(params)
    r = requests.get(GMCI_URL, headers=headers, params=payload, verify=False)
    logger.debug('get_pesagens_dte ' + r.url)
    try:
        lista_pesagens = r.json()
        return lista_pesagens
    except:
        logger.error(r, r.text)


if __name__ == '__main__':  # pragma: no cover
    start = datetime.combine(date.today(), datetime.min.time())
    end = datetime.now()
    token = get_token_api()
    print(token)
    print(get_gmci(start, end, token))
