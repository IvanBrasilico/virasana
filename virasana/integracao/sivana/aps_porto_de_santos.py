import csv
import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Tuple

import requests

sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.log import logger

CSV_FILE = 'lpr_credentials.csv'


# Função para encontrar uma entrada pela classe
def get_credentials(lpr_name: str) -> dict:
    if not os.path.isfile(CSV_FILE):
        logger.error(f'Arquivo CSV {CSV_FILE} não encontrado!!')
        return {}
    with open(CSV_FILE, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["class"] == lpr_name:
                return row  # Retorna a linha encontrada
    logger.error(f'Nenhuma entrada encontrada para a ALPR: {lpr_name}.')
    return {}


class TratamentoLPR:
    def __init__(self):
        self.dataHora: str = ''
        self.ponto: str = ''
        self.placa: str = ''
        self.sentido: str = ''
        credentials = get_credentials(type(self).__name__)
        self.username = credentials.get('username')
        self.password = credentials.get('password')

    def get_url(self, astart_date: datetime, aend_date: datetime) -> str:
        raise NotImplementedError('get_url deve ser implementado!')

    def get(self, url):
        '''
        Acopla o requests na classe, pois esta tem conhecimento da autenticação
        Com o tempo, confirmar se é necessário este acoplamente

        Returns: requests response object
        '''
        response = requests.get(aps_manager.get_url(start_date, end_date),
                                auth=(aps_manager.username, aps_manager.password))

    def format_datetime_for_url(self, dt: datetime) -> Tuple[str, str]:
        raise NotImplementedError('format_datetime_for_url deve ser implementado!')

    def parse_xml(self, alpr_record):
        """
        Retorna o próprio objeto "self" para permitir uso encadeado com to_sivana

        Args:
            alpr_record: XML de uma ALPR (estação de reconhecimento de placa) adquirida
        """
        raise NotImplementedError('parse_xml deve ser implementado!')


class APSPortodeSantos(TratamentoLPR):

    def __init__(self):
        super().__init__()
        self.Latitude: str = ''
        self.Longitude: str = ''
        self.RecordNumber: str = ''
        self.Reliability: str = ''
        self.Hit: str = ''
        self.Confidence: str = ''

    def format_datetime_for_url(self, dt: datetime) -> Tuple[str, str]:
        return dt.strftime("%Y.%m.%d"), dt.strftime("%H.%M.%S.000")

    def get_url(self, astart_date: datetime, aend_date: datetime) -> str:
        # Formatar as datas para a URL
        start_date_str, start_time_str = self.format_datetime_for_url(astart_date)
        end_date_str, end_time_str = self.format_datetime_for_url(aend_date)
        url = (
            f"http://vms3.portodesantos.com.br:8601/Interface/LPR/Search?"
            f"StartDate={start_date_str}&StartTime={start_time_str}&"
            f"EndDate={end_date_str}&EndTime={end_time_str}"
        )
        return url

    def get(self, url):
        return requests.get(url, auth=(self.username, self.password))

    def parse_xml(self, alpr_record):
        self.dataHora = alpr_record.find('DateTime').text
        self.ponto = alpr_record.find('Camera').text
        self.placa = alpr_record.find('LicensePlate').text
        self.RecordNumber = alpr_record.find('RecordNumber').text
        self.Latitude = alpr_record.find('Latitude').text
        self.Longitude = alpr_record.find('Longitude').text
        self.Reliability = alpr_record.find('Reliability').text
        self.Hit = alpr_record.find('Hit').text
        self.Confidence = alpr_record.find('Confidence').text
        return self

    def to_sivana(self):
        info = f'Reliability:{self.Reliability} - ' + \
               f'Hit: {self.Hit} - ' + \
               f'Confidence: {self.Confidence} - ' + \
               f'Latitude: {self.Latitude} - ' + \
               f'Longitude: {self.Longitude}'
        dict_sivana = {
            'placa': self.placa,
            'ponto': self.ponto,
            'sentido': self.sentido,
            'dataHora': self.dataHora,  # self.dataHora.strftime('%Y-%m-%dT%H:%M:%S'),
            'info': info
        }
        return dict_sivana


if __name__ == '__main__':
    # username = input('Usuário:')
    # password = input('Password:')
    # Definir as datas de início e fim - testar com a última hora
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)

    # Inicia o objeto de tradução do XML
    aps_manager = APSPortodeSantos()

    # Baixar o conteúdo XML
    response = aps_manager.get(aps_manager.get_url(start_date, end_date))

    # Verificar se a requisição foi bem-sucedida
    if response.status_code != 200:
        sys.exit(f'Erro:{response.status_code}, {response.text}')

    # Parse do conteúdo XML
    root = ET.fromstring(response.content)
    # Iterate over each LPRRecord and put result in a list
    records = [aps_manager.parse_xml(lpr_record).to_sivana()
               for lpr_record in root.findall('.//LPRRecord')]
    payload = {'totalLinhas': len(records), 'offset': '-03:00', 'passagens': records}
    formated = json.dumps(payload, indent=4)
    print(formated)
    with open('APS.json', 'w') as f:
        json.dump(formated, f)

# TODO: incluir código para transmitir para Sivana, após validar mecanismo de salvar
# avanço (última transmissão) e validar os dados
''' 
if passagens and len(passagens) > 0:
    r = post(URL_API_SIVANA, pkcs12_filename=PKCS12_FILENAME,
             pkcs12_password=SENHA_PCKS_SIVANA, json=payload, verify=False)
    if r.status_code != 200:
        logger.error(f'ERRO {r.status_code} no Upload para Sivana: {r.text}')
    else:
        # Verifica se houve acessos e pega o maior ID
        maior_id = max(acesso.id for acesso in acessos)
        # Atualiza o arquivo com o maior ID encontrado
        grava_ultimo_id_transmitido(maior_id)
else:
    logger.info(f'Não há novos registros de acesso a transmitir')
'''
