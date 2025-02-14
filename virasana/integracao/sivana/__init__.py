import csv
import os
import sys
from datetime import datetime
from typing import Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.sivana.pontossivana import PontoSivana
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
    def __init__(self, psession):
        self.session = psession
        self.dataHora: datetime
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
        Acopla o requests e o xmletree na classe, pois esta tem conhecimento da autenticação
        Com o tempo, confirmar se é necessário este acoplamento

        Returns: xml ElementTree
        '''
        raise NotImplementedError('get deve ser implementado!')

    def format_datetime_for_url(self, dt: datetime) -> Tuple[str, str]:
        raise NotImplementedError('format_datetime_for_url deve ser implementado!')

    def parse_xml(self, alpr_record):
        """
        Retorna o próprio objeto "self" para permitir uso encadeado com to_sivana

        Args:
            alpr_record: XML de uma ALPR (estação de reconhecimento de placa) adquirida
        """
        raise NotImplementedError('parse_xml deve ser implementado!')
