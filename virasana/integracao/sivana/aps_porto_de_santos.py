import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Tuple

import dateutil
import requests

from scripts.sivana_update import upload_to_sivana

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.sivana import TratamentoLPR
from virasana.integracao.sivana.pontossivana import PontoSivana
from ajna_commons.flask.log import logger


class APSPortodeSantos(TratamentoLPR):

    def __init__(self, psession):
        super().__init__(psession)
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
        return self.organizacao.url + f'?StartDate={start_date_str}&StartTime={start_time_str}&' + \
            f'EndDate={end_date_str}&EndTime={end_time_str}'

    def get(self, url):
        logger.info(f'Consultando url {url}')
        response = requests.get(url, auth=(self.organizacao.username, self.organizacao.password))
        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            # Parse do conteúdo XML
            return ET.fromstring(response.content)
        logger.error(f'Erro:{response.status_code}, {response.text}')
        raise ConnectionError(f'Erro:{response.status_code}, {response.text}')

    def parse_xml(self, alpr_record):
        self.dataHora = dateutil.parser.parse(alpr_record.find('DateTime').text)
        self.ponto = alpr_record.find('Camera').text
        self.placa = alpr_record.find('LicensePlate').text
        self.RecordNumber = alpr_record.find('RecordNumber').text
        self.Reliability = alpr_record.find('Reliability').text
        self.Hit = alpr_record.find('Hit').text
        self.Confidence = alpr_record.find('Confidence').text
        ponto_sivana = self.session.query(PontoSivana).filter(
            PontoSivana.codigoPonto == alpr_record.find('Camera').text
        ).one_or_none()
        if ponto_sivana is not None:
            self.sentido = ponto_sivana.sentido
            self.Latitude = ponto_sivana.latitude
            self.Longitude = ponto_sivana.longitude
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
            'dataHora': self.dataHora.strftime('%Y-%m-%dT%H:%M:%S'),
            'info': info
        }
        return dict_sivana



if __name__ == '__main__':
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    # Inicia o objeto de conexão à Fonte de dados LPR
    aps_manager = APSPortodeSantos(session)
    if aps_manager.organizacao is None:
        logger.error(f'Organização APSPortodeSantos não foi encontrada, impossível continuar.')
    else:
        # Definir as datas de início e fim
        end_date = datetime.now()
        start_date = aps_manager.organizacao.ultima_transmissao
        if start_date is None:
            logger.info(f'Organização APSPortodeSantos não tem data inicializada,'
                        f' pegando datahora atual menos 1 hora')
            start_date = end_date - timedelta(hours=1)
        payload, ultima_transmissao = aps_manager.processa_fonte_alpr(start_date, end_date)
        if upload_to_sivana(payload):
            aps_manager.set_ultima_transmissao(ultima_transmissao)
    '''
    # Códigos para visualização/debug do JSON gerado e dos dados    
    print(json.dumps(payload, indent=4))
    print(f'ultima_transmissao:{ultima_transmissao}')
    with open('APS.json', 'w') as f:
        json.dump(payload, f)
    pontos = set()
    for passagem in payload['passagens']:
        pontos.add(passagem['ponto'])
    print(pontos)
    '''
