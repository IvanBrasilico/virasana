import sys
from datetime import datetime, timedelta

import dateutil

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.sivana import TratamentoLPR
from virasana.integracao.sivana.pontossivana import PontoSivana
from virasana.scripts.sivana_update import upload_to_sivana
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
        dict_sivana = super().to_sivana()
        dict_sivana['info'] = f'Reliability:{self.Reliability} - ' + \
                              f'Hit: {self.Hit} - ' + \
                              f'Confidence: {self.Confidence} - ' + \
                              f'Latitude: {self.Latitude} - ' + \
                              f'Longitude: {self.Longitude}'
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
