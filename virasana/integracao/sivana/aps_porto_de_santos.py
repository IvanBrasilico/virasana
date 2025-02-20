import sys

import dateutil

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.sivana import TratamentoLPR
from virasana.integracao.sivana.pontossivana import PontoSivana


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


class APSPortodeSantos2(APSPortodeSantos):
    """APS tem duas fontes de dados. Fora a URL, os outros dados são idênticos."""

    def __init__(self, psession):
        super().__init__(psession)
