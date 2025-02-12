import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests


class TratamentoLPR:
    def __init__(self):
        self.dataHora: str = ''
        self.ponto: str = ''
        self.placa: str = ''
        self.sentido: str = ''

    def format_datetime_for_url(self, dt: datetime):
        return dt.strftime("%Y.%m.%d"), dt.strftime("%H.%M.%S.000")


class APSPortodeSantos(TratamentoLPR):

    def __init__(self):
        super().__init__()
        self.Latitude: str = ''
        self.Longitude: str = ''
        self.RecordNumber: str = ''
        self.Reliability: str = ''
        self.Hit: str = ''
        self.Confidence: str = ''

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

    def to_sivana(self):
        info = f'Reliability:{self.Reliability} - ' + \
               f'Hit: {self.Hit} - ' + \
               f'Confidence: {self.Confidence}'
        dict_sivana = {
            'placa': self.placa,
            'ponto': self.ponto,
            'sentido': self.sentido,
            'dataHora': self.dataHora.strftime('%Y-%m-%dT%H:%M:%S'),
            'info': info
        }
        return dict_sivana


if __name__ == '__main__':
    # Definir as datas de início e fim - testar com a última hora
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)

    # Inicia o objeto de tradução do XML
    aps_manager = APSPortodeSantos()

    # Formatar as datas para a URL
    start_date_str, start_time_str = aps_manager.format_datetime_for_url(start_date)
    end_date_str, end_time_str = aps_manager.format_datetime_for_url(end_date)

    # Construir a URL dinamicamente
    url = (
        f"http://vms3.portodesantos.com.br:8601/Interface/LPR/Search?"
        f"StartDate={start_date_str}&StartTime={start_time_str}&"
        f"EndDate={end_date_str}&EndTime={end_time_str}"
    )

    # Baixar o conteúdo XML
    response = requests.get(url)

    # Verificar se a requisição foi bem-sucedida
    if response.status_code == 200:
        # Parse do conteúdo XML
        root = ET.fromstring(response.content)
        # List to store the extracted records
        records = []

        # Iterate over each LPRRecord
        for lpr_record in root.findall('.//LPRRecord'):
            aps_manager.parse_xml(lpr_record)
            print(aps_manager.to_sivana())
