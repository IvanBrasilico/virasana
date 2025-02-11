import xml.etree.ElementTree as ET


class APSPortodeSantos():

    def __init__(self):
        self.dataHora: str = ''
        self.ponto: str = ''
        self.placa: str = ''
        self.sentido: str = ''
        self.Latitude: str = ''
        self.Longitude: str = ''
        self.RecordNumber: str = ''
        self.Reliability: str = ''
        self.Hit: str = ''
        self.Confidence: str = ''
    def parse_xml(self, lpr_record):
        self.dataHora = lpr_record.find('DateTime').text
        self.ponto = lpr_record.find('Camera').text
        self.placa = lpr_record.find('LicensePlate').text
        self.RecordNumber = lpr_record.find('RecordNumber').text
        self.Latitude = lpr_record.find('Latitude').text
        self.Longitude = lpr_record.find('Longitude').text
        self.Reliability = lpr_record.find('Reliability').text
        self.Hit = lpr_record.find('Hit').text
        self.Confidence = lpr_record.find('Confidence').text

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


# Parse the XML content
root = ET.fromstring(xml_content)

# List to store the extracted records
records = []

# Iterate over each LPRRecord
for lpr_record in root.findall('.//LPRRecord'):