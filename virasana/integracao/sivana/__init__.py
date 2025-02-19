import sys
from datetime import datetime
from typing import Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.sivana.pontossivana import OrganizacaoSivana, PontoSivana
from ajna_commons.flask.log import logger


# Função para encontrar a organização pelo nome da classe
# Criar classes descendentes de tratamento LPR e cadastrar o nome da classe
# como campo codigoOrganizacao na classe OrganizacaoSivana
def get_organizacao(session, lpr_name: str) -> OrganizacaoSivana:
    organizacao: OrganizacaoSivana = session.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == lpr_name).one_or_none()
    if organizacao is None:
        logger.error(f'Nenhuma entrada encontrada para a ALPR: {lpr_name}.')
    return organizacao


class TratamentoLPR:
    def __init__(self, psession):
        self.session = psession
        self.dataHora: datetime = datetime.now()
        self.ponto: str = ''
        self.placa: str = ''
        self.sentido: str = ''
        self.organizacao = \
            get_organizacao(self.session, type(self).__name__)

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

    def set_ultima_transmissao(self, ultima_transmissao: datetime):
        self.organizacao.ultima_transmissao = ultima_transmissao
        self.session.add(self.organizacao)
        self.session.commit()
        logger.info(f'Atualizada "ultima_transmissao" da organização {type(self).__name__}'
                    f' com novo valor: {ultima_transmissao}')

    def processa_fonte_alpr(self, pstart: datetime, pend: datetime):
        # Baixar o conteúdo XML
        payload = {'totalLinhas': 0, 'offset': self.organizacao.offset, 'passagens': []}
        ultima_transmissao = self.organizacao.ultima_transmissao
        try:
            root = self.get(self.get_url(pstart, pend))
            # Iterate over each LPRRecord and put result in a list
            logger.info('Processando XML..')
            records = []
            for lpr_record in root.findall('.//LPRRecord'):
                self.parse_xml(lpr_record)
                records.append(self.to_sivana())
                ultima_transmissao = max(self.dataHora, ultima_transmissao)
            payload = {'totalLinhas': len(records), 'offset': self.organizacao.offset,
                       'passagens': records}
            logger.info(f'Recuperadas {len(records)} passagens da organização {type(self).__name__}')
        except Exception as err:
            logger.error(err)
        return payload, ultima_transmissao

    def to_sivana(self):
        """
        Retorna o formato de registro do Sivana

        """
        return {
            'placa': self.placa,
            'ponto': self.ponto,
            'sentido': self.sentido,
            'dataHora': self.dataHora.strftime('%Y-%m-%dT%H:%M:%S'),
            'info': ''
        }
