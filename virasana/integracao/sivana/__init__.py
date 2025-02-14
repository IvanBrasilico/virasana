import sys
from datetime import datetime, timedelta
from typing import Tuple

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.integracao.sivana.pontossivana import OrganizacaoSivana, PontoSivana
from ajna_commons.flask.log import logger


# Função para encontrar a organização pelo nome da classe
# Criar classes descendentes de tratamento LPR e cadastrar o nome da classe
# como campo codigoOrganizacao na classe OrganizacaoSivana
def get_credentials(session, lpr_name: str) -> Tuple[str, str, str, datetime]:
    organizacao: OrganizacaoSivana = session.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == lpr_name).one_or_none()
    if organizacao is None:
        logger.error(f'Nenhuma entrada encontrada para a ALPR: {lpr_name}.')
        return '', '', '', datetime.now() - timedelta(hours=1)
    return (organizacao.username, organizacao.password, organizacao.url,
            organizacao.ultima_transmissao)


def set_ultima_transmissao(session, lpr_name: str, ultima_transmissao: datetime):
    organizacao: OrganizacaoSivana = session.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == lpr_name).one()
    organizacao.ultima_transmissao = ultima_transmissao
    session.add(organizacao)
    session.commit()


class TratamentoLPR:
    def __init__(self, psession):
        self.session = psession
        self.dataHora: datetime
        self.ponto: str = ''
        self.placa: str = ''
        self.sentido: str = ''
        self.username, self.password, self.url, self.ultima_transmissao = \
            get_credentials(self.session, type(self).__name__)

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
        set_ultima_transmissao(self.session, type(self).__name__, ultima_transmissao)
