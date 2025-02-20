import logging
import os
import sys
import time

from dotenv import load_dotenv  # TODO: COLOCAR NO AJNA COMMONS
from requests_pkcs12 import post
from sqlalchemy.orm import sessionmaker, scoped_session

load_dotenv()

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../commons')
sys.path.append('../bhadrasana2')

from virasana.integracao.sivana.api_recintos import le_novos_acesssos_veiculo, \
    le_ultimo_id_transmitido, grava_ultimo_id_transmitido
from ajna_commons.flask.conf import SQL_URI
from ajna_commons.flask.log import logger
from sqlalchemy import create_engine


def upload_to_sivana(url_api_sivana, pkcs12_filename, senha_pcks_sivana, payload):
    # Verifica se há acessos a transmitir
    if len(payload['passagens']) > 0:
        # Envia para o Sivana
        try:
            r = post(url_api_sivana, pkcs12_filename=pkcs12_filename,
                     pkcs12_password=senha_pcks_sivana, json=payload, verify=False)
            if r.status_code == 200:
                logger.info(f'SUCESSO! {len(payload["passagens"])} acessos enviados para a url {url_api_sivana}')
                return True
            logger.error(f'ERRO {r.status_code} no Upload para Sivana: {r.text}')
        except Exception as err:
            logger.error(err, exc_info=True)
    else:
        logger.info(f'Não há novos registros de acesso a transmitir')
    return False


def update(connection):
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=connection))
    try:
        ultimo_id = le_ultimo_id_transmitido(session)
        payload, maior_id = le_novos_acesssos_veiculo(session, ultimo_id)
        url_api_sivana = 'https://sivana.rfb.gov.br/prod/sivana/rest/upload'
        pkcs12_filename = './apirecintos1.p12'
        senha_pcks_sivana = os.environ.get('SENHA_PCKS_SIVANA')
        if senha_pcks_sivana is None:
            logger.info('Atenção!!! Senha do certificado SENHA_PCKS_SIVANA não definida no ambiente.')
        if upload_to_sivana(url_api_sivana, pkcs12_filename, senha_pcks_sivana, payload):
            # Atualiza o arquivo com o maior ID encontrado
            grava_ultimo_id_transmitido(session, maior_id)
    finally:
        session.close()


if __name__ == '__main__':
    # Ambiente de homologação
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    connection = create_engine(SQL_URI)
    s0 = time.time()
    counter = 1
    update(connection)
    while True:
        logger.info('Dormindo 1 minuto... ')
        logger.info('Tempo decorrido %s segundos.' % (time.time() - s0))
        time.sleep(10)
        if time.time() - s0 > 60:
            logger.info('Chamada periódica rodada %s' % counter)
            counter += 1
            update(connection)
            s0 = time.time()
