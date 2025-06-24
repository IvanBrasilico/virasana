import logging
import os
import sys
import time

from dotenv import load_dotenv  # TODO: COLOCAR NO AJNA COMMONS
from requests_pkcs12 import post

load_dotenv()

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.conf import SQL_URI
from ajna_commons.flask.log import logger
from sqlalchemy import create_engine
from virasana.integracao.sivana import integra_alprs


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
        except ValueError as err:
            logger.error(err)
            # TODO: Gambiarra para atualizar ultima_atualizacao mesmo se retornar o erro
            # abaixo. Aparentemente é um bug na biblioteca urllib3. Retirar essa
            # gambiarra assim que for possível atualizar a biblioteca no Servidor
            # Neste momento não dá para atualizar, pois Servidor roda python 3.6
            # if 'check_hostname' in str(err):
            #    return True
        except Exception as err:
            logger.error(type(err), err)
    else:
        logger.info(f'Não há novos registros de acesso a transmitir')
    return False


def call_update(connection, mensagem, update_function):
    s0 = time.time()
    try:
        logger.info(mensagem)
        update_function(connection)
        logger.info('%s demorou %s segundos.' % (mensagem, (time.time() - s0)))
    except Exception as err:
        logger.error(err, exc_info=True)


def update(connection):
    call_update(connection,
                'Rodando integração das fontes de ALPR e dos AcessoVeiculo para Sivana...',
                integra_alprs.update)


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
