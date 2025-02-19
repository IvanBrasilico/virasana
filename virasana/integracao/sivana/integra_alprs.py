"""
Usa as classes do Tipo TratamentoLPR, que por sua vez usam as informações da
classe OrganizacaoSivana, para baixar os últimos dados das LPRs implementadas,
colocar no padrão de payload do Sivana, conectar ao Sivana e transmitir.

Em caso de sucesso, atualiza o controle local de última datahora ou ID transmitido,
pois o Sivana não implementa esta consulta.

"""
import logging
import os
import sys
import time
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from ajna_commons.flask.conf import SQL_URI
from virasana.scripts.sivana_update import upload_to_sivana
from virasana.integracao.sivana.aps_porto_de_santos import APSPortodeSantos
from virasana.integracao.sivana.api_recintos import APIRecintos
from ajna_commons.flask.log import logger


def update(connection):
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=connection))
    try:
        manager_list = [APSPortodeSantos, APIRecintos]
        for classe in manager_list:
            # Inicia o objeto de conexão à Fonte de dados LPR
            lpr_manager = classe(session)
            if lpr_manager.organizacao is None:
                logger.error(f'Organização {classe.__name__} não foi encontrada, impossível continuar.')
            else:
                # Definir as datas de início e fim
                end_date = datetime.now()
                # Adiciona um microsegundo à última datahora, para não pegar dado repetido.
                start_date = lpr_manager.organizacao.ultima_transmissao + timedelta(milliseconds=1)
                if start_date is None:
                    logger.info(f'Organização {classe.__name__} não tem data inicializada,'
                                f' pegando datahora atual menos 1 hora')
                    start_date = end_date - timedelta(hours=1)
                payload, ultima_transmissao = lpr_manager.processa_fonte_alpr(start_date, end_date)
                logger.debug('%s', payload)
                url_api_sivana = lpr_manager.organizacao.url_api_sivana
                pkcs12_filename = lpr_manager.organizacao.pkcs12_filename
                senha_pcks_sivana = lpr_manager.organizacao.senha_pcks_sivana
                if upload_to_sivana(url_api_sivana, pkcs12_filename, senha_pcks_sivana, payload):
                    lpr_manager.set_ultima_transmissao(ultima_transmissao)
                logger.info(f'Organização {classe.__name__} atualizada com sucesso,'
                            f' dados de {start_date} a {end_date}')
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
