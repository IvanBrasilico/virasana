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
from bhadrasana.models.apirecintos import AcessoVeiculo
from sqlalchemy.orm import sessionmaker, scoped_session

from ajna_commons.flask.conf import SQL_URI
from ajna_commons.flask.log import logger
from virasana.integracao.sivana import OrganizacaoSivana
from sqlalchemy import create_engine


def le_ultimo_id_transmitido(psession):
    organizacao: OrganizacaoSivana = psession.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == 'APIRecintos').one_or_none()
    logger.info(f"Iniciando a transmissão a partir do ID: {organizacao.ultimo_id_transmitido}")
    return organizacao.ultimo_id_transmitido


def grava_ultimo_id_transmitido(psession, novo_id):
    # Abre o arquivo no modo de escrita e substitui o conteúdo
    organizacao: OrganizacaoSivana = psession.query(OrganizacaoSivana).filter(
        OrganizacaoSivana.codigoOrganizacao == 'APIRecintos').one_or_none()
    organizacao.ultimo_id_transmitido = novo_id
    psession.add(organizacao)
    psession.commit()
    logger.info(f"Atualizado 'ultimo_id_transmitido' com novo ID: {novo_id}")


def le_novos_acesssos_veiculo(psession, pultimo_id):
    acessos = psession.query(AcessoVeiculo).filter(AcessoVeiculo.id > pultimo_id).limit(500).all()
    passagens = [acesso.to_sivana() for acesso in acessos]
    payload = {'totalLinhas': len(passagens), 'offset': '-03:00', 'passagens': passagens}
    # print(payload)
    maior_id = max(acesso.id for acesso in acessos)
    return payload, maior_id


def upload_to_sivana(payload):
    SENHA_PCKS_SIVANA = os.environ['SENHA_PCKS_SIVANA']
    # Verifica se há acessos a transmitir
    if len(payload['passagens']) > 0:
        r = post(URL_API_SIVANA, pkcs12_filename=PKCS12_FILENAME,
                 pkcs12_password=SENHA_PCKS_SIVANA, json=payload, verify=False)

        if r.status_code != 200:
            logger.error(f'ERRO {r.status_code} no Upload para Sivana: {r.text}')
        return True
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
        if upload_to_sivana(payload):
            # Atualiza o arquivo com o maior ID encontrado
            grava_ultimo_id_transmitido(session, maior_id)
    finally:
        session.close()


if __name__ == '__main__':
    # URL_API_SIVANA = 'https://rf0020541092939.intrarfb.rfb.gov.br/prod/sivana/rest/upload'
    URL_API_SIVANA = 'https://sivana.rfb.gov.br/prod/sivana/rest/upload'
    PKCS12_FILENAME = './apirecintos1.p12'
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
