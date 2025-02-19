import logging
import os
import sys
import time
from typing import Tuple

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


def le_novos_acesssos_veiculo(psession, pultimo_id, limit=500) -> Tuple[dict, int]:
    ''' Pesquisa novos registros na tabela.
    Se retornar payload com passagens vazio, ocorreu erro ou não há registros novos.

    Args:
        psession: conexão com o Banco SQL
        pultimo_id: ID do AcessoVeículo a partir do qual transmitir
        limit: quantidade máxima a buscar por vez

    Returns: dict payload no formato Sivana, maior ID recuperado

    '''
    maior_id = pultimo_id
    payload = {'totalLinhas': 0, 'offset': '-03:00', 'passagens': []}
    try:
        acessos = psession.query(AcessoVeiculo).filter(AcessoVeiculo.id > pultimo_id).limit(500).all()
        passagens = [acesso.to_sivana() for acesso in acessos]
        payload = {'totalLinhas': len(passagens), 'offset': '-03:00', 'passagens': passagens}
        # print(payload)
        maior_id = max(acesso.id for acesso in acessos)
    except Exception as err:
        logger.error(f'Erro ao recuperar registros de AcessoVeiculo. pultimo_id:{pultimo_id}')
        logger.error(err)
    return payload, maior_id


def upload_to_sivana(payload):
    # TODO: buscar informações abaixo de acordo com a configuração da organização
    if os.environ.get('SIVANA_PROD'):  # Produção
        url_api_sivana = 'https://sivana.rfb.gov.br/prod/sivana/rest/upload'
        pkcs12_filename = './aps_hom.p12'
    else:  # Homologação
        logger.info('Conectando o ambiente de HOMOLOGAÇÃO!!!')
        url_api_sivana = 'https://rf0020541092939.intrarfb.rfb.gov.br/prod/sivana/rest/upload'
        pkcs12_filename = './apirecintos1.p12'
    senha_pcks_sivana = os.environ.get('SENHA_PCKS_SIVANA')
    if senha_pcks_sivana is None:
        logger.info('Atenção!!! Senha do certificado SENHA_PCKS_SIVANA não definida no ambiente.')
    # Verifica se há acessos a transmitir
    if len(payload['passagens']) > 0:
        # Envia para o Sivana
        r = post(url_api_sivana, pkcs12_filename=pkcs12_filename,
                 pkcs12_password=senha_pcks_sivana, json=payload, verify=False)
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
