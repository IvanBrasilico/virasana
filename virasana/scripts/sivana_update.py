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
from sqlalchemy import create_engine
import logging

# Nome do arquivo
NOME_ARQUIVO = 'ultimo_id_transmitido.txt'

# Caminho absoluto para a raiz do projeto
CAMINHO_ARQUIVO = os.path.join(os.getcwd(), NOME_ARQUIVO)


def verificar_ou_criar_arquivo():
    # Verifica se o arquivo já existe
    if not os.path.isfile(CAMINHO_ARQUIVO):
        # Se não existir, cria o arquivo com o conteúdo 3778860
        with open(CAMINHO_ARQUIVO, 'w') as arquivo:
            arquivo.write('3778860')
        logger.info(f"Arquivo '{NOME_ARQUIVO}' criado com o conteúdo: 3778860")
    else:
        logger.info(f"O arquivo '{NOME_ARQUIVO}' já existe.")


def le_ultimo_id_transmitido():
    verificar_ou_criar_arquivo()
    with open(CAMINHO_ARQUIVO, 'r') as arquivo:
        conteudo = arquivo.read().strip()
    logger.info(f"Iniciando a transmissão a partir do ID: {conteudo}")
    return int(conteudo)


def grava_ultimo_id_transmitido(novo_id):
    # Abre o arquivo no modo de escrita e substitui o conteúdo
    with open(CAMINHO_ARQUIVO, 'w') as arquivo:
        arquivo.write(str(novo_id))
    logger.info(f"Atualizado 'ultimo_id_transmitido.txt' com novo ID: {novo_id}")


def update(connection):
    SENHA_PCKS_SIVANA = os.environ['SENHA_PCKS_SIVANA']
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=connection))
    try:
        ultimo_id = le_ultimo_id_transmitido()
        acessos = session.query(AcessoVeiculo).filter(AcessoVeiculo.id > ultimo_id).limit(500).all()
        passagens = [acesso.to_sivana() for acesso in acessos]
        payload = {'totalLinhas': len(passagens), 'offset': '-03:00', 'passagens': passagens}
        # print(payload)
        # Verifica se há acessos a transmitir
        if passagens and len(passagens) > 0:
            r = post(URL_API_SIVANA, pkcs12_filename=PKCS12_FILENAME,
                     pkcs12_password=SENHA_PCKS_SIVANA, json=payload, verify=False)
            if r.status_code != 200:
                logger.error(f'ERRO {r.status_code} no Upload para Sivana: {r.text}')
            else:
            # Verifica se houve acessos e pega o maior ID
                maior_id = max(acesso.id for acesso in acessos)
                # Atualiza o arquivo com o maior ID encontrado
                grava_ultimo_id_transmitido(maior_id)
        else:
            logger.info(f'Não há novos registros de acesso a transmitir')
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

