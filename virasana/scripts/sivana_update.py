
import sys
import os
from dotenv import load_dotenv #TODO: COLOCAR NO AJNA COMMONS
load_dotenv()

sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.insert(0, '../commons')
sys.path.append('../bhadrasana2')
from bhadrasana.models.apirecintos import AcessoVeiculo
from sqlalchemy.orm import sessionmaker, scoped_session

from ajna_commons.flask.conf import (DATABASE,
                                     MONGODB_URI, SQL_URI,
                                     VIRASANA_URL)
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


# Extrair o ID que será o ponto de partida da extração


def update(connection):

    session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=connection))

    ultimo_id = le_ultimo_id_transmitido()
    acessos = session.query(AcessoVeiculo).filter(AcessoVeiculo.id > ultimo_id).all()
    passagens = [ acesso.to_sivana()  for acesso in acessos]
    payload = {'totalLinhas': len(passagens), 'offset': '-03:00', 'passagens': passagens}
    print(payload)
    #r = requests.post(SIVANA, json=payload)
    #if r.status_code == 200:
    #atualiza_ultima_transmissao(passagens)

    # Verifica se houve acessos e pega o maior ID
    if acessos and len(acessos)>0:
        maior_id = max(acesso.id for acesso in acessos)

        # Atualiza o arquivo com o maior ID encontrado
        grava_ultimo_id_transmitido(maior_id)


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    connection = create_engine(SQL_URI)

    update(connection)


    