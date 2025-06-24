"""
Monitora um diretório e envia arquivos BSON nele para o virasana.

USAGE
python dir_monitor.py

A cada intervalo de tempo configurado, lista os arquivos do diretório BSON_DIR
Se houverem arquivos, envia via POST para o endereco VIRASANA_URL

Pode ser importado e rodado em uma tarefa periódica (celery, cron, etc)

"""
import json
import os
import time
from sys import platform
from threading import Thread

import requests
from ajna_commons.flask.conf import VIRASANA_URL
from ajna_commons.flask.log import logger, root_path

# VIRASANA_URL = "http://localhost:5001"
LOGIN_URL = VIRASANA_URL + '/login'
if platform == 'win32':  # I am on ALFSTS???
    BSON_DIR = os.path.join('P:', 'SISTEMAS', 'roteiros', 'AJNA', 'BSON')
else:
    BSON_DIR = root_path[0:root_path.find('virasana')]
    BSON_DIR = os.path.join(BSON_DIR, 'virasana', 'BSON')
    try:
        if not os.path.exists(BSON_DIR):
            os.mkdir(BSON_DIR)
    except OSError:
        BSON_DIR = os.path.join('/home', 'ajna', 'virasana', 'BSON')

SYNC = True


def get_token(url):
    """Recupera o token CRSF necessário para fazer POST."""
    response = requests.get(url)
    csrf_token = response.text
    print(csrf_token)
    begin = csrf_token.find('csrf_token"') + 10
    end = csrf_token.find('username"') - 10
    csrf_token = csrf_token[begin: end]
    begin = csrf_token.find('value="') + 7
    end = csrf_token.find('/>')
    csrf_token = csrf_token[begin: end]
    print('****', csrf_token)
    return csrf_token


def login(username='ajna', senha='ajna'):
    """Efetua login no servidor."""
    token = get_token(LOGIN_URL)
    return requests.post(LOGIN_URL, data=dict(
        username=username,
        senha=senha,
        csrf_token=token
    ))


def despacha(filename, target, sync=SYNC):
    """Envia por HTTP POST o arquivo especificado.

    Args:
        file: caminho completo do arquivo a enviar

        target: URL do Servidor e API destino

    Returns:
        (Erro, Resposta)
        (True, None) se tudo correr bem
        (False, response) se ocorrer erro

    """
    bson = open(filename, 'rb')
    files = {'file': bson}
    # login()
    rv = requests.post(target, files=files, data={'sync': sync})
    if rv is None:
        return False, None
    try:
        response_json = rv.json()
        success = response_json.get('success', False) and \
                  (rv.status_code == requests.codes.ok)
    except json.decoder.JSONDecodeError as err:
        logger.error(err, exc_info=True)
        success = False
    return success, rv


def despacha_dir(dir=BSON_DIR, url=VIRASANA_URL, sync=SYNC):
    """Envia por HTTP POST todos os arquivos do diretório.

    Args:
        dir: caminho completo do diretório a pesquisar

        target: URL do Servidor e API destino

    Returns:
        diretório usado, lista de erros, lista de exceções

    """
    API_URL = url + '/api/uploadbson'
    TASK_URL = url + '/api/task/'
    erros = []
    sucessos = []
    exceptions = []
    # Limitar a cinco arquivos por rodada!!!
    cont = 0
    if not os.path.exists(dir):
        return dir, ['Diretório não encontrado'], []
    for filename in os.listdir(dir)[:90]:
        try:
            bsonfile = os.path.join(dir, filename)
            success, response = despacha(bsonfile, API_URL, sync)
            if success:
                # TODO: save on database list of files to delete
                #  (if light goes out or system fail, continue)
                response_json = response.json()
                if response_json.get('success', False) is True:
                    os.remove(bsonfile)
                    logger.info('Arquivo ' + bsonfile + ' removido.')
                    cont += 1
                    logger.info('********* %s arquivos processados' % cont)
            else:
                erros.append(response)
                logger.error(response.text)
        except Exception as err:
            exceptions.append(err)
            logger.error(err, exc_info=True)
    return dir, erros, exceptions




if __name__ == '__main__':
    print(despacha_dir())
