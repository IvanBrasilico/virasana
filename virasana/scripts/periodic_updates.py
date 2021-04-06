"""
Definição dos códigos que são rodados para integração.

Background tasks do sistema AJNA-virasana

Aqui ficam as rotinas que serão chamadas periodicamente, visando integrar as
diversas bases entre elas, criar campos calculados, fazer manutenção na base,
integrar as predições, etc.

Este arquivo pode ser chamado em um prompt de comando no Servidor ou
programado para rodar via crontab, conforme exempo em /periodic_updates.sh

"""
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from json.decoder import JSONDecodeError

import requests
from ajna_commons.flask.conf import (DATABASE,
                                     MONGODB_URI, SQL_URI,
                                     VIRASANA_URL)
from ajna_commons.flask.log import logger
from pymongo import MongoClient
from sqlalchemy import create_engine

from virasana.integracao import jupapi
from virasana.integracao import atualiza_stats, \
    carga, get_service_password, info_ade02, xmli
from virasana.integracao.mercante import mercante_fsfiles
from virasana.integracao.mercante import processa_xml_mercante
from virasana.integracao.mercante import resume_mercante
from virasana.models import anomalia_lote
from virasana.scripts.gera_indexes import gera_indexes
from virasana.scripts.predictionsupdate import predictions_update2
from virasana.scripts.tfservingupdate import tfs_predictions_update
from virasana.scripts import conformidadeupdate

def get_token(session, url):
    """Faz um get na url e tenta encontrar o csrf_token na resposta."""
    response = session.get(url)
    csrf_token = response.text
    begin = csrf_token.find('csrf_token"') + 10
    end = csrf_token.find('username"') - 10
    csrf_token = csrf_token[begin: end]
    begin = csrf_token.find('value="') + 7
    end = csrf_token.find('/>')
    csrf_token = csrf_token[begin: end]
    return csrf_token


def login(username, senha, session=None):
    """
    Autentica usuário no Servidor PADMA.

    Se não existir Usuário virasana, cria um com senha randômica
    """
    if session is None:
        session = requests.Session()
    url = VIRASANA_URL + '/login'
    csrf_token = get_token(session, url)
    # print('token*********', csrf_token)
    r = session.post(url, data=dict(
        username=username,
        senha=senha,
        csrf_token=csrf_token))
    return r


def reload_indexes():
    headers = {}
    result = {'success': False}
    s = requests.Session()
    username, password = get_service_password()
    r = login(username, password, s)
    try:
        print(VIRASANA_URL + '/recarrega_imageindex')
        r = s.get(VIRASANA_URL + '/recarrega_imageindex', headers=headers)
        if r.status_code == 200:
            result = r.json()
        print(result)
    except JSONDecodeError as err:
        print('Erro em reload_index(JSON inválido) %s HTTP Code:%s ' %
              (err, r.status_code))
    return result


def periodic_updates(db, connection, lote=2000):
    print('Iniciando atualizações...')
    hoje = datetime.combine(date.today(), datetime.min.time())
    doisdias = hoje - timedelta(days=2)
    cincodias = hoje - timedelta(days=5)
    dezdias = hoje - timedelta(days=10)
    vintedias = hoje - timedelta(days=20)
    ontem = hoje - timedelta(days=1)
    xmli.dados_xml_grava_fsfiles(db, lote * 5, cincodias)
    logger.info('Pegando arquivos XML')
    processa_xml_mercante.get_arquivos_novos(connection)
    processa_xml_mercante.xml_para_mercante(connection)
    resume_mercante.mercante_resumo(connection)
    mercante_fsfiles.update_mercante_fsfiles_dias(db, connection, hoje, 10)
    # carga.dados_carga_grava_fsfiles(db, lote * 2, doisdias)
    anomalia_lote.processa_zscores(db, cincodias, ontem)
    info_ade02.adquire_pesagens(db, cincodias, ontem)
    info_ade02.pesagens_grava_fsfiles(db, cincodias, ontem)
    atualiza_stats(db)
    carga.cria_campo_pesos_carga(db, lote * 3)
    carga.cria_campo_pesos_carga_pesagem(db, lote * 3)
    # predictions_update2('ssd', 'bbox', lote, 4)
    try:
        predictions_update2('index', 'index', lote, 8)
    except Exception as err:
        logger.error(err, exc_info=True)
    # gera_indexes()
    # print(reload_indexes())
    # tfs_predictions_update('vazio', lote, 20)
    try:
        tfs_predictions_update('peso', lote, 20)
    except Exception as err:
        logger.error(err, exc_info=True)
    # predictions_update2('vaziosvm', 'vazio', lote, 4)
    # predictions_update2('peso', 'peso', lote, 16)
    try:
        jupapi.novas_gmcis(connection)
    except Exception as err:
        logger.error(err, exc_info=True)
    try:
        conformidadeupdate.update_conformidade(db, connection)
        # Depois de dez dias, desiste de atualizar os campos extras puxados do bbox
        conformidadeupdate.preenche_bbox(db, connection, start=dezdias)
        # Depois de vinte dias, desiste de atualizar os campos extras puxados do Carga
        conformidadeupdate.completa_conformidade(db, connection, start=vintedias)
        conformidadeupdate.preenche_isocode(db, connection, start=vintedias)
    except Exception as err:
        logger.error(err, exc_info=True)


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    connection = create_engine(SQL_URI)
    with MongoClient(host=MONGODB_URI) as conn:
        db = conn[DATABASE]
        daemonize = '--daemon' in sys.argv
        periodic_updates(db, connection)
        s0 = time.time() - 600
        counter = 1
        while daemonize:
            logger.info('Dormindo 10 minutos... ')
            logger.info('Tempo decorrido %s segundos.' % (time.time() - s0))
            time.sleep(30)
            if time.time() - s0 > 300:
                logger.info('Periódico chamado rodada %s' % counter)
                counter += 1
                periodic_updates(db, connection)
                s0 = time.time()
