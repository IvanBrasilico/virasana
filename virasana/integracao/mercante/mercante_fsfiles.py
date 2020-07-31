"""Coleção de funções para integrar Mercante no GridFS (imagens)."""
from collections import defaultdict
from datetime import datetime, time, timedelta

import requests
import sqlalchemy
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from ajna_commons.flask.conf import VIRASANA_URL, SQL_URI
from ajna_commons.flask.log import logger
from bson import ObjectId
from pymongo import MongoClient
from sqlalchemy.orm import sessionmaker

from virasana.integracao.mercante.mercante_marshmallow import \
    conhecimento_carga, manifesto_carga

UPDATE_DATAOPERACAO_SQL = \
    'UPDATE manifestosresumo' \
    ' SET dataInicioOperacaoDate = STR_TO_DATE(dataInicioOperacao, "%Y-%m-%d")' \
    ' WHERE dataInicioOperacaoDate IS NULL AND dataInicioOperacao !=""; '


def get_conteineres_semcarga_dia(diaapesquisar: datetime) -> dict:
    """Retorna contêineres do dia com metadata.carga vazio.

    Pesquisa via API, não via banco diretamente

    Retorna dict com numero: _id

    """
    datainicial = datetime.strftime(datetime.combine(diaapesquisar, time.min), '%Y-%m-%d  %H:%M:%S')
    datafinal = datetime.strftime(datetime.combine(diaapesquisar, time.max), '%Y-%m-%d %H:%M:%S')
    logger.info('Pesquisando contêineres sem metadata do carga entre %s e %s' %
                (datainicial, datafinal))
    params = {'query':
                  {'metadata.dataescaneamento': {'$gte': datainicial, '$lte': datafinal},
                   'metadata.contentType': 'image/jpeg',
                   'metadata.carga': {'$exists': False}
                   },
              'projection':
                  {'metadata.numeroinformado': 1,
                   'metadata.dataescaneamento': 1}
              }

    r = requests.post(VIRASANA_URL + "/grid_data", json=params, verify=False)
    listacc = list(r.json())
    dict_numerocc = {item['metadata']['numeroinformado']: item['_id'] for item in listacc[:1000]}
    if dict_numerocc.get('ERRO'):
        dict_numerocc.pop('ERRO')
    if dict_numerocc.get(''):
        dict_numerocc.pop('')
    logger.info('%s imagens encontradas sem metadata do carga' % len(dict_numerocc))
    return dict_numerocc


def pesquisa_containers_no_mercante(engine, dia: datetime, listanumerocc: list):
    if len(listanumerocc) == 0:
        return {}, {}
    lista = '("' + '", "'.join(listanumerocc) + '")'
    print(lista)
    sql_manifestos = \
        'SELECT numero, idConteinerVazio FROM conteinervazioresumo c ' \
        ' inner join manifestosresumo m on c.manifesto = m.numero' \
        ' where tipoTrafego = %s AND ' \
        ' dataInicioOperacaoDate >= %s AND dataInicioOperacaoDate <= %s AND ' \
        ' c.idConteinerVazio IN ' + lista
    sql_conhecimentos = \
        'SELECT c.numeroCEmercante, codigoConteiner FROM itensresumo i' \
        ' inner join conhecimentosresumo c on i.numeroCEmercante = c.numeroCEmercante' \
        ' inner join manifestosresumo m on c.manifestoCE = m.numero' \
        ' WHERE c.tipoBLConhecimento in (\'10\', \'11\', \'12\') AND' \
        ' m.tipoTrafego = %s AND' \
        ' dataInicioOperacaoDate >= %s AND dataInicioOperacaoDate <= %s AND ' \
        ' i.codigoConteiner IN ' + lista
    before = dia - timedelta(days=6)
    before = datetime.strftime(before, '%Y-%m-%d')
    today = datetime.strftime(dia, '%Y-%m-%d')
    after = dia + timedelta(days=10)
    after = datetime.strftime(after, '%Y-%m-%d')
    # Pesquisar importação (5) e exportação (7)
    parametros_pesquisas = [(5, before, today), (7, today, after)]
    manifestos = defaultdict(set)
    conhecimentos = defaultdict(set)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.sql.text(UPDATE_DATAOPERACAO_SQL))
        for parametros_pesquisa in parametros_pesquisas:
            cursor = conn.execute(sql_manifestos, parametros_pesquisa)
            result = cursor.fetchall()
            logger.info('%s Manifestos encontrados para parâmetros %s' %
                        (len(result), parametros_pesquisa))
            for linha in result:
                manifestos[linha['idConteinerVazio']].add(linha['numero'])
            cursor = conn.execute(sql_conhecimentos, parametros_pesquisa)
            result = cursor.fetchall()
            logger.info('%s Conhecimentos encontrados para parâmetros %s' %
                        (len(result), parametros_pesquisa))
            for linha in result:
                conhecimentos[linha['codigoConteiner']].add(linha['numeroCEmercante'])
    return manifestos, conhecimentos


def update_mercante_fsfiles(db, engine, diaapesquisar: datetime):
    dict_numerocc = get_conteineres_semcarga_dia(diaapesquisar)
    # print(dict_numerocc)
    manifestos, conhecimentos = pesquisa_containers_no_mercante(
        engine,
        diaapesquisar,
        dict_numerocc.keys()
    )
    Session = sessionmaker(bind=engine)
    session = Session()
    for container, _id in dict_numerocc.items():
        if conhecimentos.get(container):  # Se encontrou conhecimento, priorizar!!!
            # logger.info('Update Conhecimento no _id %s Container %s' % (_id, container))
            db['fs.files'].update_one(
                {'_id': ObjectId(_id)},
                {'$set': {'metadata.carga':
                              conhecimento_carga(session, conhecimentos[container], container)}}
            )
        elif manifestos.get(container):
            # logger.info('Update manifesto no _id %s Container %s' % (_id, container))
            db['fs.files'].update_one(
                {'_id': ObjectId(_id)},
                {'$set': {'metadata.carga':
                              manifesto_carga(session, manifestos[container], container)}}
            )


def update_mercante_fsfiles_dias(db, engine, diainicio: datetime, diasantes=10):
    for dias in range(diasantes):
        diaapesquisar = diainicio - timedelta(days=dias)
        update_mercante_fsfiles(db, engine, diaapesquisar)

if __name__ == '__main__':
    hoje = datetime.today()
    db = MongoClient(host=MONGODB_URI)[DATABASE]
    engine = sqlalchemy.create_engine(SQL_URI)
    logger.info('Inciando atualização do metadata.carga via tabelas do Mercante...')
    # Pesquisa ontem e os dez dias anteriores
    update_mercante_fsfiles_dias(db, engine, hoje, 10)