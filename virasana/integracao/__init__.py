"""
Funções padrão para exploração do GridFS.

Como repositório de informações do Banco de Dados, facilitando a documentação
e o desenvolvimento.

Assim, neste arquivo e nos demais deste módulo pode-se conferir como está
sendo estruturado o Banco de Dados final, que nada mais é do que a integração
de diversas fontes de dados, cada uma com seu módulo neste pacote. Pode-se
também conferir os campos chave para os quais estão sendo criados índices,
conferir as "chaves primária e estrangeira", as datas, categorias, etc.

Além disso, podem ser criadas e mantidas aqui funções que dêem estatíticas
sobre a base para informar os usuários.

"""
import logging
import os
import pickle
from collections import defaultdict, OrderedDict
from datetime import datetime, date

import plotly
import plotly.graph_objs as go
from ajna_commons.flask.conf import DATABASE, MONGODB_URI
from ajna_commons.flask.log import logger
from ajna_commons.flask.user import DBUser
from integracao.risco.conformidade_alchemy import Conformidade
from pymongo import ASCENDING, MongoClient
from pymongo.errors import OperationFailure
from virasana.integracao import carga
from virasana.integracao import info_ade02
from virasana.integracao import padma
from virasana.integracao import xmli

USERNAME = 'virasana_service'
VIRASANA_PASS_FILE = os.path.join(os.path.dirname(__file__), USERNAME)
IMAGENS = {'metadata.contentType': 'image/jpeg'}
XML = {'metadata.contentType': 'text/xml'}
DATA = 'metadata.dataescaneamento'

STATS_LIVE = 30  # Tempo em minutos para manter cache de stats
stats = {}

CHAVES_GRIDFS = [
    'uploadDate',
    'md5',
    'filename',
    'metadata.id',
    'metadata.idcov',
    'metadata.UNIDADE',
    'metadata.recintoid',
    'metadata.recinto',
    'metadata.imagem',
    'metadata.numeroinformado',
    'metadata.dataescaneamento',
    'metadata.contentType'
]

TIPOS_GRIDFS = {
    'uploadDate': date
}


def create_indexes(db):
    """Cria índices necessários no GridFS."""
    db['fs.files'].create_index('uploadDate')
    db['fs.files'].create_index('md5')
    db['fs.files'].create_index('filename')
    db['fs.files'].create_index('metadata.id')
    db['fs.files'].create_index('metadata.idcov')
    db['fs.files'].create_index('metadata.recintoid')
    db['fs.files'].create_index('metadata.recinto')
    db['fs.files'].create_index('metadata.imagem')
    db['fs.files'].create_index('metadata.numeroinformado')
    db['fs.files'].create_index('metadata.dataescaneamento')
    db['fs.files'].create_index('metadata.contentType')
    db['fs.files'].create_index('metadata.carga.conhecimento.conhecimento')
    db['fs.files'].create_index('metadata.carga.manifesto.manifesto')
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING),
         ('metadata.carga.atracacao.escala', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING),
         ('metadata.carga.conhecimento.conhecimento', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING),
         ('metadata.carga.manifesto.manifesto', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING),
         ('metadata.carga.atracacao.escala', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.carga.atracacao.escala', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.carga.atracacao.escala', ASCENDING),
         ('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.xml.date', ASCENDING),
         ('metadata.contentType', ASCENDING)])
    db['fs.files'].create_index(
        [('metadata.contentType', ASCENDING),
         ('metadata.dataescaneamento', ASCENDING),
         (xmli.DATA, ASCENDING)])
    db['fs.files'].create_index([('metadata.contentType', ASCENDING),
                                 ('metadata.dataescaneamento', ASCENDING),
                                 (carga.DATA, ASCENDING)])
    db['fs.files'].create_index([('metadata.contentType', ASCENDING),
                                 ('metadata.dataescaneamento', ASCENDING),
                                 ('metadata.recinto', ASCENDING)])


def gridfs_count(db, filtro={}, limit=2000, campos=[]):
    """Aplica filtro, retorna contagem."""
    if filtro:
        if not campos:
            campos = [(key, 1) for key in filtro.keys()]
        logger.debug('integracao.gridfs_count filtro:%s hint:%s' %
                     (filtro, campos))
        try:
            params = dict(filter=filtro,
                          hint=campos)
            if limit:
                params['limit'] = limit
            print(params)
            return db['fs.files'].count_documents(**params)
        except OperationFailure as err:
            logger.error(err)
            params.pop('hint')
            return db['fs.files'].count_documents(**params)
    return db['fs.files'].count_documents({})


def tag(word: str, tags: list):
    """Coloca tags em torno de word."""
    open_tags = ['<' + tag + '>' for tag in tags]
    close_tags = ['</' + tag + '>' for tag in reversed(tags)]
    logger.debug('*************** %s ' %

                 word)
    return ''.join(open_tags) + word + ''.join(close_tags)


def dict_to_html(adict: dict):
    """Retorna HTML."""
    lista = []
    LABEL = ['span', 'b']
    TEXT = ['span', 'br']
    for key, value in adict.items():
        lista.append(tag(key, LABEL))
        if isinstance(value, str):
            val = value
        elif isinstance(value, list):
            val = '<br>'.join(value)
        else:
            val = 'Linha tipo ' + str(type(value)) + ' não suportada.'
        lista.append(tag(val, TEXT))
    return '\n'.join(lista)


def dict_to_text(adict: dict):
    r"""Retorna key\n [value\n]."""
    lista = []
    for key, value in adict.items():
        lista.append(key)
        if isinstance(value, str):
            lista.append(value)
        elif isinstance(value, list):
            lista.append('\n'.join(value))
        else:
            # print(key, value)
            lista.append('Linha tipo ' + str(type(value)) + ' não suportada.')
    return '\n'.join(lista)


def summary(grid_data=None, registro=None):
    """Selecionar campos mais importantes para exibição.

    Args:
        grid_data: Registro GridData do Gridfs.get
        registro: registro (dict) lido de cursor do MongoDB

    Returns:
        dict com descrição e valor de campos. (str: str)

    """
    result = OrderedDict()
    if grid_data:
        meta = grid_data.metadata
        upload = grid_data.uploadDate.strftime('%Y-%m-%d %H:%M')
    else:
        meta = registro.get('metadata')
        upload = meta.get('uploadDate')
    if not meta:
        raise TypeError('Não foi passado registro válido' +
                        'para a função integracao.__init__.summary')
    result['Número contêiner informado pelo recinto'] = meta.get(
        'numeroinformado')
    result['Data de escaneamento'] = meta.get(
        'dataescaneamento').strftime('%Y-%m-%d %H:%M')
    xmldoc = meta.get('xml')
    if xmldoc is None:
        result['XML não integrado.'] = ''
    else:
        if xmldoc.get('alerta') is True:
            result['CONTÊINER COM ALERTA DO OPERADOR DE ESCÂNER(alerta Recinto)'] = ''
    result['Data de Carregamento da imagem no sistema'] = upload
    result['Nome Recinto'] = meta.get('recinto')
    if meta.get('diferencapeso'):
        result['Diferença de peso imagem'] = \
            '{:0.2f}'.format(meta.get('diferencapeso'))
    if meta.get('diferencapeso2'):
        result['Diferença de peso balança'] = \
            '{:0.2f}'.format(meta.get('diferencapeso2'))
    return result


def get_data(db, data, filtro_data, campos, ordem=1):
    linha = db['fs.files'].find(
        filter=filtro_data,
        projection={data: 1},
        # hint=campos
    ).sort(data, ordem).limit(1)
    data_path = data
    try:
        linha = next(linha)
        for data_path in data.split('.'):
            if linha:
                if isinstance(linha, list):
                    linha = linha[0]
                linha = linha.get(data_path)
        if isinstance(linha, datetime):
            linha = linha.strftime('%d/%m/%Y %H:%M:%S %z')
        return linha, data_path
    except StopIteration:  # Não há registro nas datas filtradas
        return 'Inexistente para o período', data_path


def stats_resumo_imagens(db, datainicio=None, datafim=None):
    """Números gerais do Banco de Dados e suas integrações.

    Estatísticas gerais sobre as imagens
    """
    stats = OrderedDict()
    filtro = IMAGENS
    if datainicio and datafim:
        logger.debug('STATS IMAGENS Inicio %s Fim %s.' % (datainicio, datafim))
        filtro['metadata.dataescaneamento'] = {
            '$gt': datainicio, '$lt': datafim}
    logger.debug('Consultando Totais')
    now_atual = datetime.now()
    stats['Data do levantamento'] = now_atual
    stats['Periodo do levantamento'] = 'Inicio %s Fim %s.' % (datainicio, datafim)
    total = gridfs_count(db, filtro, limit=None)
    logger.debug('Total %s ' % filtro)
    stats['Total de imagens'] = total
    filtro_carga = dict(filtro, **carga.FALTANTES)
    stats['Imagens com info do Carga'] = total - \
                                         gridfs_count(db, filtro_carga,
                                                      limit=None)
    filtro_xml = dict(filtro, **xmli.FALTANTES)
    stats['Imagens com info do XML'] = total - \
                                       gridfs_count(db, filtro_xml,
                                                    limit=None)
    filtro_padma = dict(filtro, **padma.FALTANTES)
    count_padma = gridfs_count(db, filtro_padma, limit=None)
    stats['Imagens com bbox do Padma'] = total - count_padma

    filtro_pesagem = dict(filtro, **info_ade02.FALTANTES)
    stats['Imagens com informações de Pesagem'] = total - \
                                                  gridfs_count(db, filtro_pesagem,
                                                               limit=None)
    # DATAS
    logger.debug('Totais consultados')
    datas = OrderedDict()
    datas['imagem'] = DATA
    datas['XML'] = xmli.DATA
    datas['Carga'] = carga.DATA
    datas['Pesagem'] = info_ade02.DATA
    for base, data in datas.items():
        filtro_data = dict(filtro)
        if filtro_data.get(data):
            filtro_data[data].update({'$ne': None})
        else:
            filtro_data[data] = {'$ne': None}
        campos = [(key, 1) for key in filtro_data.keys()]
        logger.debug('Inicio consulta data projection:%s Filtro:%s Hint:%s'
                     % (data, filtro_data, campos))
        adata, data_path = get_data(db, data, filtro_data, campos)
        stats['Menor ' + data_path + ' ' + base] = adata
        adata, data_path = get_data(db, data, filtro_data, campos, -1)
        stats['Maior ' + data_path + ' ' + base] = adata
    # Qtde por Terminal
    logger.debug('Inicio consulta recintos 1. Filtro: %s ' % filtro)
    cursor = db['fs.files'].aggregate(
        [{'$match': filtro},
         {'$group':
              {'_id': '$metadata.recinto',
               'count': {'$sum': 1}}
          }])
    recintos = dict()
    for recinto in cursor:
        rid = recinto['_id']
        if rid is not None:
            recintos[rid] = recinto['count']
    ordered = OrderedDict()
    # print(recintos.keys())
    for key in sorted(recintos.keys()):
        ordered[key] = recintos[key]
    stats['recinto'] = ordered
    logger.debug('Inicio consulta recintos 2')
    cursor = db['stat_recinto'].find()
    recinto_mes = defaultdict(dict)
    for linha in cursor:
        recinto = linha['_id'].get('recinto')
        if recinto is not None:
            ano_mes = '%04d%02d' % (linha['_id']['year'],
                                    linha['_id']['month'])
            recinto_mes[recinto][ano_mes] = linha['count']
    for recinto, value in recinto_mes.items():
        ordered = OrderedDict(
            {key: value[key] for key in sorted(value)})
        recinto_mes[recinto] = ordered
    stats['recinto_mes'] = recinto_mes
    logger.debug('Fim consulta recintos 2')
    return stats


def atualiza_total_diario(db):
    """Cria coleção com estatística de total diário escaneamento."""
    logger.debug('Inicio atualizaçap total diário escaneamento')
    db['fs.files'].aggregate([
        {'$match': {'metadata.contentType': 'image/jpeg'}},
        {'$project':
             {'yearMonthDay': {'$dateToString':
                                   {'format': '%Y-%m-%d',
                                    'date': '$metadata.dataescaneamento'
                                    }
                               }
              }
         },
        {'$group':
             {'_id': '$yearMonthDay',
              'count': {'$sum': 1}
              }
         },
        {'$out': 'total_diario_escaneamento'}
    ])
    logger.debug('Fim atualização total diário escaneamento')


def atualiza_totais_recintos2(db):
    """Cria coleção com estatísticas de recinto por ano e mês."""
    logger.debug('Inicio atualização consulta recintos 2')
    db['fs.files'].aggregate(
        [{'$match': {'metadata.contentType': 'image/jpeg'}},
         {'$project':
              {'month': {'$month': '$metadata.dataescaneamento'},
               'year': {'$year': '$metadata.dataescaneamento'},
               'recinto': '$metadata.recinto'
               }
          },
         {'$group':
              {'_id':
                   {'recinto': '$recinto', 'month': '$month',
                    'year': '$year'},
               'count': {'$sum': 1}
               }
          },
         {'$out': 'stat_recinto'}
         ])
    logger.debug('Fim atualização consulta recintos 2')


def atualiza_stats(db, tipo='all'):
    """Recebe tipo, roda atualização de estat[istica correspondente."""
    logger.debug('Atualiza stats. Tipo: %s' % tipo)
    atualizacoes = {
        'recintos2': atualiza_totais_recintos2,
    }
    if tipo == 'all':
        for key, func in atualizacoes.items():
            func(db)
    else:
        func = tipo.get(tipo)
        if func is None:
            logger.debug('Atualiza stats. Tipo %s inexistente' % tipo)
        else:
            func(db)


def plot_pie_plotly(values, labels):
    """Gera gráfico de terminais."""
    # labels = ['1', '2', '3']
    # values =  [10, 20, 30]
    logger.debug(labels)
    logger.debug(values)
    plot = plotly.offline.plot({
        'data': [go.Pie(labels=labels, values=values)],
        'layout': go.Layout(title='Imagens por Recinto')
    },
        show_link=False,
        output_type='div')
    return plot


def plot_bar_plotly(values, labels):
    """Gera gráfico de barras."""
    # x = list(range(len(labels)))
    logger.debug(labels)
    logger.debug(values)
    plot = plotly.offline.plot({
        'data': [go.Bar(x=labels, y=values)],
        'layout': go.Layout(title='',
                            xaxis=go.layout.XAxis(type='category'))
    },
        show_link=False,
        output_type='div',
        image_width=400)
    return plot


def stats_por(db):
    """soon."""
    pass


def datas_bases():
    """Retorna nomes dos campos que possuem as datas de referência.

    Para cada integração, consulta se há data de referência e retorna

    """
    bases = {}
    bases['gridfs'] = DATA
    bases['xml'] = xmli.DATA
    bases['carga'] = carga.DATA
    return bases


def peso_container_documento(db, numeros: list):
    """Procedimento necessário para apurar o peso do contêiner.

    Procedimento necessário para apurar o peso do contêiner tendo em vista
    as informações importadas do CARGA.

    Args:
        db: conexão ao MongoDB GridFS
        numero: lista com os números de contêiner

    Returns:
        dict[numero] = peso

    """
    cursor = db['fs.files'].find(
        {'metadata.carga.atracacao.escala': {'$ne': None},
         'metadata.contentType': 'image/jpeg',
         'metadata.carga.container.container': {'$in': numeros}},
        {'_id': 0, 'metadata.carga.container.container': 1,
         'metadata.carga.container.pesobrutoitem': 1}
    )
    result = {}
    # Contêiner pode ter uso parcial, portanto é necessário somar
    for linha in cursor:
        peso = 0.
        for item in linha['metadata']['carga']['container']:
            peso += float(item['pesobrutoitem'].replace(',', '.'))
        result[linha['metadata']['carga']['container'][0]['container']] = peso
    return result


def volume_container(db, numeros: list):
    """Procedimento necessário para apurar o volume do contêiner.

    Procedimento necessário para apurar o volume do contêiner tendo em vista
    as informações importadas do CARGA.

    Args:
        db: conexão ao MongoDB GridFS
        numero: lista com os números de contêiner

    Returns:
        dict[numero] = volume

    """
    cursor = db['fs.files'].find(
        {'metadata.carga.atracacao.escala': {'$ne': None},
         'metadata.contentType': 'image/jpeg',
         'metadata.carga.container.container': {'$in': numeros}},
        {'_id': 0, 'metadata.carga.container.container': 1,
         'metadata.carga.container.volumeitem': 1}
    )
    # Contêiner pode ter uso parcial, portanto é necessário somar
    result = {}
    for linha in cursor:
        volume = 0.
        for item in linha['metadata']['carga']['container']:
            volume += float(item['volumeitem'].replace(',', '.'))
        result[linha['metadata']['carga']
        ['container'][0]['container']] = volume
    return result


def peso_container_balanca(db, numero: list):
    """Procedimento necessário para apurar o peso pesado do contêiner.

    Procedimento necessário para apurar o peso do contêiner tendo em vista
    as informações importadas dos sistemas de pesagem dos Recintos Aduaneiros.

    Args:
        db: conexão ao MongoDB GridFS
        numero: lista com os números de

    Returns:
        dict[numero] = peso

    """
    pass


def get_service_password():
    """Retorna virasana_service password.

    Se não existir, cria password randômico e cria/atualiza usuário no DB.
    """
    password = None
    try:
        with open(VIRASANA_PASS_FILE, 'rb') as secret:
            try:
                password = pickle.load(secret)
            except pickle.PickleError:
                password = None
    except FileNotFoundError:
        password = None
    if password is None:
        password = str(os.urandom(24))
        db = MongoClient(host=MONGODB_URI)[DATABASE]
        DBUser.dbsession = db
        DBUser.add(USERNAME, password)
        with open(VIRASANA_PASS_FILE, 'wb') as out:
            pickle.dump(password, out, pickle.HIGHEST_PROTOCOL)
    return USERNAME, password


if __name__ == '__main__':
    os.environ['DEBUG'] = '1'
    logger.setLevel(logging.DEBUG)
    db = MongoClient(host=MONGODB_URI)[DATABASE]
    logger.info('Criando índices para metadata')
    create_indexes(db)
    logger.info('Atualizando estatísticas')
    atualiza_stats(db)
    logger.info('Exibindo estatísticas')
    datainicio = datetime(2017, 7, 1)
    datafim = datetime.now()
    print(stats_resumo_imagens(db, datainicio, datafim))
