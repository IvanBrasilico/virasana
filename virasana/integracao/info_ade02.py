"""Funções para leitura e tratamento dos dados de pesagem e gate dos recintos.
"""
import csv
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta

import requests
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, mongo_sanitizar

FALTANTES = {'metadata.contentType': 'image/jpeg',
             'metadata.pesagens': {'$exists': False, '$eq': None}}

DTE_USERNAME = os.environ.get('DTE_USERNAME')
DTE_PASSWORD = os.environ.get('DTE_PASSWORD')

if DTE_PASSWORD is None:
    dte_file = os.path.join(os.path.dirname(__file__), 'dte.info')
    with open(dte_file) as dte_info:
        linha = dte_info.readline()
    DTE_USERNAME = linha.split(',')[0]
    DTE_PASSWORD = linha.split(',')[1]

try:
    recintos_file = os.path.join(os.path.dirname(__file__), 'recintos.csv')
    with open(recintos_file, encoding='utf-8') as csv_in:
        reader = csv.reader(csv_in)
        recintos_list = [row for row in reader]
except FileNotFoundError:
    recintos_list = []

# DTE_URL = 'https://www.janelaunicaportuaria.org.br/ws_homologacao/sepes/api/Pesagem'
DTE_TOKEN = 'https://jupapi.org.br/api/sepes/Pesagem/token'
DTE_URL = 'https://jupapi.org.br/api/sepes/PesagemMovimentacao'
FIELDS = ()

# Fields to be converted to ISODate
DATE_FIELDS = ('Date', 'UpdateDateTime', 'LastStateDateTime',
               'SCANTIME', 'ScanTime')

DATA = 'metadata.pesagens.saida'

CHAVES_RECINTO = [
    'metadata.pesagens.placacavalo',
    'metadata.pesagens.saida',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração."""
    db['PesagensDTE'].create_index('codigoconteinerentrada')
    db['PesagensDTE'].create_index('codigoconteinersaida')
    db['PesagensDTE'].create_index('datahoraentradaiso')
    db['PesagensDTE'].create_index('datahorasaidaiso')
    db['PesagensDTE'].create_index('pesoentradafloat')
    db['PesagensDTE'].create_index('pesosaidafloat')
    db['PesagensDTE'].create_index('codigorecinto')
    db['fs.files'].create_index('metadata.pesagens.tipo')
    db['fs.files'].create_index('metadata.pesagens.entrada')
    db['fs.files'].create_index('metadata.pesagens.saida')
    db['fs.files'].create_index('metadata.pesagens.peso')
    db['fs.files'].create_index('metadata.pesagens.placacavalo')
    return True


def get_token_dte(username=DTE_USERNAME, password=DTE_PASSWORD):
    data = {'username': username, 'password': password, 'grant_type': 'password'}
    r = requests.post(DTE_TOKEN, data=data, verify=False)
    print(r.url)
    print(r.text)
    print(r.status_code)
    token = r.json().get('access_token')
    return token


def get_pesagens_dte(datainicial, datafinal, recinto, token):
    payload = {'data_inicio': datetime.strftime(datainicial, '%Y-%m-%d'),
               'data_fim': datetime.strftime(datafinal, '%Y-%m-%d'),
               'cod_recinto': recinto}
    headers = {'Authorization': 'Bearer ' + token}
    r = requests.get(DTE_URL, headers=headers, params=payload, verify=False)
    logger.debug('get_pesagens_dte ' + r.url)
    try:
        lista_pesagens = r.json()['JUP_WS']['Pesagens']['Lista_Pesagens_Movimentacao']
    except:
        logger.error(r, r.text)
    return lista_pesagens


def get_pesagens_dte_recintos(recintos_list, datainicial, datafinal):
    token = get_token_dte()
    pesagens_recintos = defaultdict(list)
    for linha in recintos_list[1:]:
        recinto = linha[0]
        pesagens_recinto = get_pesagens_dte(datainicial, datafinal,
                                            recinto, token)
        if pesagens_recinto and len(pesagens_recinto) > 0:
            logger.info('%s: %s pesagens baixadas do recinto %s' %
                        (datainicial, len(pesagens_recinto), recinto))
            pesagens_recintos[recinto].extend(pesagens_recinto)
    return pesagens_recintos


def trata_registro_pesagem_dte(registro):
    def float_or_zero(s: str) -> float:
        try:
            return float(s)
        except ValueError:
            return 0.

    new_dict = {}
    for key, value in registro.items():
        key = sanitizar(key, mongo_sanitizar)
        if value is not None:
            value = sanitizar(value, mongo_sanitizar)
        new_dict[key] = value
    new_dict['codigoconteinerentrada'] = new_dict['codigoconteiner_entrada']
    new_dict['codigoconteinersaida'] = new_dict['codigoconteiner_saida']
    new_dict.pop('codigoconteiner_entrada')
    new_dict.pop('codigoconteiner_saida')
    datahoraentrada = new_dict['datahoraentrada']
    if datahoraentrada:
        new_dict['datahoraentradaiso'] = datetime.strptime(
            new_dict['datahoraentrada'],
            '%Y-%m-%d %H:%M:%S')
    datahorasaida = new_dict['datahorasaida']
    if datahorasaida:
        new_dict['datahorasaidaiso'] = datetime.strptime(
            new_dict['datahorasaida'],
            '%Y-%m-%d %H:%M:%S')
    new_dict['pesoentradafloat'] = float_or_zero(new_dict['pesocarregado_entrada'].replace(',', '.'))
    new_dict['pesosaidafloat'] = float_or_zero(new_dict['pesocarregado_saida'].replace(',', '.'))
    new_dict['taraentradafloat'] = float_or_zero(new_dict['tara_entrada'].replace(',', '.'))
    new_dict['tarasaidafloat'] = float_or_zero(new_dict['tara_saida'].replace(',', '.'))
    new_dict['veiculocarregadosaidabool'] = new_dict['veiculocarregado_saida'] == "sim"
    new_dict['veiculocarregadoentradabool'] = new_dict['veiculocarregado_entrada'] == "sim"
    return (new_dict)


def insert_pesagens_dte(db, pesagens_recintos):
    qtde = 0
    for recinto, pesagens in pesagens_recintos.items():
        for pesagem in pesagens:
            if pesagem['CodigoConteiner_Entrada'] or pesagem['CodigoConteiner_Saida']:
                pesagem_insert_mongo = {'codigo_recinto': recinto}
                pesagem_insert_mongo.update(trata_registro_pesagem_dte(pesagem))
                db['PesagensDTE'].insert_one(pesagem_insert_mongo)
                qtde += 1
    return qtde


def adquire_pesagens(db, datainicial, datafinal, refresh=False):
    try:
        ldata = datainicial
        # Trata somente um dia por vez, para não sobrecarregar DT-E
        while ldata <= datafinal:
            if refresh:
                tem_passagem_na_data = False
            else:
                tem_passagem_na_data = db['PesagensDTE'].find_one(
                    {'datahoraentradaiso': {'$gt': ldata,
                                            '$lt': ldata + timedelta(days=1)}})
            if tem_passagem_na_data:
                logger.info('adquire_pesagens dia %s abortado'
                            ' por já existirem registros' % ldata)
            else:
                pesagens = get_pesagens_dte_recintos(recintos_list, ldata, ldata)
                insert_ok = insert_pesagens_dte(db, pesagens)
            ldata = ldata + timedelta(days=1)
    except Exception as err:
        logger.error(err, exc_info=True)


def compara_pesagens_imagens(fs_cursor, pesagens_cursor, campo_comparacao):
    ind = 0
    linhas_ainserir = []
    if fs_cursor and len(fs_cursor) > 0:
        fs_row = fs_cursor[ind]
        for pesagem in pesagens_cursor:
            # print(1, pesagem[campo_comparacao], fs_row['metadata']['numeroinformado'].lower())
            while fs_row['metadata']['numeroinformado'].lower() < pesagem[campo_comparacao]:
                # print(2, pesagem[campo_comparacao], fs_row['metadata']['numeroinformado'].lower())
                ind += 1
                if ind >= len(fs_cursor):
                    break
                fs_row = fs_cursor[ind]
            if fs_row['metadata']['numeroinformado'].lower() == pesagem[campo_comparacao]:
                linhas_ainserir.append((fs_row['_id'], pesagem))
    # Conferência do algoritmo
    containers_imagens = [row['metadata']['numeroinformado'].lower() for row in fs_cursor]
    containers_pesagens = [row[campo_comparacao] for row in pesagens_cursor]
    containers_comuns = set(containers_imagens) & set(containers_pesagens)
    print('TOTAL DE PESAGENS COMUNS:', len(containers_comuns))
    return linhas_ainserir


def inserepesagens_fsfiles(db, pesagens: list, tipo: str):
    cont = 0
    for linha in pesagens:
        _id = linha[0]
        dte = linha[1]
        registro = db.fs.files.find_one(
            {'_id': _id},
            ['metadata.pesagens']
        )
        pesagens = registro['metadata'].get('pesagens', [])
        if pesagens is None or not isinstance(pesagens, list):
            pesagens = []
        pesagem = {}
        pesagem['recinto'] = dte['recinto']
        pesagem['tipo'] = tipo
        pesagem['entrada'] = dte.get('datahoraentradaiso', None)
        pesagem['saida'] = dte.get('datahorasaidaiso', None)
        pesagem['placacavalo'] = dte['placacavalo']
        placacarreta = dte.get('placacarreta')
        if not placacarreta:
            placacarreta = dte.get('placacarreta_entrada')
        if not placacarreta:
            placacarreta = dte.get('placacarreta_saida')
        pesagem['placacarreta'] = placacarreta
        pesobruto = dte.get('pesoentradafloat', 0.)
        if pesobruto == 0.:
            pesobruto = dte.get('pesosaidafloat', 0.)
        pesagem['pesobruto'] = pesobruto
        tara = dte.get('taraentradafloat', 0.)
        if tara == 0.:
            tara = dte.get('tarasaidafloat', 0.)
        pesagem['tara'] = tara
        pesagem['peso'] = pesobruto - tara
        pesagem['carregadoentrada'] = dte['veiculocarregadoentradabool']
        pesagem['carregadosaida'] = dte['veiculocarregadosaidabool']
        if pesagem not in pesagens:
            cont += 1
            pesagens.append(pesagem)
        db.fs.files.update_one(
            {'_id': _id},
            {'$set': {'metadata.pesagens': pesagens}}
        )
    return cont


def pesagens_grava_fsfiles(db, data_inicio, data_fim, delta=7):
    """Busca por registros no GridFS sem info da Pesagem

    Busca por registros no fs.files (GridFS - imagens) que não tenham metadata
    importada da pesagem.

    Args:
        db: connection to mongo with database setted
        batch_size: número de registros a consultar/atualizar por chamada
        data_inicio: filtra por data de escaneamento maior que a informada

    Returns:
        Número de registros encontrados

    """
    try:
        filtro = {'metadata.contentType': 'image/jpeg'}
        #  {'metadata.pesagens': {'$exists': False},
        delta_days = timedelta(days=delta)  # Pega dias antes/depois
        ldata = data_inicio
        acum = 0
        # Trata somente um dia por vez
        while ldata <= data_fim:
            ldata_inicio = ldata
            ldata_fim = datetime.combine(
                ldata, time.max)  # Pega atá a última hora do dia
            filtro['metadata.dataescaneamento'] = {'$gte': ldata_inicio,
                                                   '$lte': ldata_fim}
            projection = ['metadata.numeroinformado', 'metadata.dataescaneamento']
            total = db['fs.files'].count_documents(filtro)
            fs_cursor = list(
                db['fs.files'].find(filtro, projection=projection
                                    ).sort('metadata.numeroinformado')
            )
            print(filtro)
            pesagens_cursor_entrada = list(
                db['PesagensDTE'].find(
                    {'datahoraentradaiso':
                         {'$gte': ldata - delta_days,
                          '$lte': datetime.combine(ldata_fim + delta_days, time.max)
                          },
                     'codigoconteinerentrada':
                         {'$exists': True, '$ne': None, '$ne': ''}}
                ).sort('codigoconteinerentrada')
            )
            pesagens_cursor_saida = list(
                db['PesagensDTE'].find(
                    {'datahorasaidaiso':
                         {'$gte': ldata - delta_days,
                          '$lte': datetime.combine(ldata_fim + delta_days, time.max)
                          },
                     'codigoconteinersaida':
                         {'$exists': True, '$ne': None, '$ne': ''}}
                ).sort('codigoconteinersaida')
            )
            logger.info(
                'Processando pesagens para imagens de %s a %s. '
                'Pesquisando pesagens %s dias antes e depois. '
                'Imagens encontradas: %s  Pesagens encontradas %s(entrada) %s(saída).'
                % (ldata, ldata_fim, delta, len(fs_cursor),
                   len(pesagens_cursor_entrada), len(pesagens_cursor_saida))
            )
            linhas_entrada = compara_pesagens_imagens(fs_cursor, pesagens_cursor_entrada, 'codigoconteinerentrada')
            linhas_saida = compara_pesagens_imagens(fs_cursor, pesagens_cursor_saida, 'codigoconteinersaida')
            # acum = len(linhas_entrada) + len(linhas_saida)
            logger.info(
                'Resultado pesagens_grava_fsfiles '
                'Pesquisados %s. '
                'Encontrados %s entrada %s saida.'
                % (total, len(linhas_entrada), len(linhas_saida))
            )
            inseridos_entrada = inserepesagens_fsfiles(db, linhas_entrada, 'entrada')
            acum += inseridos_entrada
            inseridos_saida = inserepesagens_fsfiles(db, linhas_saida, 'saida')
            acum += inseridos_saida
            ldata = ldata + timedelta(days=1)
            logger.info('%s dados de entrada inseridos em fs.files',
                        inseridos_entrada)
            logger.info('%s dados de saida inseridos em fs.files',
                        inseridos_saida)
        return acum
    except Exception as err:
        logger.error(err, exc_info=True)
        return acum


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    # print('Criando índices para Pesagens')
    # print(create_indexes(db))
    print('Adquirindo pesagens do dia anterior')
    start = datetime.combine(date.today(), datetime.min.time()) - timedelta(days=1)
    end = start
    print(adquire_pesagens(db, start, end))
    print('Integrando pesagens do dia')
    print('Atualizados %s registros de pesagens' %
          pesagens_grava_fsfiles(db, start, end))
