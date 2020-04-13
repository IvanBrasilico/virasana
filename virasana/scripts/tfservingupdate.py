"""Script de linha de comando para integração do Tensorflow Serving.

Script de linha de comando para fazer atualização 'manual'
dos metadados do módulo AJNA-PADMA nas imagens.

Importante: todos os modelos precisam atuar sobre um recorte da imagem
orginal, EXCETO os modelos treinados justamente para detectar este recorte.
Assim, serão filtrados apenas os registros que possuam a chave bbox para
recorte, a menos que o modelo selecionado seja um dos modelos próprios para
detecção do objeto contêiner (lista BBOX_MODELS do integracao.padma).

Args:

    modelo: modelo a consultar

    limit: tamanho do lote de atualização/limite de registros da consulta

    batch_size: quantidade de consultas simultâneas (mais rápido até limite do Servidor)

"""
import io
import time

import click
import numpy as np
import requests
from PIL import Image
from ajna_commons.flask.log import logger
from ajna_commons.utils.images import mongo_image, recorta_imagem

from virasana.db import mongodb as db
from virasana.integracao.padma import (BBOX_MODELS)


def monta_filtro(model: str, limit: int,
                 pulaerros=False) -> dict:
    """Retorna filtro para MongoDB."""
    filtro = {'metadata.contentType': 'image/jpeg'}
    # Modelo que cria uma caixa de coordenadas para recorte é pré requisito
    # para os outros modelos. Assim, outros modelos só podem rodar em registros
    # que já possuam o campo bbox (bbox: exists: True)
    if model not in BBOX_MODELS:
        filtro['metadata.predictions.bbox'] = {'$exists': True}
        filtro['metadata.predictions.' + model] = {'$exists': False}
    else:
        filtro['metadata.predictions.bbox'] = {'$exists': False}
    if pulaerros:
        filtro['metadata.predictions'] = {'$ne': []}
    logger.info('Estimando número de registros a processar...')
    count = db['fs.files'].count_documents(filtro, limit=limit)
    logger.info(
        '%d arquivos sem predições com os parâmetros passados...' % count
    )
    cursor = db['fs.files'].find(
        filtro, {'metadata.predictions': 1}).limit(limit)[:limit]
    logger.info('Consulta ao banco efetuada, iniciando conexões ao Padma')
    return cursor


def mostra_tempo_final(s_inicial, registros_vazios, registros_processados):
    """Exibe mensagem de tempo decorrido."""
    s1 = time.time()
    elapsed = s1 - s_inicial
    horas = elapsed // 3600
    minutos = (elapsed % 3600) // 60
    segundos = elapsed % 60
    print('%d:%d:%d' % (horas, minutos, segundos),
          'registros vazios', registros_vazios,
          'registros processados', registros_processados)


BATCH_SIZE = 64
MODEL = 'peso'
LIMIT = 128


@click.command()
@click.option('--modelo', help='Modelo de predição a ser consultado',
              required=True)
@click.option('--campo', help='Nome do campo a atualizar.'
                              + 'Se omitido, usa o nome do modelo.',
              default='')
@click.option('--limit',
              help='Tamanho do lote (padrão ' + str(LIMIT) + ')',
              default=LIMIT)
@click.option('--batch_size',
              help='Quantidade de consultas paralelas (padrão '
                   + str(BATCH_SIZE) + ')',
              default=BATCH_SIZE)
@click.option('--pulaerros', is_flag=True,
              help='Pular registros que tenham erro anterior' +
                   '(metadata.predictions == [])')
def predictions_update(modelo, campo, limit, batch_size, pulaerros):
    """Consulta padma e grava predições de retorno no MongoDB."""
    if not campo:
        campo = modelo
    cursor = monta_filtro(campo, limit, pulaerros)
    if not cursor:
        return False
    registros_processados = 0
    registros_vazios = 0
    s_inicio = time.time()
    images = []
    for registro in cursor:
        _id = registro['_id']
        pred_gravado = registro.get('metadata').get('predictions')
        registros_processados += 1
        if registros_processados == 1:
            logger.info('Iniciando varredura de registros')
        image_bytes = mongo_image(db, _id)
        image = Image.open(io.BytesIO(image_bytes))
        coords = pred_gravado[0].get('bbox')
        image = image.crop((coords[1], coords[0], coords[3], coords[2]))
        image = image.resize((288, 144), Image.LANCZOS)
        image_array = np.array(image) / 255
        images.append(image_array.tolist())
        if len(images) >= batch_size:
            json_batch = {"signature_name": "serving_default", "instances": images}
            r = requests.post('http://10.80.100.90/v1/models/vazio:predict', json=json_batch)
            print(r.json())
            images = []

    mostra_tempo_final(s_inicio, registros_vazios, registros_processados)


if __name__ == '__main__':
    s0 = time.time()
    predictions_update()
    s1 = time.time()
    print(
        'Tempo total de execução em segundos: {0:.2f}'.format(s1 - s0))
    # update()
