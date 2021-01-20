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
import sys
import time

import click
import numpy as np
import requests
from PIL import Image

sys.path.insert(0, '.')
sys.path.insert(0, '../commons')

from ajna_commons.flask.log import logger
from ajna_commons.utils.images import mongo_image

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


def rescale(pred):
    peso = pred * 7558.96 + 18204.96
    return peso


def interpreta_pred(prediction: float, modelo: str):
    """Resume predições se necessário."""
    if modelo == 'vazio':
        return prediction < 0.5
    if modelo == 'peso':
        return rescale(prediction)


def prepara_imagem(image, modelo: str):
    if modelo == 'peso':
        image = image.resize((288, 144), Image.LANCZOS)
    elif modelo == 'vazio':
        image = image.resize((224, 224), Image.LANCZOS)
    # logger.info('Image size after resize: %s ' % (image.size, ))
    image_array = np.array(image) / 255
    return image_array


TFSERVING_URL = 'http://10.68.100.40/v1/models/'
BATCH_SIZE = 64
MODEL = 'peso'
LIMIT = 128


@click.command()
@click.option('--tfserving_url', help='URL do tfserving (Padrão %s)' % TFSERVING_URL,
              default=TFSERVING_URL)
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
def predictions_update(tfserving_url, modelo, campo, limit, batch_size, pulaerros):
    """Consulta padma e grava predições de retorno no MongoDB."""
    tfs_predictions_update(modelo, limit, batch_size, pulaerros, campo, tfserving_url)


def tfs_predictions_update(modelo, limit=2000, batch_size=20,
                           pulaerros=False, campo=None,
                           tfserving_url=TFSERVING_URL):
    if not campo:
        campo = modelo
    cursor = monta_filtro(campo, limit, pulaerros)
    if not cursor:
        return False
    registros_processados = 0
    registros_vazios = 0
    s_inicio = time.time()
    images = []
    _ids = []
    logger.info('Iniciando leitura das imagens')
    for registro in cursor:
        _id = registro['_id']
        pred_gravado = registro.get('metadata').get('predictions')
        registros_processados += 1
        image_bytes = mongo_image(db, _id)
        image = Image.open(io.BytesIO(image_bytes)).convert('RGB')
        coords = pred_gravado[0].get('bbox')
        logger.info('Image size: %s - bbox: %s' % (image.size, coords))
        image = image.crop((coords[1], coords[0], coords[3], coords[2]))
        logger.info('Image size after crop: %s ' % (image.size,))
        image_array = prepara_imagem(image, modelo)
        logger.info('Image array shape: %s ' % (image_array.shape,))
        images.append(image_array.tolist())
        _ids.append(_id)
        # print(len(images), end=' ')
        if len(images) >= batch_size:
            logger.info('Batch carregado, enviando ao Servidor TensorFlow.'
                        ' Modelo: %s' % modelo)
            s1 = time.time()
            json_batch = {"signature_name": "serving_default", "instances": images}
            r = requests.post(tfserving_url + '%s:predict' % modelo,
                              json=json_batch)
            logger.info('Predições recebidas do Servidor TensorFlow'
                        ' Modelo: %s' % modelo)
            s2 = time.time()
            logger.info('Consulta ao tensorflow serving em %s segundos' % (s2 - s1) +
                        ' para %s exemplos' % batch_size +
                        ' Modelo: %s' % modelo)
            try:
                preds = r.json()['predictions']
                # print(preds)
            except Exception as err:
                print(r.status_code)
                print(r.text)
                raise err
            # Salvar predições
            for oid, new_pred in zip(_ids, preds):
                pred_modelo = interpreta_pred(new_pred[0], modelo)
                logger.info({'_id': oid,
                             f'metadata.predictions.{modelo}': pred_modelo})
                # print('Gravando...', pred_gravado, oid)
                db['fs.files'].update(
                    {'_id': oid},
                    {'$set': {f'metadata.predictions[0].{modelo}': pred_modelo}}
                )
            logger.info('Predições novas salvas no MongoDB')
            images = []
            _ids = []
    mostra_tempo_final(s_inicio, registros_vazios, registros_processados)


if __name__ == '__main__':
    s0 = time.time()
    predictions_update()
    s1 = time.time()
    print(
        'Tempo total de execução em segundos: {0:.2f}'.format(s1 - s0))
    # update()
