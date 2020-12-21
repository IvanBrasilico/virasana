"""Script de linha de comando para integração do Sistema PADMA.

Script de linha de comando para fazer atualização 'manual'
dos metadados do módulo AJNA-PADMA nas imagens.

Importante: todos os modelos precisam atuar sobre um recorte da imagem
orginal, EXCETO os modelos treinados justamente para detectar este recorte.
Assim, serão filtrados apenas os registros que possuam a chave bbox para
recorte, a menos que o modelo selecionado seja um dos modelos próprios para
detecção do objeto contêiner (lista BBOX_MODELS do integracao.padma).

Args:

    modelo: modelo a consultar

    t: tamanho do lote de atualização/limite de registros da consulta

    q: quantidade de consultas simultâneas (mais rápido até limite do Servidor)

    sovazios: selecionar contêineres declarados como vazio somente

    force: Tentar mesmo se consulta anterior a este registro falhou.
    Obs: em caso de consulta retornar vazia (não detectou resultado),
    o script irá gravar um valor em branco [] no campo destino e irá
    pular este registro nas próximas atualizações. Habilitar force
    processa estes registros novamente.

"""
import asyncio
import concurrent.futures
import datetime
import time
from collections import namedtuple

import click
from ajna_commons.flask.log import logger
from ajna_commons.utils.images import mongo_image, recorta_imagem
from bson import ObjectId

from virasana.db import mongodb as db
from virasana.integracao.padma import (BBOX_MODELS, consulta_padma,
                                       interpreta_pred)


# os.environ['DEBUG'] = '1'
# from ajna_commons.flask.log import logger


def monta_filtro(model: str, sovazios: bool,
                 update: str, tamanho: int,
                 pulaerros=False) -> dict:
    """Retorna filtro para MongoDB."""
    batch_size = tamanho
    filtro = {'metadata.contentType': 'image/jpeg'}
    if sovazios:
        filtro['metadata.carga.vazio'] = True
    # Modelo que cria uma caixa de coordenadas para recorte é pré requisito
    # para os outros modelos. Assim, outros modelos só podem rodar em registros
    # que já possuam o campo bbox (bbox: exists: True)
    if model not in BBOX_MODELS:
        filtro['metadata.predictions.bbox'] = {'$exists': True}
    if update is None:
        if model in BBOX_MODELS:
            filtro['metadata.predictions.bbox'] = {'$exists': False}
        else:
            filtro['metadata.predictions.' + model] = {'$exists': False}
    else:  # Parâmetro tamanho vira qtde de dias e filtra-se por datas
        try:
            dt_inicio = datetime.datetime.strptime(update, '%d/%m/%Y')
            dt_fim = dt_inicio + datetime.timedelta(days=tamanho)
        except ValueError:
            print('--update: Data em formato inválido!!!')
            return None
        print(dt_inicio, dt_fim)
        filtro['metadata.dataescaneamento'] = {'$gt': dt_inicio, '$lt': dt_fim}

    if pulaerros:
        filtro['metadata.predictions'] = {'$ne': []}
    logger.info('Estimando número de registros a processar...')
    count = db['fs.files'].count_documents(filtro, limit=batch_size)
    logger.info(
        '%d arquivos sem predições com os parâmetros passados...' % count
    )
    cursor = db['fs.files'].find(
        filtro, {'metadata.predictions': 1}).limit(batch_size)[:batch_size]
    logger.info('Consulta ao banco efetuada, iniciando conexões ao Padma')
    return cursor


def cropped_images(predictions: dict, image: bytes, _id: int) -> list:
    """Recorta imagens de acordo com bbox passada.

    Para acessar algumas predições, é necessário recortar as imagens antes.

    Args:
        predictions: dicionário com as predições
        image: imagem do contêiner original, em bytes
        _id: id do MongoDB se existir. Apenas para reportar erro

    Returns:
        lista de imagem(ns) recortada[s]

    """
    images = []
    for prediction in predictions:
        bbox = prediction.get('bbox')
        if bbox:
            try:
                image_crop = recorta_imagem(image, bbox)
                images.append(image_crop)
            except TypeError as err:
                print('Erro ao recortar imagem', _id, str(err))
    return images


ImageID = namedtuple('ImageID', ['id', 'content', 'predictions'])


def get_images(model: str, _id: ObjectId, image: bytes,
               predictions: dict) -> list:
    """Retorna ImagemID.

    Se model for BBOX, simplesmente anexa imagem original.
    Se model for outro tipo, recorta BBOX antes utilizando predictions BBOX
    já vinculadas à imagem _id e anexa uma ou duas imagens resultantes.
    """
    result = []
    if model in BBOX_MODELS:
        result.append(ImageID(_id, [image], None))
    else:
        # print('original', predictions, _id)
        if predictions:
            content = cropped_images(predictions, image, _id)
            result.append(ImageID(_id, content, predictions))

    return result


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


def consulta_padma_retorna_image(image: ImageID, model: str, campo: str):
    """Realiza request no padma. Retorna response e ImageID da consulta."""
    response = {'success': False}
    if model in BBOX_MODELS:
        response = consulta_padma(image.content[0], model)
    else:
        response = {'success': False}
        predictions = image.predictions
        # print(predictions, '************')
        for ind, content in enumerate(image.content):
            prediction = consulta_padma(content, model)
            print(prediction, '************')
            if prediction and prediction.get('success'):
                print(prediction, '************')
                # print(prediction['predictions'][0], '************')
                predictions[ind][campo] = interpreta_pred(
                    prediction['predictions'][0], model)
                # print('model:', model, 'campo:', campo)
        # print(predictions, '************')
        response['success'] = prediction['success']
        response['predictions'] = predictions
        response['erro'] = prediction.get('erro')
    return image, response


async def fazconsulta(images: list, model: str, campo: str):
    """Recebe lista de ImageID, monta uma ThreadPool.

    Monta ThreadPool do tamanho da lista recebida, chama consulta ao padma.
    Recebe retorno de cada Thread e grava no BD.

    Args:
        images: lista de NamedTuple tipo ImageID
        model: nome do modelo
        campo: nome do campo a gravar no BD
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = []
        # print(images)
        for image in images:
            loop_item = loop.run_in_executor(
                executor,
                consulta_padma_retorna_image,
                image,
                model,
                campo
            )
            futures.append(loop_item)
    seq = 0
    for image, response in await asyncio.gather(*futures):
        print('Consultou modelo:', model, 'image', image.id,
              'sequência', seq, 'resultado:', response)
        if response and response['success']:
            new_predictions = response['predictions']
            seq += 1
            # Por ora, uma consulta BBOX APAGA predições anteriores
            print('Gravando...', new_predictions, image.id, seq)
            db['fs.files'].update(
                {'_id': image.id},
                {'$set': {'metadata.predictions': new_predictions}}
            )
        else:
            print(
                'Consulta retornou erro! (modelo %s existe?)' % model)
            print('ERRO: %s' % response.get('erro'))


BATCH_SIZE = 10000
MODEL = 'ssd'
THREADS = 4


@click.command()
@click.option('--modelo', help='Modelo de predição a ser consultado',
              required=True)
@click.option('--campo', help='Nome do campo a atualizar.'
                              + 'Se omitido, usa o nome do modelo.',
              default='')
@click.option('--tamanho',
              help='Tamanho do lote (padrão ' + str(BATCH_SIZE) + ')',
              default=BATCH_SIZE)
@click.option('--qtde',
              help='Quantidade de consultas paralelas (padrão '
                   + str(THREADS) + ')',
              default=THREADS)
@click.option('--sovazios', is_flag=True,
              help='Processar somente vazios')
@click.option('--force', is_flag=True,
              help='Tentar mesmo se consulta anterior a este registro falhou.')
@click.option('--update', default=None,
              help='Reescrever dados existentes.'
                   + 'Passa por cima de dados existentes - especificar '
                   + 'data inicial (para não começar sempre do mesmo ponto)'
                   + ' no formato DD/MM/AAAA. Se update for selecionado, o'
                   + ' parâmetro --t passa a ser a quantidade de dias a serem '
                   + ' processados.')
@click.option('--pulaerros', is_flag=True,
              help='Pular registros que tenham erro anterior' +
                   '(metadata.predictions == [])')
def predictions_update(modelo, campo, tamanho, qtde,
                       sovazios, force, update, pulaerros):
    """Consulta padma e grava predições de retorno no MongoDB."""
    if not campo:
        campo = modelo
    cursor = monta_filtro(campo, sovazios, update, tamanho, pulaerros)
    if not cursor:
        return False
    registros_processados = 0
    registros_vazios = 0
    s_inicio = time.time()
    images = []
    logger.info('get_event_loop')
    loop = asyncio.get_event_loop()
    for registro in cursor:
        _id = registro['_id']
        pred_gravado = registro.get('metadata').get('predictions')
        if not force:
            if pred_gravado == []:
                registros_vazios += 1
                logger.info('Pulando registros com anterior insucesso ' +
                            ' (vazios:[]). Registro %d ' % registros_vazios)
                continue
        registros_processados += 1
        if registros_processados == 1:
            logger.info('Iniciando varredura de registros')
        image = mongo_image(db, _id)
        images.extend(get_images(model=modelo, _id=_id, image=image,
                                 predictions=pred_gravado))
        if registros_processados % qtde == 0:
            s0 = time.time()
            loop.run_until_complete(fazconsulta(images, modelo, campo))
            images = []
            s1 = time.time()
            logger.info(
                'Sequência real ...... %d ' % registros_processados +
                '{0: .2f} segundos'.format(s1 - s0)
            )
    # Processa pilha restante...
    logger.info('looping')
    loop.run_until_complete(fazconsulta(images, modelo, campo))
    mostra_tempo_final(s_inicio, registros_vazios, registros_processados)


def predictions_update2(modelo, campo, tamanho, qtde):
    """Consulta padma e grava predições de retorno no MongoDB."""
    if not campo:
        campo = modelo
    cursor = monta_filtro(campo, False, None, tamanho, pulaerros=True)
    if not cursor:
        return False
    registros_processados = 0
    registros_vazios = 0
    s_inicio = time.time()
    images = []
    logger.info('get_event_loop')
    loop = asyncio.get_event_loop()
    for registro in cursor:
        _id = registro['_id']
        pred_gravado = registro.get('metadata').get('predictions')
        if pred_gravado == []:
            registros_vazios += 1
            logger.info('Pulando registros com anterior insucesso ' +
                        ' (vazios:[]). Registro % d ' % registros_vazios)
            continue
        registros_processados += 1
        if registros_processados == 1:
            logger.info('Iniciando varredura de registros')
        image = mongo_image(db, _id)
        images.extend(get_images(model=modelo, _id=_id, image=image,
                                 predictions=pred_gravado))
        if registros_processados % qtde == 0:
            s0 = time.time()
            loop.run_until_complete(fazconsulta(images, modelo, campo))
            images = []
            s1 = time.time()
            logger.info(
                'Sequência real ...... %d ' % registros_processados +
                '{0: .2f} segundos'.format(s1 - s0) +
                ' modelo %s' % modelo
            )
    # Processa pilha restante...
    logger.info('final_loop')
    loop.run_until_complete(fazconsulta(images, modelo, campo))
    logger.info('end final_loop')
    mostra_tempo_final(s_inicio, registros_vazios, registros_processados)


if __name__ == '__main__':
    s0 = time.time()
    predictions_update()
    s1 = time.time()
    print(
        'Tempo total de execução em segundos: {0:.2f}'.format(s1 - s0))
    # update()
