"""
Script que pega os índices já gerados e coloca em array numpy
"""
import os
from datetime import timedelta, datetime

import numpy as np
from ajna_commons.flask.log import logger

from virasana.db import mongodb as db

VIRASANA_MODELS = os.path.join('virasana', 'models')


def gera_indexes(data_inicio=None):
    logger.info('Gerando índices de busca por similaridade...')
    # FIXME: Modificado para pegar apenas os últimos 6 meses de imagens,
    # pois estava estourando a memória
    if data_inicio is None:
        data_inicio = datetime.now() - timedelta(days=180)
    cursor = db['fs.files'].find(
        {'metadata.dataescaneamento': {'$gt': data_inicio},
         'metadata.predictions.index': {'$exists': True, '$ne': None}},
        {'metadata.predictions.index': 1}
    )

    lista_indexes = []
    lista_ids = []
    for index in cursor:
        lista_indexes.append(index.get('metadata'
                                       ).get('predictions')[0].get('index'))
        lista_ids.append(index.get('_id'))

    np_indexes = np.asarray(lista_indexes, dtype=np.float16)
    np_ids = np.asarray(lista_ids)

    np.save(os.path.join(VIRASANA_MODELS, 'indexes.npy'), np_indexes)
    np.save(os.path.join(VIRASANA_MODELS, '_ids.npy'),
            np.asarray(np_ids))
    logger.info('Indices de busca de similar gerados com sucesso!')
    logger.info('Salvo no arquivo: %s' % os.path.join(VIRASANA_MODELS, 'indexes.npy'))


if __name__ == '__main__':
    gera_indexes()
