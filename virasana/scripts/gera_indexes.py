"""
Script que pega os índices já gerados e coloca em array numpy
"""
import os

import numpy as np
from ajna_commons.flask.log import logger

from virasana.db import mongodb as db

VIRASANA_MODELS = os.path.join('virasana', 'models')


def gera_indexes():
    logger.info('Gerando índices de busca por similaridade...')
    cursor = db['fs.files'].find(
        {'metadata.predictions.index': {'$exists': True, '$ne': None}},
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


if __name__ == '__main__':
    gera_indexes()
