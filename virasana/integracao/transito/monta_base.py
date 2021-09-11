import os
import sys
from datetime import datetime
from typing import Tuple

import pymongo
from PIL import Image
from bson import ObjectId
from gridfs import GridFS

sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')
from ajna_commons.utils.images import recorta_imagem
from ajna_commons.flask.log import logger

recintos_destino = ['CRAGEA']


def get_img_recortada(db, _id) -> Image:
    fs = GridFS(db)
    grid_data = fs.get(_id)
    preds = grid_data.metadata.get('predictions')
    if preds:
        bboxes = preds[0].get('bbox')
        # print(bboxes)
        image = grid_data.read()
        image = recorta_imagem(image, bboxes, pil=True)
        return image


def get_pares_periodo(db, inicio: datetime, fim: datetime, save=False,
                      limit=1000, outpath='pares_transito'):
    result = []
    filtro = {'metadata.dataescaneamento':
                  {'$gt': inicio, '$lt': fim},
              'metadata.recinto': {'$in': recintos_destino},
              'metadata.contentType': 'image/jpeg'}
    cursor = db.fs.files.find(filtro,
                              {'_id': 1}).limit(limit)
    for row in cursor:
        try:
            if save:
                # salva pares no disco
                grava_imagens(db, row['_id'], outpath)
            else:
                # retorna pares origem e destino
                result.append(get_pares(db, row['_id']))
        except ValueError as err:
            logger.error(err)
    return result


def get_pares(db, _id: ObjectId) -> Tuple[dict, dict]:
    """Monta pares de contêiner de trânsito.

    Monta, se houverem, pares de trânsito aduaneiro.

    Args:
        db: conexão ao mongo
        codigo_conteiner: número do contêiner

    Returns destino, origem ->
      -> dicts retirados do Mongo (_id, metadata.numeroinformado, metadata.dataescaneamento)
    """
    try:
        destino = db.fs.files.find_one({'_id': _id},
                                       {'metadata.numeroinformado': 1,
                                        'metadata.dataescaneamento': 1})

        metadata = destino['metadata']
        origem = db.fs.files.find({'metadata.numeroinformado': metadata['numeroinformado'],
                                   'metadata.dataescaneamento':
                                       {'$lt': metadata['dataescaneamento']},
                                   'metadata.recinto': {'$nin': recintos_destino},
                                   'metadata.contentType': 'image/jpeg'},
                                  {'metadata.numeroinformado': 1,
                                   'metadata.dataescaneamento': 1}). \
            sort([('metadata.dataescaneamento', pymongo.DESCENDING)])[0]
        return destino, origem
    except Exception as err:
        logger.error(err, exc_info=True)
        return (None, None)


def save_image(db, savepath, _id):
    img_save = get_img_recortada(db, _id)
    img_save.save(os.path.join(savepath, str(_id) + '.jpg'))


def grava_imagens(db, _id: ObjectId, outpath='pares_transito'):
    if not os.path.exists(outpath):
        os.mkdir(outpath)
    destino, origem = get_pares(db, _id)
    if destino and origem:
        savepath = os.path.join(outpath, destino['metadata']['numeroinformado'])
        if not os.path.exists(savepath):
            os.mkdir(savepath)
        save_image(db, savepath, destino['_id'])
        save_image(db, savepath, origem['_id'])


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Recuperando pares...')
    # grava_imagens(db, 'MSKU9918380')
    inicio = datetime(2021, 1, 1)
    fim = datetime(2021, 9, 1)
    get_pares_periodo(db, inicio, fim, save=True)
