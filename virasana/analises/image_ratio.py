""""Análise do ratio de imagens por Recinto/Escâner.

Extrai e sumariza relação largura/altura de imagens agrupando por
por Recinto/Escâner para permitir a detecção de imagens que estão
sendo geradas com poucos pulsos de X-Ray/pouca informação e consequentemente
terão a qualidade prejudicada.

"""
import io
import sys
import time
from collections import defaultdict

sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')

from virasana.db import mongodb as db
from ajna_commons.utils.images import mongo_image
from PIL import Image


def do():
    print('Iniciando...')
    s0 = time.time()
    sizes_recinto = defaultdict(list)
    cursor = db.fs.files.find({'metadata.contentType': 'image/jpeg',
                               'metadata.recinto': {'$exists': True}},
                              {'_id': 1, 'metadata.recinto': 1}).limit(100)
    for doc in cursor:
        _id = doc['_id']
        image = Image.open(io.BytesIO(mongo_image(db, _id)))
        # print(image.size)
        sizes_recinto[doc['metadata']['recinto']].append(image.size)
    s1 = time.time()
    print('{:0.2f} segundos'.format(s1 - s0))
    print(sizes_recinto)


if __name__ == '__main__':
    do()
