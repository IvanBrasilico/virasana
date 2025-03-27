"""
Definição dos códigos que serão rodados pelo Celery.

Background tasks do sistema AJNA-virasana
Gerenciados por celery.sh
Aqui ficam as que rodam tarefas custosas/demoradas em background.

"""
# Código dos celery tasks

import gridfs
from pymongo import MongoClient

from ajna_commons.models.bsonimage import BsonImageList


# Tasks que respondem a ações da VIEW
def trata_bson(bson_file: str, db: MongoClient) -> list:
    """Recebe o nome de um arquivo bson e o insere no MongoDB."""
    # .get_default_database()
    fs = gridfs.GridFS(db)
    bsonimagelist = BsonImageList.fromfile(abson=bson_file)
    files_ids = bsonimagelist.tomongo(fs)
    return files_ids
