import json
import pymongo
from bson import ObjectId
from ajna_commons.flask.log import logger


def update_due(db, dues):
    # print(dues)
    for _id, due in dues.items():
        print('Updating %s ' % _id)
        print('with %s ' % json.dumps(due)[:50])
        result = db.fs.files.update_one(
            {'_id': ObjectId(_id)},
            {'$set': {'metadata.due': due, 'metadata.carga.vazio': False}}
        )
        print(result)


CHAVES_DUE = [
    'metadata.due.numero',
    'metadata.due.ruc',
    'metadata.due.Declarante',
    'metadata.due.PaisImportador',
    'metadata.due.itens.Exportador',
    'metadata.due.itens.recintoAduaneiroDespacho.codigo',
    'metadata.due.itens.recintoAduaneiroEmbarque.codigo',
]


def create_indexes(db):
    """Utilitário. Cria índices relacionados à integração.

    São criados índices para desempenho nas consultas.
    Alguns índices únicos também são criados, estes para evitar importação
    duplicada do mesmo registro.
    """
    for campo in CHAVES_DUE:
        try:
            db['fs.files'].create_index(campo)
        except pymongo.errors.OperationFailure:
            pass


def get_metadata_due(grid_data):
    print('Metadata DUE')
    if grid_data:
        metadata = grid_data.get('metadata')
        print(metadata)
        if metadata:
            due = metadata.get('due')
            print(due)
            if due:
                return due
    return None


def get_dados(grid_data):
    try:
        metadata = get_metadata_due(grid_data)
        if metadata:
            declarante = metadata.get('Declarante')
            nome_declarante = declarante.get('Nome Declarante')
            destino = metadata.get('paisImportador')
            numero = metadata.get('numero')
            ruc = metadata.get('ruc')
            return 'DUE %s - RUC %s - IMPORTADOR %s/%s - DESTINO %s' % \
                   (numero, ruc, declarante, nome_declarante, destino)
        return ''
    except Exception as err:
        logger.error(err, exc_info=True)
        return ''


if __name__ == '__main__':  # pragma: no cover
    from pymongo import MongoClient
    from ajna_commons.flask.conf import DATABASE, MONGODB_URI

    db = MongoClient(host=MONGODB_URI)[DATABASE]
    print('Criando índices para DUE')
    create_indexes(db)
