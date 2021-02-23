"""Processamento da conformidade de imagens x dados

Script de linha de comando para processar imagens no Banco de Dados
e avaliar resolução, ratio w/h, se tem imagem faltando, etc

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import io
import sys
import time
from datetime import date, datetime, timedelta

import click
from PIL import Image
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import SQL_URI
from ajna_commons.utils.images import mongo_image
from virasana.db import mongodb as db
from virasana.integracao.conformidade_alchemy import Conformidade

today = date.today()
str_today = datetime.strftime(today, '%d/%m/%Y')
lastweek = today - timedelta(days=7)
str_lastweek = datetime.strftime(lastweek, '%d/%m/%Y')


@click.command()
@click.option('--inicio', default=str_lastweek,
              help='Dia de início (dia/mês/ano) - padrão uma semana')
@click.option('--fim', default=str_today,
              help='Dia de fim (dia/mês/ano) - padrão hoje')
@click.option('--limit', default=1000,
              help='Quantidade de registros - padrão 1000 - informar 0 para usar datas')
def update(inicio, fim, limit):
    """Script de linha de comando para integração do arquivo XML."""
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    end = datetime.strptime(fim, '%d/%m/%Y')
    if limit == 0:
        start = datetime.strptime(inicio, '%d/%m/%Y')
    else:
        start = session.query(func.max(Conformidade.datahora)).scalar()
    print('Começando a integração... Inicio %s Fim %s' % (inicio, fim))
    tempo = time.time()
    query = {'metadata.contentType': 'image/jpeg',
             'uploadDate': {'$gte': start, '$lte': end}
             }
    print(query)
    cursor = db['fs.files'].find(query).limit(limit)
    qtde = 0
    for linha in cursor:
        image_bytes = mongo_image(db, linha['_id'])
        # print(image_bytes)
        try:
            image = Image.open(io.BytesIO(image_bytes))
            conformidade = Conformidade()
            conformidade.set_size(image.size)
            conformidade.cod_recinto = linha['metadata']['recinto']
            conformidade.datahora = linha['uploadDate']
            conformidade.id_imagem = linha['_id']
            session.add(conformidade)
            session.commit()
            qtde += 1
        except UnicodeDecodeError:
            print(f'Erro de encoding no id: {linha["_id"]}')
        except IntegrityError:
            session.rollback()
            print(f'Linha duplicada: {linha["_id"]}')
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    print(qtde, 'análises de conformidade inseridas em ',
          tempo, 'segundos.',
          tempo_registro, 'por registro')


if __name__ == '__main__':
    update()
