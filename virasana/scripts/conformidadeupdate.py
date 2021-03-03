"""Processamento da conformidade de imagens x dados

Script de linha de comando para processar imagens no Banco de Dados
e avaliar resolução, ratio w/h, se tem imagem faltando, etc

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import datetime
import io
import sys
import time

import click
from PIL import Image
from bson import ObjectId
from sqlalchemy import create_engine, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.log import logger
from ajna_commons.flask.conf import SQL_URI
from ajna_commons.utils.images import mongo_image
from virasana.db import mongodb as db
from virasana.integracao.conformidade_alchemy import Conformidade

today = datetime.date.today()
str_today = datetime.datetime.strftime(today, '%d/%m/%Y')
lastweek = today - datetime.timedelta(days=7)
str_lastweek = datetime.datetime.strftime(lastweek, '%d/%m/%Y')


def update_conformidade(db, engine, start=None, end=None, limit=2000):
    Session = sessionmaker(bind=engine)
    session = Session()
    if start is None:
        start = session.query(func.max(Conformidade.uploadDate)).scalar()
    if end is None:
        end = datetime.datetime.now() + datetime.time.max
    tempo = time.time()
    query = {'metadata.contentType': 'image/jpeg',
             'uploadDate': {'$gte': start, '$lte': end}
             }
    logger.info(query)
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
            conformidade.uploadDate = linha['uploadDate']
            conformidade.id_imagem = str(linha['_id'])
            conformidade.dataescaneamento = linha['metadata']['dataescaneamento']
            conformidade.numeroinformado = linha['metadata']['numeroinformado']
            session.add(conformidade)
            session.commit()
            qtde += 1
        except UnicodeDecodeError:
            logger.error(f'Erro de encoding no id: {linha["_id"]}')
        except IntegrityError:
            session.rollback()
            logger.error(f'Linha duplicada: {linha["_id"]}')
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    logger.info(f'{qtde} análises de conformidade inseridas em {tempo} segundos.' +
                f'{tempo_registro} por registro')


def completa_conformidade(db, engine, limit=2000, start=None):
    Session = sessionmaker(bind=engine)
    session = Session()
    if start:
        lista_conformidade = session.query(Conformidade) \
            .filter(Conformidade.dataescaneamento >= start) \
            .filter(Conformidade.tipotrafego.is_(None)).limit(limit).all()
    else:
        lista_conformidade = session.query(Conformidade) \
            .filter(Conformidade.tipotrafego.is_(None)).limit(limit).all()

    tempo = time.time()
    qtde = 0
    for conformidade in lista_conformidade:
        row = db['fs.files'].find_one({'_id': ObjectId(conformidade.id_imagem)})
        tipotrafego = None
        vazio = None
        paisdestino = None
        try:
            metadata = row['metadata']
            carga = metadata.get('carga')
            if carga:
                conhecimento = carga.get('conhecimento')[0]
                if conhecimento:
                    tipotrafego = conhecimento.get('trafego')
                if tipotrafego:
                    vazio = False
                    paisdestino = conhecimento.get('paisdestino')
                else:
                    manifesto = carga.get('manifesto')[0]
                    tipotrafego = manifesto.get('trafego')
                    vazio = True
                    if tipotrafego == 'lce':
                        paisdestino = manifesto.get('codigoportodescarregamento')[:2]
            conformidade.tipotrafego = tipotrafego
            conformidade.vazio = vazio
            conformidade.paisdestino = paisdestino
            session.add(conformidade)
            session.commit()
            qtde += 1
        except Exception as err:
            logger.error(err, exc_info=True)
            session.rollback()
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    logger.info(f'{qtde} análises de conformidade complementadas em {tempo} segundos.' +
                f'{tempo_registro} por registro')


def preenche_isocode(db, engine, limit=2000, start=None):
    Session = sessionmaker(bind=engine)
    session = Session()
    if start:
        lista_conformidade = session.query(Conformidade) \
            .filter(Conformidade.dataescaneamento >= start) \
            .filter(Conformidade.isocode_size.is_(None)).limit(limit).all()
    else:
        lista_conformidade = session.query(Conformidade) \
            .filter(Conformidade.isocode_size.is_(None)).limit(limit).all()
    tempo = time.time()
    qtde = 0
    querys = [
        'SELECT distinct isoCode FROM dbmercante.itensresumo where codigoConteiner="%s"',
        'SELECT distinct isoConteinerVazio FROM dbmercante.conteinervazioresumo ' +
        'WHERE idConteinerVazio="%s"'
    ]
    try:
        for conformidade in lista_conformidade:
            isocode = None
            for query in querys:
                rs = session.execute(query % conformidade.numeroinformado)
                lines = list(rs)
                if lines and len(lines) > 0:
                    isocode = lines[0][0].strip()
                    break
            if isocode:
                conformidade.isocode_size = isocode[:3]
                conformidade.isocode_group = isocode[-2:]
                session.add(conformidade)
                qtde += 1
        session.commit()
    except Exception as err:
        logger.error(err, exc_info=True)
        session.rollback()
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    logger.info(f'{qtde} isocode preenchidos em {tempo} segundos.' +
                f'{tempo_registro} por registro')


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
    end = datetime.datetime.strptime(fim, '%d/%m/%Y')
    end = datetime.datetime.strptime(fim, '%d/%m/%Y')
    start = None
    if limit == 0:
        start = datetime.datetime.strptime(inicio, '%d/%m/%Y')
    print('Começando a integração... Inicio %s Fim %s' % (inicio, fim))
    update_conformidade(db, engine, start, end, limit)


if __name__ == '__main__':
    update()
