"""Processamento da conformidade de imagens x dados

Script de linha de comando para processar imagens no Banco de Dados
e avaliar resolução, ratio w/h, se tem imagem faltando, etc

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import sys
import time
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import click

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import SQL_URI
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
def update(inicio, fim):
    """Script de linha de comando para integração do arquivo XML."""
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    start = datetime.strptime(inicio, '%d/%m/%Y')
    end = datetime.strptime(fim, '%d/%m/%Y')
    print('Começando a integração... Inicio %s Fim %s' % (inicio, fim))
    tempo = time.time()
    query = {'metadata.contentType': 'image/jpeg',
             'metadata.dataescaneamento': {'$gte': start, '$lte': end}
             }
    print(query)
    cursor = db['fs.files'].find(query)
    for linha in cursor:
        image = mongo_image(linha['_id'])
        conformidade = Conformidade()
        conformidade.set_size(image.size)
        conformidade.cod_recinto = linha['recinto']
        conformidade.id_imagem = linha['_id']
        session.add(conformidade)
    session.commit()
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    print(qtde, 'análises de conformidade inseridas em ',
          tempo, 'segundos.',
          tempo_registro, 'por registro')


if __name__ == '__main__':
    update()
