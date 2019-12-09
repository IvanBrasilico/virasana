"""Processamento das anomalias em cargas de um mesmo lote

Script de linha de comando para processar lotes (CE Mercante) e
gravar distância entre as imagens de lotes com um único NCM nos
metadados das imagens

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import time
from datetime import date, datetime, timedelta

import click

from virasana.db import mongodb as db
from virasana.models import anomalia_lote

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
    start = datetime.strptime(inicio, '%d/%m/%Y')
    end = datetime.strptime(fim, '%d/%m/%Y')
    print('Criando índices')
    anomalia_lote.create_indexes(db)
    print('Começando a integração... Inicio %s Fim %s' % (inicio, fim))
    tempo = time.time()
    qtde = anomalia_lote.processa_zscores(db, start, end)
    tempo = time.time() - tempo
    tempo_registro = 0 if (qtde == 0) else (tempo / qtde)
    print(qtde, 'zscores inseridos em fs.files em ',
          tempo, 'segundos.',
          tempo_registro, 'por registro')


if __name__ == '__main__':
    update()
