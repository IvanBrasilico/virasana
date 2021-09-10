"""Processamento de pares do trânsito para treinamentos

Script de linha de comando para criar base de imagens para
treinamento de similaridade no trânsito

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa
    caminho: onde gravar os dados

"""
import time
from datetime import date, datetime, timedelta

import click
from virasana.integracao.transito.monta_base import get_pares_periodo

from virasana.db import mongodb as db

today = date.today()
str_today = datetime.strftime(today, '%d/%m/%Y')
trinta_dias = today - timedelta(days=30)
str_trinta_dias = datetime.strftime(trinta_dias, '%d/%m/%Y')


@click.command()
@click.option('--inicio', default=str_trinta_dias,
              help='Dia de início (dia/mês/ano) - padrão há 30 dias')
@click.option('--fim', default=str_today,
              help='Dia de fim (dia/mês/ano) - padrão hoje')
@click.option('--caminho', default='/data/ajna_dump/pares_transito/',
              help='Caminho para gravação')
def update(inicio, fim, caminho):
    """Script de linha de comando para integração do arquivo XML."""
    start = datetime.strptime(inicio, '%d/%m/%Y')
    end = datetime.strptime(fim, '%d/%m/%Y')
    print('Começando a gravação da base... Inicio %s Fim %s' % (inicio, fim))
    get_pares_periodo(db, inicio, fim, save=True, outpath=caminho)


if __name__ == '__main__':
    update()
