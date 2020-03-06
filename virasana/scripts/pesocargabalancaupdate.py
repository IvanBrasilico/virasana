"""Processamento das pesagens de balança

Script de linha de comando para inclusão
dos metadados das comparações entre peso declarado x balança

Args:

    diainicio: dia de início da pesquisa
    diafim: dia final da pesquisa

"""
import time
from datetime import date, datetime, timedelta

import click

from virasana.db import mongodb as db
from virasana.integracao import carga

today = date.today()
str_today = datetime.strftime(today, '%d/%m/%Y')
yesterday = today - timedelta(days=1)
str_yesterday = datetime.strftime(yesterday, '%d/%m/%Y')


@click.command()
@click.option('--batch_size', default=1000,
              help='Lote de registros para processar (padrão 1000)')
def update(batch_size):
    """Script de linha de comando para geração de resumos de peso."""
    carga.cria_campo_pesos_carga_pesagem(db, batch_size=batch_size)


if __name__ == '__main__':
    update()
