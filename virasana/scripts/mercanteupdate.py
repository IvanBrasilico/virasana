"""Processamento de dados novos do Mercante

Script de linha de comando para processar lotes de transmissão do Mercante e
localizar/integrar dados do sistema Mercante aos metadados das imagens

Script segue 4 passsos:

1 - Baixar novos arquivos XML do Mercante, se houver
2 - Processar arquivos do Mercante (basicamente copiar dados para tabela MySQL
3 - "Resumir" e tratar dados - Dados podem ser inserção, atualização ou exclusão
4 - Integrar dados do Mercante em fs.files

Args:

    diainicio: dia de início da pesquisa
    diasantes: quantos dias regredir (processa um dia por vez na parte da integração)

"""
import time
from datetime import date, datetime

import click
from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import create_engine

from virasana.db import mongodb as db
from virasana.integracao.mercante import mercante_fsfiles
from virasana.integracao.mercante import processa_xml_mercante
from virasana.integracao.mercante import resume_mercante

today = date.today()
str_today = datetime.strftime(today, '%d/%m/%Y')


@click.command()
@click.option('--dias', default=10,
              help='Quantidade de dias a processar para trás - padrão 10')
@click.option('--fim', default=str_today,
              help='Dia de fim (dia/mês/ano) - padrão hoje')
def update(dias, fim):
    """Script de linha de comando para integração do arquivo XML."""
    end = datetime.strptime(fim, '%d/%m/%Y')
    print('Começando a integração... Fim %s  Dias antes %s' % (fim, dias))
    connection = create_engine(SQL_URI)
    tempo = time.time()

    processa_xml_mercante.get_arquivos_novos(connection, end, dias)
    processa_xml_mercante.xml_para_mercante(connection)
    resume_mercante.mercante_resumo(connection)
    mercante_fsfiles.update_mercante_fsfiles_dias(db, connection, end, dias)
    tempototal = time.time() - tempo
    print('Tempo total: %s' % tempototal)


if __name__ == '__main__':
    update()
