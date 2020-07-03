import sys
from sqlalchemy import create_engine

from ajna_commons.flask.conf import SQL_URI
from virasana.integracao.mercante.processa_xml_mercante import get_arquivos_novos, \
    xml_para_mercante
from virasana.integracao.mercante.resume_mercante import mercante_resumo


def do():
    lote = 100
    if len(sys.argv) > 1:
        lote = int(sys.argv[1])
        print('Lote de %s arquivos' % lote)
    sql = create_engine(SQL_URI)
    print('Baixando arquivos novos...')
    # get_arquivos_novos(sql)
    print('Processando XML...')
    xml_para_mercante(sql, lote)
    print('Fazendo resumo operações...')
    mercante_resumo(sql)


if __name__ == '__main__':
    do()
