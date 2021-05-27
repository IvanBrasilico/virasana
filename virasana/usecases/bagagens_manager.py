from typing import List, Tuple

from virasana.integracao.carga import get_peso_balanca
from pymongo.database import Database
from virasana.integracao.mercante.mercantealchemy import Item, Conhecimento


def get_bagagens(mongodb: Database,
                 session, datainicio=None, datafim=None,
                 paginaatual=0) -> Tuple[List[Item], int]:
    query = '''SELECT codigoConteiner, c.numeroCEmercante FROM itensresumo i
    INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
    WHERE NCM LIKE '9797%' AND c.tipoBLConhecimento IN(10, 11) AND c.tipoTrafego = 5
    AND c.portoDestFinal = 'BRSSZ' 
    AND c.dataEmissao BETWEEN :datainicio AND :datafim'''
    ROWS_PER_PAGE = 10
    conteineres = session.execute(query, {'datainicio': datainicio, 'datafim': datafim})
    # lista_itens = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * paginaatual).all()
    # lista_itens = q.fetchall()
    print(datainicio, datafim)
    lista_itens = []

    for row in conteineres:
        item = session.query(Item).filter(Item.codigoConteiner == row[0]). \
            filter(Item.numeroCEmercante == row[1]). \
            one_or_none()
        conhecimento = session.query(Conhecimento).filter(
            Conhecimento.numeroCEmercante == item.numeroCEmercante).one_or_none()
        filhotes = session.query(Conhecimento).filter(
            Conhecimento.numeroCEMaster == item.numeroCEmercante).all()
        item.conhecimentos = [conhecimento, *filhotes]
        grid_data = mongodb['fs.files'].find_one(
            {'metadata.carga.conhecimento.conhecimento': item.numeroCEmercante})
        item.pesoBalanca = 0.
        if grid_data:
            item.id_imagem = grid_data.get('_id')
            item.pesoBalanca = get_peso_balanca(grid_data.get('metadata').get('pesagens'))
        lista_itens.append(item)
    npaginas = 0
    return lista_itens, npaginas
