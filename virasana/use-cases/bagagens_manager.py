from integracao.mercante.mercantealchemy import Item
from sqlalchemy import func, and_, desc, text


def get_bagagens(session, datainicio=None, datafim=None,
                 order=None, reverse=False, paginaatual=0):
    ROWS_PER_PAGE = 10
    filtro = and_(Item.NCM.startswith('9797'),
                  Item.create_date.between(datainicio, datafim))
    npaginas = int(session.query(func.count(Item.ID)).filter(filtro).scalar()
                   / ROWS_PER_PAGE)
    q = session.query(Item).filter(filtro)
    if order:
        print(reverse)
        if reverse:
            q = q.order_by(desc(text(order)))
        else:
            q = q.order_by(text(order))
    lista_conformidade = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * paginaatual).all()
    print(datainicio, datafim)
    return lista_conformidade, npaginas
