from typing import List

from sqlalchemy import and_, func, desc, text

from virasana.integracao.risco.alertas_alchemy import Alerta

ROWS_PER_PAGE = 10

def get_alertas_imagem(session, _id: str)-> List[Alerta]:
    q = session.query(Alerta).filter(Alerta.id_imagem == _id).all()
    return q.all()


def get_alertas_filtro(session, recinto=None, datainicio=None, datafim=None,
                order=None, reverse=False, paginaatual=1):
    filtro = and_(Alerta.dataescaneamento.between(datainicio, datafim))
    if recinto:
        filtro = and_(filtro, Alerta.cod_recinto == recinto)
    npaginas = int(session.query(func.count(Alerta.ID)).filter(filtro).scalar()
                   / ROWS_PER_PAGE)
    q = session.query(Alerta).filter(filtro)
    if order:
        if reverse:
            q = q.order_by(desc(text(order)))
        else:
            q = q.order_by(text(order))
    lista_alertas = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * (paginaatual - 1)).all()
    print(datainicio, datafim)
    return lista_alertas, npaginas
