import sys
from datetime import datetime, timedelta
from typing import List

from pymongo import MongoClient
from sqlalchemy import and_, func, desc, text, create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import SQL_URI, MONGODB_URI, DATABASE
from ajna_commons.flask.log import logger
from virasana.models.auditoria import Auditoria
from virasana.integracao.risco.alertas_alchemy import Alerta, Apontamento, NivelAlerta, EstadoAlerta

ROWS_PER_PAGE = 10


def get_alertas_imagem(session, _id: str) -> List[Alerta]:
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
    logger.info(str(q))
    logger.info(' '.join([recinto, str(datainicio), str(datafim),
                          str(order), str(paginaatual)]))
    lista_alertas = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * (paginaatual - 1)).all()
    print(datainicio, datafim)
    return lista_alertas, npaginas


class Integrador():
    def __init__(self, session, db_ajna, db_fichas):
        self.session = session
        self.db_ajna = db_ajna
        self.db_fichas = db_fichas

    def integra_filtro(self, apontamento: Apontamento, function_filtro):
        inicio = datetime.now() - timedelta(days=10)
        cursor = function_filtro(self.db_ajna, inicio)
        for row in cursor[:5]:
            alerta = Alerta()
            alerta.id_imagem = row['_id']
            metadata = row['metadata']
            alerta.numeroinformado = metadata['numeroinformado']
            alerta.dataescaneamento = metadata['dataescaneamento']
            alerta.origem = apontamento.ID
            alerta.nivel = apontamento.nivel
            alerta.estado = EstadoAlerta.Ativo
            self.session.add(alerta)
        try:
            self.session.commit()
        except Exception as err:
            self.session.rollback()
            raise err


def filtro_vazio(db, data):
    filtro = Auditoria.FILTROS_AUDITORIA['1']['filtro']
    filtro.update({'uploadDate': {'$gt': data}})
    print(filtro)
    return db['fs.files'].find(filtro)


if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    apontamento_vazio = session.query(Apontamento). \
        filter(Apontamento.nome == 'vazio').one_or_none()
    if not apontamento_vazio:
        apontamento_vazio = Apontamento()
        apontamento_vazio.nome = 'vazio'
        apontamento_vazio.nivel = NivelAlerta.Alto
        session.add(apontamento_vazio)
        session.commit()
        session.refresh(apontamento_vazio)
    conn = MongoClient(host=MONGODB_URI)
    db = conn[DATABASE]
    integrador = Integrador(session, db, None)
    integrador.integra_filtro(apontamento_vazio, filtro_vazio)
