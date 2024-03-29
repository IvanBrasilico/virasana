import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Tuple

from pymongo import MongoClient
from sqlalchemy import and_, func, desc, text, or_, create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
from ajna_commons.flask.conf import MONGODB_URI, SQL_URI, DATABASE
from ajna_commons.flask.log import logger
from virasana.forms.filtros import FormFiltroAlerta
from virasana.models.auditoria import Auditoria
from virasana.integracao.risco.alertas_alchemy import Alerta, EstadoAlerta, \
    Apontamento, NivelAlerta

ROWS_PER_PAGE = 10


def get_alertas_imagem(session, _id: str) -> List[Alerta]:
    q = session.query(Alerta).filter(Alerta.id_imagem == _id).all()
    return q.all()


dict_condicao = {
    'AND': and_,
    'OR': or_
}


class FiltroAlerta():
    def __init__(self, datainicio, datafim):
        self.filtro = and_(Alerta.dataescaneamento.between(datainicio, datafim))

    def add_campo(self, campo, valor,
                  condicao='AND', operador='IGUAL'):
        condicao_ = dict_condicao[condicao]
        self.filtro = condicao_(self.filtro, getattr(Alerta, campo) == valor)


def get_alertas_filtro(session, form: FormFiltroAlerta) -> Tuple[List[Alerta], int]:
    start = datetime.combine(form.start.data, datetime.min.time())
    end = datetime.combine(form.end.data, datetime.max.time())
    recinto = form.recinto.data
    order = form.order.data
    reverse = form.reverse.data
    paginaatual = form.pagina_atual.data
    filtroalerta = FiltroAlerta(start, end)
    if recinto:
        filtroalerta.add_campo('cod_recinto', recinto)
    filtro = filtroalerta.filtro
    npaginas = int(session.query(func.count(Alerta.ID)).filter(filtro).scalar()
                   / ROWS_PER_PAGE) + 1
    q = session.query(Alerta).filter(filtro)
    if order:
        if reverse:
            q = q.order_by(desc(text(order)))
        else:
            q = q.order_by(text(order))
    logger.info(str(q))
    logger.info(' '.join([recinto, str(start), str(end),
                          str(order), str(paginaatual)]))
    lista_alertas = q.limit(ROWS_PER_PAGE).offset(ROWS_PER_PAGE * (paginaatual - 1)).all()
    return lista_alertas, npaginas


def get_alertas_filtro_agrupados(session, form: FormFiltroAlerta) -> Tuple[dict, int]:
    lista_alertas, npaginas = get_alertas_filtro(session, form)
    result = defaultdict(list)
    for alerta in lista_alertas:
        result[alerta.numeroinformado].append(alerta)
    return result, npaginas


class Integrador():
    def __init__(self, session, db_ajna, db_fichas):
        self.session = session
        self.db_ajna = db_ajna
        self.db_fichas = db_fichas

    def integra_filtro(self, apontamento: Apontamento, function_filtro,
                       inicio: datetime):
        cursor = function_filtro(self.db_ajna, inicio)
        for row in cursor[:5]:
            alerta = Alerta()
            alerta.id_imagem = row['_id']
            metadata = row['metadata']
            alerta.numeroinformado = metadata['numeroinformado']
            alerta.dataescaneamento = metadata['dataescaneamento']
            alerta.apontamento_id = apontamento.ID
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
    filtro.update({'metadata.dataescaneamento': {'$gt': data}})
    print(filtro)
    return db['fs.files'].find(filtro)


def filtro_nvazio(db, data):
    filtro = Auditoria.FILTROS_AUDITORIA['2']['filtro']
    filtro.update({'metadata.dataescaneamento': {'$gt': data}})
    print(filtro)
    return db['fs.files'].find(filtro)


def filtro_peso(db, data):
    filtro = Auditoria.FILTROS_AUDITORIA['4']['filtro']
    filtro.update({'metadata.dataescaneamento': {'$gt': data}})
    print(filtro)
    return db['fs.files'].find(filtro)

def filtro_reefer_contaminado(db, data):
    filtro = Auditoria.FILTROS_AUDITORIA['11']['filtro']
    filtro.update({'metadata.dataescaneamento': {'$gt': data}})
    print(filtro)
    return db['fs.files'].find(filtro)


def captura_caixa_corporativa():
    return []

tipos_apontamento = {
    'Vazio com carga': filtro_vazio,
    'Não vazio sem carga': filtro_nvazio,
    'Peso declarado X Balança': filtro_peso,
    'Alertas de recintos': captura_caixa_corporativa,
    'Reefer contaminado': filtro_reefer_contaminado

}

def processa_apontamentos(session, db, db_fichas):
    """Processa o dicionário de apontamentos, para criar alertas
    Args:
        session: conexão ao MySQL
        conn: conexão ao MongoDB
    """
    for nome_apontamento, funcao_apontamento in tipos_apontamento.items():
        apontamento = session.query(Apontamento). \
            filter(Apontamento.nome == nome_apontamento).one_or_none()
        inicio = session.query(func.max(Alerta.dataescaneamento)). \
            filter(Alerta.apontamento_id == apontamento.ID).scalar()
        if inicio is None:
            inicio = datetime.now() - timedelta(days=10)
        integrador = Integrador(session, db, db_fichas)
        integrador.integra_filtro(apontamento, funcao_apontamento, inicio)

if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    conn = MongoClient(host=MONGODB_URI)
    db = conn[DATABASE]
    for nome_apontamento, funcao_apontamento in tipos_apontamento.items():
        apontamento = session.query(Apontamento). \
            filter(Apontamento.nome == nome_apontamento).one_or_none()
        if not apontamento:
            apontamento = Apontamento()
            apontamento.nome = nome_apontamento
            apontamento.nivel = NivelAlerta.Alto.value
            session.add(apontamento)
            session.commit()

