from datetime import datetime, timedelta

from sqlalchemy import func, case, exists, and_
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (CicloDeVidaImportacao,
                                                   CicloDeVidaImportacaoEscaneamentos)


def _limite_recente():
    """Entradas há menos de 3h ainda não tiveram tempo hábil de escaneamento."""
    return datetime.now() - timedelta(hours=3)


def query_completude_agregada(session, data_inicio, data_fim):
    Ciclo = CicloDeVidaImportacao
    Esc = CicloDeVidaImportacaoEscaneamentos

    tem_escaneamento = exists().where(Esc.ciclo_de_vida_id == Ciclo.ID)
    tem_imagem = exists().where(and_(Esc.ciclo_de_vida_id == Ciclo.ID, Esc.imagem_id.isnot(None)))

    total = func.count(Ciclo.ID)
    sem_escaneamento = func.sum(case((~tem_escaneamento, 1), else_=0))
    sem_imagem = func.sum(case((~tem_imagem, 1), else_=0))

    linhas = (
        session.query(Ciclo.codigoRecintoArmazenagem, total, sem_escaneamento, sem_imagem)
        .filter(Ciclo.data_entrada_seco.isnot(None))
        .filter(Ciclo.data_entrada_seco.between(data_inicio, data_fim))
        .filter(Ciclo.data_entrada_seco < _limite_recente())
        .group_by(Ciclo.codigoRecintoArmazenagem)
        .all()
    )

    resultados = []
    for cod_recinto, tot, sem_esc, sem_img in linhas:
        sem_esc = sem_esc or 0
        sem_img = sem_img or 0
        pct = (sem_esc / tot) if tot else 0
        resultados.append({
            'codigo_recinto': cod_recinto,
            'total': tot,
            'sem_escaneamento': sem_esc,
            'sem_imagem': sem_img,
            'pct_sem_escaneamento': pct,
        })
    resultados.sort(key=lambda r: r['pct_sem_escaneamento'], reverse=True)
    return resultados


def query_completude_detalhe(session, data_inicio, data_fim, cod_recinto):
    Ciclo = CicloDeVidaImportacao
    Esc = CicloDeVidaImportacaoEscaneamentos

    return (
        session.query(
            Ciclo.numeroConteiner,
            Ciclo.data_entrada_seco,
            Esc.escaneamento_id,
            Esc.imagem_id,
        )
        .outerjoin(Esc, Esc.ciclo_de_vida_id == Ciclo.ID)
        .filter(Ciclo.data_entrada_seco.isnot(None))
        .filter(Ciclo.data_entrada_seco.between(data_inicio, data_fim))
        .filter(Ciclo.data_entrada_seco < _limite_recente())
        .filter(Ciclo.codigoRecintoArmazenagem == cod_recinto)
        .order_by(Ciclo.data_entrada_seco)
        .all()
    )
