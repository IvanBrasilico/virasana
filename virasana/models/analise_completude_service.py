from datetime import datetime, timedelta

from sqlalchemy import func, case, exists, and_

from bhadrasana.models.ovr import Recinto
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao, \
    CicloDeVidaImportacaoEscaneamentos


def _limite_recente():
    """Entradas há menos de 3h ainda não tiveram tempo hábil de escaneamento."""
    return datetime.now() - timedelta(hours=3)

def query_completude_agregada(session, data_inicio, data_fim, cod_recinto=None):
    Ciclo = CicloDeVidaImportacao
    Esc = CicloDeVidaImportacaoEscaneamentos

    tem_escaneamento = exists().where(Esc.ciclo_de_vida_id == Ciclo.ID)
    tem_imagem = exists().where(and_(Esc.ciclo_de_vida_id == Ciclo.ID, Esc.imagem_id.isnot(None)))

    total = func.count(Ciclo.ID)
    sem_escaneamento = func.sum(case((~tem_escaneamento, 1), else_=0))
    sem_imagem = func.sum(case((~tem_imagem, 1), else_=0))
    total_saidas = func.sum(case((Ciclo.data_saida_seco.isnot(None), 1), else_=0))
    saidas_sem_escaneamento = func.sum(
        case((and_(Ciclo.data_saida_seco.isnot(None), ~tem_escaneamento), 1), else_=0)
    )

    query = (
        session.query(
            Ciclo.codigoRecintoArmazenagem,
            Recinto.nome,
            total,
            sem_escaneamento,
            sem_imagem,
            total_saidas,
            saidas_sem_escaneamento,
        )
        .outerjoin(Recinto, Recinto.cod_siscomex == Ciclo.codigoRecintoArmazenagem)
        .filter(Ciclo.data_entrada_seco.isnot(None))
        .filter(Ciclo.data_entrada_seco.between(data_inicio, data_fim))
        .filter(Ciclo.data_entrada_seco < _limite_recente())
    )
    if cod_recinto is not None:
        query = query.filter(Ciclo.codigoRecintoArmazenagem == cod_recinto)

    linhas = query.group_by(Ciclo.codigoRecintoArmazenagem, Recinto.nome).all()

    resultados = [_monta_item_completude(*linha) for linha in linhas]
    resultados.sort(key=lambda r: r['pct_sem_escaneamento'], reverse=True)
    return resultados


def _monta_item_completude(cod_recinto, nome_recinto, tot, sem_esc, sem_img, tot_saidas, saidas_sem_esc):
    sem_esc = sem_esc or 0
    sem_img = sem_img or 0
    tot_saidas = tot_saidas or 0
    saidas_sem_esc = saidas_sem_esc or 0
    return {
        'codigo_recinto': cod_recinto,
        'nome_recinto': nome_recinto or '(recinto não cadastrado)',
        'total': tot,
        'sem_escaneamento': sem_esc,
        'sem_imagem': sem_img,
        'pct_sem_escaneamento': (sem_esc / tot) if tot else 0,
        'total_saidas': tot_saidas,
        'saidas_sem_escaneamento': saidas_sem_esc,
        'pct_saidas_sem_escaneamento': (saidas_sem_esc / tot_saidas) if tot_saidas else 0,
    }


def query_completude_totais_recinto(session, data_inicio, data_fim, cod_recinto):
    """Totais de um único recinto — reaproveita query_completude_agregada filtrada."""
    resultados = query_completude_agregada(session, data_inicio, data_fim, cod_recinto=cod_recinto)
    if resultados:
        return resultados[0]
    recinto = session.query(Recinto).filter(Recinto.cod_siscomex == cod_recinto).one_or_none()
    return _monta_item_completude(cod_recinto, recinto.nome if recinto else None, 0, 0, 0, 0, 0)

def query_completude_detalhe(session, data_inicio, data_fim, cod_recinto):
    Ciclo = CicloDeVidaImportacao
    Esc = CicloDeVidaImportacaoEscaneamentos

    return (
        session.query(
            Ciclo.numeroConteiner,
            Ciclo.data_entrada_seco,
            Ciclo.data_saida_seco,
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


def totais_completude(resultados):
    total = sum(r['total'] for r in resultados)
    sem_escaneamento = sum(r['sem_escaneamento'] for r in resultados)
    sem_imagem = sum(r['sem_imagem'] for r in resultados)
    total_saidas = sum(r['total_saidas'] for r in resultados)
    saidas_sem_escaneamento = sum(r['saidas_sem_escaneamento'] for r in resultados)
    return {
        'total': total,
        'sem_escaneamento': sem_escaneamento,
        'sem_imagem': sem_imagem,
        'pct_sem_escaneamento': (sem_escaneamento / total) if total else 0,
        'total_saidas': total_saidas,
        'saidas_sem_escaneamento': saidas_sem_escaneamento,
        'pct_saidas_sem_escaneamento': (saidas_sem_escaneamento / total_saidas) if total_saidas else 0,
    }