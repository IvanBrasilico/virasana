import sys
from datetime import datetime, timedelta, time
from statistics import mean, stdev
from typing import Iterable, Optional, List

# from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

sys.path.append('../bhadrasana2')

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque
from virasana.integracao.due.due_alchemy import Due


def parse_conteineres(lista_id_conteiner: str) -> List[str]:
    if not lista_id_conteiner:
        return []
    return [c.strip() for c in lista_id_conteiner.split(',') if c.strip()]


def first_event_datetime(events) -> Optional[datetime]:
    if not events:
        return None
    return min(e.dataHoraOcorrencia for e in events if e.dataHoraOcorrencia is not None)


def stats_dues(session: Session, inicio: datetime, fim: datetime, codigos_recintos: Iterable[str]):
    codigos_recintos = list(codigos_recintos)

    dues = (
        session.query(Due)
        .filter(Due.data_criacao_due >= inicio.date())
        .filter(Due.data_criacao_due < fim.date())
        .all()
    )

    resultados = []

    for due in dues:
        conteineres = parse_conteineres(due.lista_id_conteiner)
        if not conteineres:
            continue

        janela_inicio = datetime.combine(due.data_criacao_due, time.min) if due.data_criacao_due else inicio
        janela_fim = fim

        acessos = (
            session.query(AcessoVeiculo)
            .filter(AcessoVeiculo.numeroConteiner.in_(conteineres))
            .filter(AcessoVeiculo.direcao == 'E')
            .filter(AcessoVeiculo.codigoRecinto.in_(codigos_recintos))
            .filter(AcessoVeiculo.dataHoraOcorrencia >= janela_inicio)
            .filter(AcessoVeiculo.dataHoraOcorrencia <= janela_fim)
            .order_by(AcessoVeiculo.dataHoraOcorrencia.asc())
            .all()
        )

        embarques = (
            session.query(EmbarqueDesembarque)
            .filter(EmbarqueDesembarque.numeroConteiner.in_(conteineres))
            .filter(EmbarqueDesembarque.embarqueDesembarque == 'E')
            .filter(EmbarqueDesembarque.dataHoraOcorrencia >= janela_inicio)
            .filter(EmbarqueDesembarque.dataHoraOcorrencia <= janela_fim)
            .order_by(EmbarqueDesembarque.dataHoraOcorrencia.asc())
            .all()
        )

        data_entrada_operador = first_event_datetime(acessos)
        data_embarque = first_event_datetime(embarques)

        delta_criacao_entrada = None
        delta_criacao_embarque = None
        delta_entrada_embarque = None

        if data_entrada_operador:
            delta_criacao_entrada = data_entrada_operador - datetime.combine(due.data_criacao_due, time.min)

        if data_embarque:
            delta_criacao_embarque = data_embarque - datetime.combine(due.data_criacao_due, time.min)

        if data_entrada_operador and data_embarque:
            delta_entrada_embarque = data_embarque - data_entrada_operador

        resultados.append({
            "numero_due": due.numero_due,
            "data_criacao_due": due.data_criacao_due,
            "conteineres": conteineres,
            "data_entrada_operador": data_entrada_operador,
            "data_embarque": data_embarque,
            "delta_criacao_entrada": delta_criacao_entrada,
            "delta_criacao_embarque": delta_criacao_embarque,
            "delta_entrada_embarque": delta_entrada_embarque,
        })



    return resultados


def td_to_hours(td):
    return td.total_seconds() / 3600


def stats_tempos(resultados):
    total_dues = len(resultados)

    entradas_nulas = sum(1 for r in resultados if r["data_entrada_operador"] is None)
    embarques_nulos = sum(1 for r in resultados if r["data_embarque"] is None)

    def resumo_campo(campo):
        valores = [
            td_to_hours(r[campo])
            for r in resultados
            if r.get(campo) is not None
        ]
        if not valores:
            return {
                "n": 0,
                "media_horas": None,
                "desvio_padrao_horas": None,
            }
        return {
            "n": len(valores),
            "media_horas": mean(valores),
            "desvio_padrao_horas": stdev(valores) if len(valores) > 1 else 0.0,
        }

    stats = {
        "total_dues": total_dues,
        "entradas_nulas": entradas_nulas,
        "embarques_nulos": embarques_nulos,
        "delta_criacao_entrada": resumo_campo("delta_criacao_entrada"),
        "delta_criacao_embarque": resumo_campo("delta_criacao_embarque"),
        "delta_entrada_embarque": resumo_campo("delta_entrada_embarque"),
    }

    return stats


if __name__ == '__main__':  # pragma: no-cover
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define o fim às 00h de amanhã e início X dias antes
    fim = datetime.combine(datetime.now(), time.min) - timedelta(days=15)
    inicio = fim - timedelta(days=5)

    resultados = stats_dues(session, inicio, fim, ['8931318', '8931356', '8931359', '8931404'])

    resumo = stats_tempos(resultados)

    print(resumo["total_dues"])
    print(resumo["entradas_nulas"])
    print(resumo["embarques_nulos"])
    print(resumo["delta_criacao_entrada"])
    print(resumo["delta_criacao_embarque"])
    print(resumo["delta_entrada_embarque"])