import sys
from datetime import time

sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from ajna_commons.flask.log import logger

from bhadrasana.models.apirecintos import InspecaoNaoInvasiva, EmbarqueDesembarque, AcessoVeiculo
from datetime import datetime, date, timedelta
from collections import defaultdict


def _json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def valida_regra_escaneamento_json(session, data_inicio, data_fim, recintos):
    recintos = [str(r) for r in recintos if r is not None]

    logger.info(
        "Pesquisando desembarques - parâmetros recintos=%s, intervalo=%s até %s",
        recintos, data_inicio, data_fim
    )

    desembarques = (
        session.query(EmbarqueDesembarque)
        .filter(EmbarqueDesembarque.codigoRecinto.in_(recintos))
        .filter(EmbarqueDesembarque.embarqueDesembarque == 'D')
        .filter(EmbarqueDesembarque.dataHoraOcorrencia >= data_inicio)
        .filter(EmbarqueDesembarque.dataHoraOcorrencia < data_fim)
        .all()
    )

    logger.info("%s desembarques encontrados", len(desembarques))

    contes = sorted({d.numeroConteiner for d in desembarques if d.numeroConteiner})
    if not contes:
        resultado = {
            "sem_saida_sem_escaneamento": [],
            "com_saida_sem_escaneamento": [],
            "escaneados_apos_24h": [],
            "info": {
                "parametros": {
                    "recintos": recintos,
                    "intervaloPesquisa": {
                        "data_inicio": data_inicio,
                        "data_fim": data_fim,
                    },
                },
                "totais": {
                    "por_recinto": [],
                    "geral": {
                        "totalDesembarque": 0,
                        "totalSemEscaneamento": 0,
                    },
                },
            },
        }
        return _json_safe(resultado)

    logger.info("Pesquisando inspecoes - recintos=%s", recintos)
    escaneamentos = (
        session.query(InspecaoNaoInvasiva)
        .filter(InspecaoNaoInvasiva.numeroConteiner.in_(contes))
        .filter(InspecaoNaoInvasiva.dataHoraOcorrencia >= data_inicio - timedelta(hours=1))
        .filter(InspecaoNaoInvasiva.dataHoraOcorrencia <= data_fim + timedelta(hours=72))
        .all()
    )
    logger.info("%s inspecoes encontradas", len(escaneamentos))

    logger.info("Pesquisando entradas e saidas - recintos=%s", recintos)
    acessos = (
        session.query(AcessoVeiculo)
        .filter(AcessoVeiculo.numeroConteiner.in_(contes))
        .filter(AcessoVeiculo.dataHoraOcorrencia >= data_inicio)
        .filter(AcessoVeiculo.dataHoraOcorrencia <= data_fim + timedelta(hours=72))
        .all()
    )
    logger.info("%s acessos encontrados", len(acessos))

    escaneamentos_por_conteiner = defaultdict(list)
    for e in escaneamentos:
        escaneamentos_por_conteiner[e.numeroConteiner].append(e)
    for k in escaneamentos_por_conteiner:
        escaneamentos_por_conteiner[k].sort(key=lambda x: x.dataHoraOcorrencia or datetime.min)

    acessos_por_conteiner = defaultdict(list)
    for a in acessos:
        acessos_por_conteiner[a.numeroConteiner].append(a)
    for k in acessos_por_conteiner:
        acessos_por_conteiner[k].sort(key=lambda x: x.dataHoraOcorrencia or datetime.min)

    sem_saida_sem_escaneamento = []
    com_saida_sem_escaneamento = []
    escaneados_apos_24h = []

    total_desembarque_por_recinto = defaultdict(int)
    total_sem_esc_por_recinto = defaultdict(int)
    total_entrada_por_recinto = defaultdict(int)
    total_sem_esc_entrada_por_recinto = defaultdict(int)

    for idx, d in enumerate(desembarques, 1):
        if idx % 100 == 0:
            logger.info("Processando desembarque %s/%s", idx, len(desembarques))

        total_desembarque_por_recinto[d.codigoRecinto] += 1
        dt_des = d.dataHoraOcorrencia

        if not d.numeroConteiner or not dt_des:
            sem_saida_sem_escaneamento.append({
                "codigoRecinto": d.codigoRecinto,
                "numeroConteiner": d.numeroConteiner,
                "datadesembarque": dt_des,
            })
            total_sem_esc_por_recinto[d.codigoRecinto] += 1
            continue

        escaneou_24h = False
        for e in escaneamentos_por_conteiner.get(d.numeroConteiner, []):
            if e.codigoRecinto == d.codigoRecinto and e.dataHoraOcorrencia is not None:
                delta = e.dataHoraOcorrencia - dt_des
                if timedelta(0) <= delta <= timedelta(hours=24):
                    escaneou_24h = True
                    break

        if escaneou_24h:
            continue

        saidas = [
            a for a in acessos_por_conteiner.get(d.numeroConteiner, [])
            if a.codigoRecinto == d.codigoRecinto
               and a.direcao == 'S'
               and a.dataHoraOcorrencia is not None
               and timedelta(0) <= (a.dataHoraOcorrencia - dt_des) <= timedelta(hours=48)
        ]

        logger.info(
            "Desembarque contêiner=%s recinto=%s saidas_validas=%s",
            d.numeroConteiner, d.codigoRecinto, len(saidas)
        )

        if not saidas:
            sem_saida_sem_escaneamento.append({
                "codigoRecinto": d.codigoRecinto,
                "numeroConteiner": d.numeroConteiner,
                "datadesembarque": dt_des,
            })
            total_sem_esc_por_recinto[d.codigoRecinto] += 1
            escaneados_apos_24h.append({
                "codigoRecinto": d.codigoRecinto,
                "numeroConteiner": d.numeroConteiner,
                "datadesembarque": dt_des,
            })
            continue

        cadeia_ok = False
        entrada_encontrada = None

        for s in saidas:
            logger.info(
                "Pesquisando entradas no recinto %s para contêiner=%s",
                d.codigoRecinto, d.numeroConteiner
            )
            entradas = [
                a for a in acessos_por_conteiner.get(d.numeroConteiner, [])
                if a.direcao == 'E'
                   and a.codigoRecinto != d.codigoRecinto
                   and a.dataHoraOcorrencia is not None
                   and timedelta(0) <= (a.dataHoraOcorrencia - s.dataHoraOcorrencia) <= timedelta(hours=6)
            ]
            logger.info("%s entradas encontradas", len(entradas))

            for ent in entradas:
                scans = [
                    e for e in escaneamentos_por_conteiner.get(d.numeroConteiner, [])
                    if e.codigoRecinto == ent.codigoRecinto
                       and e.dataHoraOcorrencia is not None
                       and s.dataHoraOcorrencia <= e.dataHoraOcorrencia <= ent.dataHoraOcorrencia + timedelta(hours=1)
                ]
                if scans:
                    cadeia_ok = True
                    entrada_encontrada = ent
                    logger.info(
                        "Cadeia valida encontrada contêiner=%s saida=%s entrada=%s scans=%s",
                        d.numeroConteiner,
                        s.dataHoraOcorrencia,
                        ent.dataHoraOcorrencia,
                        len(scans),
                    )
                    break
            if cadeia_ok:
                break

        if cadeia_ok:
            continue

        logger.info(
            "Sem escaneamento valido contêiner=%s recinto=%s",
            d.numeroConteiner, d.codigoRecinto
        )
        com_saida_sem_escaneamento.append({
            "codigoRecinto": d.codigoRecinto,
            "codigoRecintoEntrada": entrada_encontrada.codigoRecinto if entrada_encontrada else None,
            "numeroConteiner": d.numeroConteiner,
            "datadesembarque": dt_des,
            "dataentrada": entrada_encontrada.dataHoraOcorrencia if entrada_encontrada else None,
        })
        total_sem_esc_por_recinto[d.codigoRecinto] += 1

    por_recinto = []
    todos_recintos = sorted(set(total_desembarque_por_recinto) | set(total_sem_esc_por_recinto))
    for r in todos_recintos:
        por_recinto.append({
            "codigoRecinto": r,
            "totalDesembarque": total_desembarque_por_recinto.get(r, 0),
            "totalSemEscaneamento": total_sem_esc_por_recinto.get(r, 0),
            "totalEntrada": total_entrada_por_recinto.get(r, 0),
            "totalSemEscaneamentoEntrada": total_sem_esc_entrada_por_recinto.get(r, 0),
        })

    resultado = {
        "sem_saida_sem_escaneamento": sem_saida_sem_escaneamento,
        "com_saida_sem_escaneamento": com_saida_sem_escaneamento,
        "escaneados_apos_24h": escaneados_apos_24h,
        "info": {
            "parametros": {
                "recintos": recintos,
                "intervaloPesquisa": {
                    "data_inicio": data_inicio,
                    "data_fim": data_fim,
                },
            },
            "totais": {
                "por_recinto": por_recinto,
                "geral": {
                    "totalDesembarque": len(desembarques),
                    "totalSemEscaneamento": len(sem_saida_sem_escaneamento) + len(com_saida_sem_escaneamento),
                },
            },
        },
    }

    logger.info(
        "Validação concluída - total_desembarques=%s total_sem_escaneamento=%s",
        len(desembarques),
        len(sem_saida_sem_escaneamento) + len(com_saida_sem_escaneamento),
    )

    return _json_safe(resultado)


if __name__ == '__main__':
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define o fim às 00h de hoje menos 2 dias e início 2 dias antes
    fim = datetime.combine(datetime.now(), time.min) - timedelta(days=2)
    inicio = fim - timedelta(days=2)
    print(valida_regra_escaneamento_json(session, inicio, fim, recintos=['8931356']))
