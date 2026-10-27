import sys
import json
from collections import defaultdict
from datetime import datetime, date, timedelta, time
from pathlib import Path

sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from ajna_commons.flask.log import logger

from bhadrasana.models.apirecintos import InspecaoNaoInvasiva, EmbarqueDesembarque, AcessoVeiculo
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item


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


def filtra_desembarques_santos(session, desembarques, porto_desembarque='BRSSZ'):
    contes = sorted({d.numeroConteiner for d in desembarques if d.numeroConteiner and d.dataHoraOcorrencia})
    if not contes:
        return []

    data_minima = min(d.dataHoraOcorrencia for d in desembarques if d.numeroConteiner and d.dataHoraOcorrencia)
    dt_inicio_geral = data_minima - timedelta(days=50)

    q = (
        session.query(Item.codigoConteiner)
        .join(Conhecimento, Conhecimento.numeroCEmercante == Item.numeroCEmercante)
        .filter(Item.codigoConteiner.in_(contes))
        .filter(Conhecimento.portoDestFinal == porto_desembarque)
        .filter(Conhecimento.dataEmissao.isnot(None))
        .filter(Conhecimento.dataEmissao >= dt_inicio_geral.strftime('%Y-%m-%d'))
    )
    rows = q.all()
    return sorted({r[0] for r in rows if r[0]})


def valida_regra_escaneamento_json(session, data_inicio, data_fim, recintos)-> dict:
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

    conteineres_santos = filtra_desembarques_santos(session, desembarques)
    conteineres_santos_set = set(conteineres_santos)
    logger.info("%s desembarques encontrados com porto destino final BRSSZ", len(conteineres_santos_set))

    if not conteineres_santos_set:
        resultado = {
            "desembarques_baldeacao": [],
            "sem_saida_sem_escaneamento": [],
            "com_saida_sem_escaneamento": [],
            "com_entrada_sem_escaneamento": [],
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
                        "totalBaldeacao": len(desembarques),
                        "totalComEntradaSemEscaneamento": 0,
                    },
                },
            },
        }
        return _json_safe(resultado)

    logger.info("Pesquisando inspecoes para %s contêineres elegíveis", len(conteineres_santos_set))
    escaneamentos = (
        session.query(InspecaoNaoInvasiva)
        .filter(InspecaoNaoInvasiva.numeroConteiner.in_(conteineres_santos_set))
        .filter(InspecaoNaoInvasiva.dataHoraOcorrencia >= data_inicio)
        .filter(InspecaoNaoInvasiva.dataHoraOcorrencia <= data_fim + timedelta(hours=72))
        .all()
    )
    logger.info("%s inspecoes encontradas", len(escaneamentos))

    logger.info("Pesquisando acessos para %s contêineres elegíveis", len(conteineres_santos_set))
    acessos = (
        session.query(AcessoVeiculo)
        .filter(AcessoVeiculo.numeroConteiner.in_(conteineres_santos_set))
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
    com_entrada_sem_escaneamento = []
    escaneados_apos_24h = []
    desembarques_baldeacao = []

    total_desembarque_por_recinto = defaultdict(int)
    total_sem_esc_por_recinto = defaultdict(int)
    total_entrada_por_recinto = defaultdict(int)
    total_sem_esc_entrada_por_recinto = defaultdict(int)

    for d in desembarques:
        if d.numeroConteiner not in conteineres_santos_set:
            desembarques_baldeacao.append({
                "codigoRecinto": d.codigoRecinto,
                "numeroConteiner": d.numeroConteiner,
                "datadesembarque": d.dataHoraOcorrencia,
            })
            continue

        total_desembarque_por_recinto[d.codigoRecinto] += 1
        dt_des = d.dataHoraOcorrencia

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
            entradas = [
                a for a in acessos_por_conteiner.get(d.numeroConteiner, [])
                if a.direcao == 'E'
                and a.codigoRecinto != d.codigoRecinto
                and a.dataHoraOcorrencia is not None
                and timedelta(0) <= (a.dataHoraOcorrencia - s.dataHoraOcorrencia) <= timedelta(hours=6)
            ]

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
                    total_entrada_por_recinto[ent.codigoRecinto] += 1
                    break
            if cadeia_ok:
                break

        if cadeia_ok:
            continue

        if entrada_encontrada:
            com_entrada_sem_escaneamento.append({
                "codigoRecinto": d.codigoRecinto,
                "codigoRecintoEntrada": entrada_encontrada.codigoRecinto,
                "numeroConteiner": d.numeroConteiner,
                "datadesembarque": dt_des,
                "dataentrada": entrada_encontrada.dataHoraOcorrencia,
            })
            total_sem_esc_entrada_por_recinto[entrada_encontrada.codigoRecinto] += 1
        else:
            com_saida_sem_escaneamento.append({
                "codigoRecinto": d.codigoRecinto,
                "codigoRecintoEntrada": None,
                "numeroConteiner": d.numeroConteiner,
                "datadesembarque": dt_des,
                "dataentrada": None,
            })
            total_sem_esc_por_recinto[d.codigoRecinto] += 1

    por_recinto = []
    todos_recintos = sorted(set(total_desembarque_por_recinto) | set(total_sem_esc_por_recinto) | set(total_entrada_por_recinto) | set(total_sem_esc_entrada_por_recinto))
    for r in todos_recintos:
        por_recinto.append({
            "codigoRecinto": r,
            "totalDesembarque": total_desembarque_por_recinto.get(r, 0),
            "totalSemEscaneamento": total_sem_esc_por_recinto.get(r, 0),
            "totalEntrada": total_entrada_por_recinto.get(r, 0),
            "totalSemEscaneamentoEntrada": total_sem_esc_entrada_por_recinto.get(r, 0),
        })

    resultado = {
        "desembarques_baldeacao": desembarques_baldeacao,
        "sem_saida_sem_escaneamento": sem_saida_sem_escaneamento,
        "com_saida_sem_escaneamento": com_saida_sem_escaneamento,
        "com_entrada_sem_escaneamento": com_entrada_sem_escaneamento,
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
                    "totalDesembarque": len(desembarques) - len(desembarques_baldeacao),
                    "totalSemEscaneamento": len(sem_saida_sem_escaneamento) + len(com_saida_sem_escaneamento) + len(com_entrada_sem_escaneamento),
                    "totalBaldeacao": len(desembarques_baldeacao),
                    "totalComEntradaSemEscaneamento": len(com_entrada_sem_escaneamento),
                },
            },
        },
    }

    logger.info(
        "Validação concluída - total_desembarques=%s total_sem_escaneamento=%s total_baldeacao=%s total_com_entrada_sem_escaneamento=%s",
        len(desembarques) - len(desembarques_baldeacao),
        len(sem_saida_sem_escaneamento) + len(com_saida_sem_escaneamento) + len(com_entrada_sem_escaneamento),
        len(desembarques_baldeacao),
        len(com_entrada_sem_escaneamento),
    )

    return resultado


if __name__ == '__main__':
    from ajna_commons.flask.conf import SQL_URI
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    fim = datetime.combine(datetime.now(), time.min) - timedelta(days=2)
    inicio = fim - timedelta(hours=1)

    resultado = _json_safe(valida_regra_escaneamento_json(session, inicio, fim, recintos=['8931356']))

    Path("output").mkdir(exist_ok=True)
    arquivo = Path("output") / f"validacao_escaneamento_{inicio:%Y%m%d}_{fim:%Y%m%d}.json"
    with arquivo.open("w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2, default=str)

    print(arquivo)