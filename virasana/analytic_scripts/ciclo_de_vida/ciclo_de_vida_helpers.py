import sys
from pprint import pprint

sys.path.append('.')
sys.path.append('../ajna_docs/commons')
sys.path.append('../bhadrasana2')
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import logger
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item, Manifesto
from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva

from datetime import datetime, timedelta, timezone
from typing import Optional, List

TOLERANCIA_PREVISAO_ATRACACAO = 5  # 5 dias
TOLERANCIA_DATA_DESEMBARQUE = 1  # 1 dia
HORAS_PERMANENCIA_MOLHADO = 72  # 72 horas
HORAS_TRANSIT_TIME = 6  # 6 horas
HORAS_ESCANEAMENTO_SECO = 6  # 2 horas
HORAS_IMAGEM_ANTES = 4
HORAS_IMAGEM_DEPOIS = 4
DIAS_BUSCA_SAIDA_SECO = 30


def _parse_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in (
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d',
    ):
        try:
            return datetime.strptime(str(value)[:19], fmt)
        except ValueError:
            pass
    raise ValueError(f'Formato de data inválido: {value}')


def busca_ce_mercante_conteiner_desembarque(session, codigoConteiner, data_desembarque) -> Optional[Conhecimento]:
    data_desembarque = _parse_dt(data_desembarque)
    inicio = data_desembarque - timedelta(days=TOLERANCIA_PREVISAO_ATRACACAO)
    fim = data_desembarque + timedelta(days=TOLERANCIA_PREVISAO_ATRACACAO)
    q = (
        session.query(Conhecimento)
        .join(Item, Item.numeroCEmercante == Conhecimento.numeroCEmercante)
        .join(Manifesto, Manifesto.numero == Conhecimento.manifestoCE)
        .join(EscalaManifesto, EscalaManifesto.manifesto == Manifesto.numero)
        .join(Escala, Escala.numeroDaEscala == EscalaManifesto.escala)
        .filter(
            Item.codigoConteiner == codigoConteiner,
            Escala.dataPrevistaAtracacao.isnot(None),
            Escala.dataPrevistaAtracacao >= inicio.strftime('%Y-%m-%d'),
            Escala.dataPrevistaAtracacao <= fim.strftime('%Y-%m-%d'),
        )
        .order_by(Conhecimento.dataEmissao.asc())
    )
    return q.first()


def busca_desembarque_conteiner(session, numeroConteiner, data_desembarque) -> Optional[EmbarqueDesembarque]:
    inicio = data_desembarque - timedelta(days=TOLERANCIA_DATA_DESEMBARQUE)
    fim = data_desembarque + timedelta(days=1)
    return session.query(EmbarqueDesembarque).filter(
        EmbarqueDesembarque.numeroConteiner == numeroConteiner,
        EmbarqueDesembarque.dataHoraOcorrencia >= data_desembarque,
        EmbarqueDesembarque.dataHoraOcorrencia <= data_desembarque,
    ).first()


def busca_acesso_veiculo_desembarque_conteiner(session, codigoRecinto: str, numeroConteiner: str,
                                               data_desembarque: datetime) -> Optional[AcessoVeiculo]:
    inicio = data_desembarque
    fim = inicio + timedelta(hours=HORAS_PERMANENCIA_MOLHADO)
    return (
        session.query(AcessoVeiculo)
        .filter(
            AcessoVeiculo.codigoRecinto == codigoRecinto,
            AcessoVeiculo.numeroConteiner == numeroConteiner,
            AcessoVeiculo.direcao == 'S',
            AcessoVeiculo.dataHoraOcorrencia >= inicio,
            AcessoVeiculo.dataHoraOcorrencia < fim,
        )
        .order_by(AcessoVeiculo.dataHoraOcorrencia.asc())
        .first()
    )


def busca_escaneamentos_desembarque_conteiner(session, codigoRecinto: str,
                                              numeroConteiner: str,
                                              data_desembarque: datetime,
                                              data_saida: datetime) -> List[InspecaoNaoInvasiva]:
    return (
        session.query(InspecaoNaoInvasiva)
        .filter(
            InspecaoNaoInvasiva.codigoRecinto == codigoRecinto,
            InspecaoNaoInvasiva.numeroConteiner == numeroConteiner,
            InspecaoNaoInvasiva.dataHoraOcorrencia >= data_desembarque,
            InspecaoNaoInvasiva.dataHoraOcorrencia < data_saida,
        )
        .order_by(InspecaoNaoInvasiva.dataHoraOcorrencia.asc())
        .all()
    )


def busca_escaneamentos_seco(session, codigoRecinto: str, numeroConteiner: str,
                             data_entrada_seco: datetime) -> List[InspecaoNaoInvasiva]:
    inicio = data_entrada_seco
    fim = inicio + timedelta(days=HORAS_ESCANEAMENTO_SECO)
    return (
        session.query(InspecaoNaoInvasiva)
        .filter(
            InspecaoNaoInvasiva.codigoRecinto == codigoRecinto,
            InspecaoNaoInvasiva.numeroConteiner == numeroConteiner,
            InspecaoNaoInvasiva.dataHoraOcorrencia >= inicio,
            InspecaoNaoInvasiva.dataHoraOcorrencia < fim,
        )
        .order_by(InspecaoNaoInvasiva.dataHoraOcorrencia.asc())
        .all()
    )


def busca_mongo(db, numeroConteiner, data_escaneamento: datetime):
    # Procura imagens de escaneamentos do contêiner pela data do escaneamento.
    # Procura até X horas antes ou depois da data do evento
    # TODO: Mongo está em UTC e MySQL não... ver a conversão correta!
    data_escaneamento = data_escaneamento.astimezone(
        timezone.utc) if data_escaneamento.tzinfo else data_escaneamento.replace(tzinfo=timezone.utc)
    dataminima = data_escaneamento - timedelta(hours=HORAS_IMAGEM_ANTES)
    datamaxima = data_escaneamento + timedelta(hours=HORAS_IMAGEM_DEPOIS)
    filtro = {
        'metadata.numeroinformado': numeroConteiner,
        'metadata.dataescaneamento': {
            '$gte': dataminima,
            '$lte': datamaxima
        }
    }
    grid_data = db['fs.files'].find_one(filtro)
    if grid_data:
        return grid_data.get('_id')
    logger.info(f'Nenhum resultado foi encontrado para os parametros: {numeroConteiner}, {dataminima}, {datamaxima}')
    pprint(filtro)
    return None


def busca_entrada_seco(session, codigoRecinto: str, numeroConteiner: str,
                       data_saida: datetime) -> Optional[AcessoVeiculo]:
    inicio = data_saida
    fim = inicio + timedelta(hours=HORAS_TRANSIT_TIME)
    # logger.info(f'busca_entrada_seco {codigoRecinto}, {numeroConteiner}, {inicio}, {fim}')
    return (
        session.query(AcessoVeiculo)
        .filter(
            AcessoVeiculo.codigoRecinto != codigoRecinto,
            AcessoVeiculo.numeroConteiner == numeroConteiner,
            AcessoVeiculo.direcao == 'E',
            AcessoVeiculo.dataHoraOcorrencia >= inicio,
            AcessoVeiculo.dataHoraOcorrencia < fim,
        )
        .order_by(AcessoVeiculo.dataHoraOcorrencia.asc())
        .first()
    )


def busca_saida_seco(session, codigoRecinto: str, numeroConteiner: str,
                     data_entrada_seco: datetime) -> Optional[AcessoVeiculo]:
    inicio = data_entrada_seco
    fim = inicio + timedelta(days=DIAS_BUSCA_SAIDA_SECO)
    return (
        session.query(AcessoVeiculo)
        .filter(
            AcessoVeiculo.codigoRecinto == codigoRecinto,
            AcessoVeiculo.numeroConteiner == numeroConteiner,
            AcessoVeiculo.direcao == 'S',
            AcessoVeiculo.dataHoraOcorrencia >= inicio,
            AcessoVeiculo.dataHoraOcorrencia < fim,
        )
        .order_by(AcessoVeiculo.dataHoraOcorrencia.asc())
        .first()
    )
