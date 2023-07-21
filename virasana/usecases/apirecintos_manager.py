from datetime import datetime, timedelta

from bhadrasana.models.apirecintos import AcessoVeiculo, PesagemVeiculo, InspecaoNaoInvasiva
from pymongo.database import Database


def get_pesagem_entrada(session, entrada: AcessoVeiculo) -> PesagemVeiculo:
    q = session.query(PesagemVeiculo).filter(PesagemVeiculo.placa == entrada.placa)
    q = q.filter(PesagemVeiculo.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, entrada.dataHoraOcorrencia + timedelta(hours=12)
    )).order_by(PesagemVeiculo.dataHoraOcorrencia)
    return q.first()


def get_inspecaonaoinvasiva_entrada(session, entrada: AcessoVeiculo) -> InspecaoNaoInvasiva:
    if not entrada.numeroConteiner:
        return None
    q = session.query(InspecaoNaoInvasiva).filter(InspecaoNaoInvasiva.numeroConteiner == entrada.numeroConteiner)
    q = q.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, entrada.dataHoraOcorrencia + timedelta(hours=12)
    )).order_by(InspecaoNaoInvasiva.dataHoraOcorrencia)
    return q.first()


def get_saida_entrada(session, entrada: AcessoVeiculo) -> AcessoVeiculo:
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.direcao == 'S')
    q = q.filter(AcessoVeiculo.placa == entrada.placa)
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, entrada.dataHoraOcorrencia + timedelta(hours=12)
    )).order_by(AcessoVeiculo.dataHoraOcorrencia)
    return q.first()


def get_id_imagem(mongodb, numeroConteiner, dataentradaouescaneamento, datasaida):
    # Procura escaneamentos do contêiner pela data do escaneamento.
    # Se não tiver esta data, procura até X horas depois da entrada ou X horas antes da saída
    dataminima = dataentradaouescaneamento - timedelta(hours=3)
    datamaxima = datasaida + timedelta(hours=6)
    grid_data = mongodb['fs.files'].find_one(
        {'metadata.numeroinformado': numeroConteiner,
         'metadata.dataescaneamento': {'$gte': dataminima, '$lte': datamaxima}})
    if not grid_data:
        grid_data = mongodb['fs.files'].find_one(
            {'metadata.numeroinformado': numeroConteiner,
             'metadata.dataescaneamento': {'$gte': dataminima, '$lte': datamaxima}})
    if grid_data:
        return grid_data.get('_id')
    return ''


def get_eventos(mongodb: Database, session,
                datainicio: datetime, datafim: datetime,
                placa=''):
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.direcao == 'E')
    if placa:
        q = q.filter(AcessoVeiculo.placa == placa)
    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()
    lista_eventos = []
    for entrada in lista_entradas:
        dataentradaouescaneamento = entrada.dataHoraOcorrencia
        datasaida = dataentradaouescaneamento
        numeroConteiner = entrada.numeroConteiner
        dict_eventos = {}
        dict_eventos['entrada'] = entrada
        dict_eventos['pesagem'] = get_pesagem_entrada(session, entrada)
        inspecaonaoinvasiva = get_inspecaonaoinvasiva_entrada(session, entrada)
        dict_eventos['inspecaonaoinvasiva'] = inspecaonaoinvasiva
        if inspecaonaoinvasiva:
            dataentradaouescaneamento = inspecaonaoinvasiva.dataHoraOcorrencia
            numeroConteiner = inspecaonaoinvasiva.numeroConteiner
        saida = get_saida_entrada(session, entrada)
        if saida:
            datasaida = saida.dataHoraOcorrencia
        dict_eventos['saida'] = saida
        dict_eventos['id_imagem'] = get_id_imagem(mongodb, numeroConteiner,
                                                  dataentradaouescaneamento, datasaida)
        lista_eventos.append(dict_eventos)
    return lista_eventos
