from datetime import datetime, timedelta
from typing import Optional

from bhadrasana.models.apirecintos import AcessoVeiculo, PesagemVeiculo, InspecaoNaoInvasiva
from bhadrasana.models.apirecintos_risco import Motorista
from bhadrasana.models.ovr import Recinto
from bhadrasana.models.virasana_manager import get_conhecimento
from integracao.mercante.mercantealchemy import Conhecimento
from pymongo.database import Database


def get_pesagem_entrada(session, entrada: AcessoVeiculo) -> PesagemVeiculo:
    q = session.query(PesagemVeiculo).filter(PesagemVeiculo.placa == entrada.placa)
    q = q.filter(PesagemVeiculo.codigoRecinto == entrada.codigoRecinto)
    q = q.filter(PesagemVeiculo.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, entrada.dataHoraOcorrencia + timedelta(hours=12)
    )).order_by(PesagemVeiculo.dataHoraOcorrencia)
    return q.first()


def get_inspecaonaoinvasiva_entrada(session, entrada: AcessoVeiculo) -> Optional[InspecaoNaoInvasiva]:
    if not entrada.numeroConteiner:
        return None
    q = session.query(InspecaoNaoInvasiva).filter(InspecaoNaoInvasiva.numeroConteiner == entrada.numeroConteiner)
    q = q.filter(InspecaoNaoInvasiva.codigoRecinto == entrada.codigoRecinto)
    q = q.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, entrada.dataHoraOcorrencia + timedelta(hours=12)
    )).order_by(InspecaoNaoInvasiva.dataHoraOcorrencia)
    return q.first()


def get_saida_entrada(session, entrada: AcessoVeiculo) -> AcessoVeiculo:
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    q = q.filter(AcessoVeiculo.codigoRecinto == entrada.codigoRecinto)
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


def get_recinto_nome(session, entrada: AcessoVeiculo) -> str:
    recinto_nome = entrada.codigoRecinto
    recinto = session.query(Recinto).filter(Recinto.cod_siscomex == entrada.codigoRecinto).one_or_none()
    if recinto:
        recinto_nome = recinto_nome + ' - ' + recinto.nome
    return recinto_nome


def aplica_filtros(q, placa, numeroConteiner, cpfMotorista, codigoRecinto):
    if placa:
        q = q.filter(AcessoVeiculo.placa == placa)
    if numeroConteiner:
        q = q.filter(AcessoVeiculo.numeroConteiner == numeroConteiner)
    if cpfMotorista:
        q = q.filter(AcessoVeiculo.cpfMotorista == cpfMotorista)
    if codigoRecinto:
        q = q.filter(AcessoVeiculo.codigoRecinto == codigoRecinto)
    return q




def search_conhecimento(session, entrada)-> Optional[Conhecimento]:
    if entrada.numeroConhecimento:
        return get_conhecimento(session, entrada.numeroConhecimento)
    return None


def get_missao(session, entrada: AcessoVeiculo, conhecimento:Conhecimento):
    if entrada.vazioSemirreboque:
        return 'Veículo Vazio'
    if entrada.vazioConteiner:
        return 'Contêiner Vazio'
    if conhecimento is not None:
        if conhecimento.tipoTrafego == 5:
            return 'Importação'
        elif conhecimento.tipoTrafego == 7:
            return 'Exportação'
        elif conhecimento.tipoTrafego == 3:
            return 'Cabotagem'
        else:
            return 'Passagem'
    return 'Não foi possível determinar'


def get_eventos_entradas(session, mongodb, lista_entradas):
    lista_eventos = []
    for entrada in lista_entradas:
        dataentradaouescaneamento = entrada.dataHoraOcorrencia
        datasaida = dataentradaouescaneamento
        numeroConteiner = entrada.numeroConteiner
        dict_eventos = {}
        # Recinto
        dict_eventos['recinto'] = get_recinto_nome(session, entrada)
        conhecimento = search_conhecimento(session, entrada)
        dict_eventos['conhecimento'] = conhecimento
        dict_eventos['missao'] = get_missao(session, entrada, conhecimento)
        dict_eventos['motorista'] = session.query(Motorista).filter(Motorista.cpf == entrada.cpfMotorista).one_or_none()
        # Entrada
        dict_eventos['entrada'] = entrada
        # Pesagem
        dict_eventos['pesagem'] = get_pesagem_entrada(session, entrada)
        # InspecaoNaoInvasiva
        inspecaonaoinvasiva = get_inspecaonaoinvasiva_entrada(session, entrada)
        dict_eventos['inspecaonaoinvasiva'] = inspecaonaoinvasiva
        if inspecaonaoinvasiva:
            dataentradaouescaneamento = inspecaonaoinvasiva.dataHoraOcorrencia
            numeroConteiner = inspecaonaoinvasiva.numeroConteiner
        # Saída
        saida = get_saida_entrada(session, entrada)
        if saida:
            datasaida = saida.dataHoraOcorrencia
            dict_eventos['permanencia'] = saida.dataHoraOcorrencia - entrada.dataHoraOcorrencia
        dict_eventos['saida'] = saida
        # Imagem do Ajna
        dict_eventos['id_imagem'] = get_id_imagem(mongodb, numeroConteiner,
                                                  dataentradaouescaneamento, datasaida)

        lista_eventos.append(dict_eventos)
    return lista_eventos


def aplica_risco_motorista(q):
    return q.join(Motorista, Motorista.cpf == AcessoVeiculo.cpfMotorista)


def get_eventos(mongodb: Database, session,
                datainicio: datetime, datafim: datetime,
                placa='', numeroConteiner='', cpfMotorista='',
                motoristas_de_risco=False, codigoRecinto='',
                tempo_permanencia=0):
    print(f'motoristas_de_risco: {motoristas_de_risco} - {type(motoristas_de_risco)}')
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    print(datainicio, datafim)
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = aplica_filtros(q, placa, numeroConteiner, cpfMotorista, codigoRecinto)
    if motoristas_de_risco:
        q = aplica_risco_motorista(q)
    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()
    lista_eventos = get_eventos_entradas(session, mongodb, lista_entradas)
    if tempo_permanencia > 0:  # Filtrar por tempo de permanencia:
        lista_eventos = [evento for evento in lista_eventos
                         if evento.get('permanencia') and
                         evento['permanencia'].seconds >  (tempo_permanencia * 60)]
    return lista_eventos
