import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

sys.path.append('../bhadrasana2')
sys.path.append('.')
sys.path.append('virasana')

from bhadrasana.models.apirecintos import AcessoVeiculo, PesagemVeiculo, InspecaoNaoInvasiva
from bhadrasana.models.apirecintos_risco import Motorista
from bhadrasana.models.ovr import Recinto
from bhadrasana.models.virasana_manager import get_conhecimento
from integracao.mercante.mercantealchemy import Conhecimento
from pymongo.database import Database


def get_pesagem_entrada(session, entrada: AcessoVeiculo, datasaida) -> PesagemVeiculo:
    q = session.query(PesagemVeiculo).filter(PesagemVeiculo.placa == entrada.placa)
    q = q.filter(PesagemVeiculo.codigoRecinto == entrada.codigoRecinto)
    q = q.filter(PesagemVeiculo.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, datasaida
    )).order_by(PesagemVeiculo.dataHoraOcorrencia)
    return q.first()


def get_inspecaonaoinvasiva_entrada(session, entrada: AcessoVeiculo, datasaida) -> Optional[InspecaoNaoInvasiva]:
    if not entrada.numeroConteiner:
        return None
    q = session.query(InspecaoNaoInvasiva).filter(InspecaoNaoInvasiva.numeroConteiner == entrada.numeroConteiner)
    q = q.filter(InspecaoNaoInvasiva.codigoRecinto == entrada.codigoRecinto)
    q = q.filter(InspecaoNaoInvasiva.dataHoraOcorrencia.between(
        entrada.dataHoraOcorrencia, datasaida
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
    datamaxima = datasaida
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


def search_conhecimento(session, entrada) -> Optional[Conhecimento]:
    if entrada.numeroConhecimento:
        return get_conhecimento(session, entrada.numeroConhecimento)
    return None


class Missao():
    def __init__(self):
        self.missoes_list = ['Veículo Vazio', 'Contêiner Vazio', 'Importação',
                             'Exportação', 'Cabotagem', 'Passagem', 'Interior',
                             'Não foi possível determinar']
        self.missoes_select = [(ind, descricao) for ind, descricao in enumerate(self.missoes_list)]

    def get_tipos_missao(self):
        return sorted(self.missoes_select, key=lambda x: x[1])

    def get_descricao_missao(self, ind):
        try:
            return self.missoes_list[ind]
        except:
            return None

    def get_missao(self, session, entrada: AcessoVeiculo, conhecimento: Conhecimento):
        if (entrada.tipoDeclaracao and entrada.tipoDeclaracao == 'DUE') or entrada.listaNfe:  # Exportação
            return self.get_descricao_missao(3)
        if conhecimento is not None:
            if conhecimento.tipoTrafego == '5':  # lci
                return self.get_descricao_missao(2)
            elif conhecimento.tipoTrafego == '7':  # lce
                return self.get_descricao_missao(3)
            elif conhecimento.tipoTrafego == '3':  # cabotagem
                return self.get_descricao_missao(4)
            elif conhecimento.tipoTrafego == '9':  # passagem
                return self.get_descricao_missao(5)
            elif conhecimento.tipoTrafego == '1':  # interior
                return self.get_descricao_missao(6)
        if entrada.vazioConteiner:  # Veículo com contêiner vazio
            return self.get_descricao_missao(1)
        if entrada.vazioSemirreboque:  # Veículo descarregado/vazio
            return self.get_descricao_missao(0)
        return self.get_descricao_missao(7)  # Sem informações


def get_eventos_entradas(session, mongodb, lista_entradas, filtra_missao=None, motoristas_risco=None):
    lista_eventos = []
    count_missao = defaultdict(int)
    for entrada in lista_entradas:
        dataentradaouescaneamento = entrada.dataHoraOcorrencia
        datasaida = dataentradaouescaneamento + timedelta(hours=12)
        numeroConteiner = entrada.numeroConteiner
        dict_eventos = {}
        # Missão
        conhecimento = search_conhecimento(session, entrada)
        dict_eventos['conhecimento'] = conhecimento
        missao = Missao().get_missao(session, entrada, conhecimento)
        if filtra_missao:  # Pular linha se não for da missão desejada
            if missao != filtra_missao:
                continue
        dict_eventos['missao'] = missao
        count_missao[missao] += 1
        # Recinto
        dict_eventos['recinto'] = get_recinto_nome(session, entrada)
        # Motorista
        motorista: Motorista = session.query(Motorista).filter(Motorista.cpf == entrada.cpfMotorista).one_or_none()
        if motoristas_risco and not (motoristas_risco in ['0', '99']):  # 0 = Ignorar, 99 = TODOS
            if motorista.classificacao != motoristas_risco:
                continue
        dict_eventos['motorista'] = motorista
        # Entrada
        dict_eventos['entrada'] = entrada
        # Saída
        saida = get_saida_entrada(session, entrada)
        if saida:
            datasaida = saida.dataHoraOcorrencia
            dict_eventos['permanencia'] = saida.dataHoraOcorrencia - entrada.dataHoraOcorrencia
        dict_eventos['saida'] = saida
        # Pesagem
        dict_eventos['pesagem'] = get_pesagem_entrada(session, entrada, datasaida)
        # InspecaoNaoInvasiva
        inspecaonaoinvasiva = get_inspecaonaoinvasiva_entrada(session, entrada, datasaida)
        dict_eventos['inspecaonaoinvasiva'] = inspecaonaoinvasiva
        if inspecaonaoinvasiva:
            dataentradaouescaneamento = inspecaonaoinvasiva.dataHoraOcorrencia
            numeroConteiner = inspecaonaoinvasiva.numeroConteiner
        # Imagem do Ajna
        dict_eventos['id_imagem'] = get_id_imagem(mongodb, numeroConteiner,
                                                  dataentradaouescaneamento, datasaida)

        lista_eventos.append(dict_eventos)
    return lista_eventos, count_missao


def aplica_risco_motorista(q):
    return q.join(Motorista, Motorista.cpf == AcessoVeiculo.cpfMotorista)


def get_eventos(mongodb: Database, session,
                datainicio: datetime, datafim: datetime,
                placa='', numeroConteiner='', cpfMotorista='',
                motoristas_de_risco_select='0', codigoRecinto='',
                tempo_permanencia=0, missao=None):
    q = session.query(AcessoVeiculo).filter(AcessoVeiculo.operacao == 'C')
    print(datainicio, datafim)
    q = q.filter(AcessoVeiculo.dataHoraOcorrencia.between(datainicio, datafim))
    q = q.filter(AcessoVeiculo.direcao == 'E')
    q = aplica_filtros(q, placa, numeroConteiner, cpfMotorista, codigoRecinto)
    if motoristas_de_risco_select != '0':
        q = aplica_risco_motorista(q)
    lista_entradas = q.order_by(AcessoVeiculo.dataHoraOcorrencia).all()
    lista_eventos, count_missao = get_eventos_entradas(session, mongodb, lista_entradas,
                                                       missao, motoristas_de_risco_select)
    if tempo_permanencia > 0:  # Filtrar por tempo de permanencia:
        lista_eventos = [evento for evento in lista_eventos
                         if evento.get('permanencia') and
                         evento['permanencia'].seconds > (tempo_permanencia * 60)]
    return lista_eventos, count_missao


def monta_planilha_apirecintos(lista_eventos):
    titulos = ['recinto', 'missao',
               'motorista.cpf', 'motorista.nome', 'motorista.risco',
               'entrada.dataHoraOcorrencia',
               'entrada.placa', 'entrada.numeroConteiner', 'entrada.cnpjTransportador',
               'entrada.numeroDeclaracao', 'entrada.numeroConhecimento', 'entrada.listaNfe',
               'pesagem.dataHoraOcorrencia', 'pesagem.taraSemirreboque',
               'pesagem.pesoBrutoManifesto', 'pesagem.pesoBrutoBalanca',
               'saida.dataHoraOcorrencia',
               'saida.placa', 'saida.numeroConteiner', 'saida.cnpjTransportador',
               'saida.numeroDeclaracao', 'saida.numeroConhecimento', 'saida.listaNfe']
    linhas = []
    for evento in lista_eventos:
        motorista: Motorista = evento['motorista']
        entrada: AcessoVeiculo = evento['entrada']
        pesagem: PesagemVeiculo = evento.get('pesagem', PesagemVeiculo())
        saida: AcessoVeiculo = evento.get('saida', AcessoVeiculo())
        if pesagem is None:
            pesagem = PesagemVeiculo()
        if saida is None:
            saida = AcessoVeiculo()
        if motorista is None:
            motorista_cpf = ''
            motorista_nome = ''
            motorista_risco = ''
        else:
            motorista_cpf = motorista.cpf
            motorista_nome = motorista.nome
            motorista_risco = motorista.get_risco()
        linha = [evento['recinto'], evento['missao'],
                 motorista_cpf, motorista_nome, motorista_risco,
                 entrada.dataHoraOcorrencia,
                 entrada.placa, entrada.numeroConteiner, entrada.cnpjTransportador,
                 entrada.numeroDeclaracao, entrada.numeroConhecimento, entrada.listaNfe,
                 pesagem.dataHoraOcorrencia, pesagem.taraSemirreboque,
                 pesagem.pesoBrutoManifesto, pesagem.pesoBrutoBalanca,
                 saida.dataHoraOcorrencia,
                 saida.placa, saida.numeroConteiner, saida.cnpjTransportador,
                 saida.numeroDeclaracao, saida.numeroConhecimento, saida.listaNfe]
        linhas.append(linha)
    if len(linhas) > 0:
        df = pd.DataFrame(linhas)
        df.columns = titulos
        return df
    return None


if __name__ == '__main__':
    missao = Missao()
    print(missao.missoes_list)
    print(missao.missoes_select)
    print(missao.get_tipos_missao())
    print(missao.get_descricao_missao(0))
    print(missao.get_descricao_missao(6))
    print(missao.get_descricao_missao(99))
