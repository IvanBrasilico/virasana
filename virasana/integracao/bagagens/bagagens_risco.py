import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from bhadrasana.models.laudo import Empresa, get_empresa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, '.')
sys.path.insert(0, '../ajna_docs/commons')
sys.path.append('../bhadrasana2')

from ajna_commons.flask.conf import SQL_URI
from virasana.integracao.bagagens.viajantesalchemy import Viagem, Pessoa, DSI


def pessoa_bagagens_sem_info(con, opcao: str):
    adicoes_sql = {
        'viagem': ' AND c.consignatario NOT IN (SELECT DISTINCT cpf from bagagens_viagens)',
        'empresa': ' AND  SUBSTR(c.consignatario, 1, 8) NOT IN (SELECT DISTINCT cnpj from laudo_empresas)',
        'pessoa': ' AND c.consignatario NOT IN (SELECT DISTINCT cpf from bagagens_pessoas)',
        'dsi': ' AND c.consignatario NOT IN (SELECT DISTINCT consignatario from bagagens_dsi)'
    }
    sql_novos_viajantes = """SELECT DISTINCT c.consignatario as cpf_cnpj FROM itensresumo i
    INNER JOIN conhecimentosresumo c ON i.numeroCEmercante = c.numeroCEmercante
    WHERE NCM LIKE '9797' AND c.tipoTrafego = 5
    AND c.dataEmissao >= '%s'"""
    data_inicial = datetime.strftime(datetime.now() - timedelta(days=90), '%Y-%m-%d')
    sql_novos_viajantes = sql_novos_viajantes % data_inicial
    sql_novos_viajantes += adicoes_sql[opcao.lower()]
    print(sql_novos_viajantes)
    df = pd.read_sql(sql_novos_viajantes, con)
    df.to_csv(f'{opcao}.csv')


def importa_viagens(session):
    if not os.path.exists('viagens_nome.csv'):
        print('Pulando viagens - arquivo não existe')
        return
    df = pd.read_csv('viagens_nome.csv')
    df = df.fillna('')
    for index, row in df.iterrows():
        viagem = Viagem()
        viagem.cpf = str(row['cpf']).zfill(11)
        viagem.data_chegada = row['data_chegada']
        viagem.origem = row['codigo_local_embarque']
        viagem.destino = row['codigo_local_destino']
        viagem.localizador = row['codigo_reserva']
        viagem.voo = row['numero_voo']
        viagem.codigo_vu = row['codigo_vu']
        session.add(viagem)
    session.commit()
    os.remove('viagens_nome.csv')


def importa_cpfs(session):
    if not os.path.exists('cpfs.csv'):
        print('Pulando cpfs - arquivo não existe')
        return
    df = pd.read_csv('cpfs.csv')
    for index, row in df.iterrows():
        cpf = row['cpf']
        cpf = str(int(cpf)).zfill(11)
        pessoa = session.query(Pessoa).filter(Pessoa.cpf == cpf).one_or_none()
        if pessoa is None:
            pessoa = Pessoa()
        pessoa.cpf = cpf
        pessoa.nome = row['nome']
        pessoa.endereco = row['endereco']
        complemento = row['complemento']
        if complemento != 'Não informado':
            pessoa.endereco = pessoa.endereco + ' ' + complemento
        pessoa.cep = row['cep']
        session.add(pessoa)
    session.commit()
    os.remove('cpfs.csv')


def importa_cnpjs(session):
    if not os.path.exists('cnpjs.csv'):
        print('Pulando cnpjs - arquivo não existe')
        return
    df = pd.read_csv('cnpjs.csv')
    for index, row in df.iterrows():
        cnpj = row['cnpj']
        cnpj = str(int(cnpj)).zfill(8)
        try:
            empresa = get_empresa(session, cnpj)
        except ValueError:
            empresa = None
        if empresa is None:
            empresa = Empresa()
        empresa.cnpj = cnpj
        empresa.nome = row['nome']
        session.add(empresa)
    session.commit()
    os.remove('cnpjs.csv')


def importa_dsis(session):
    if not os.path.exists('dsis.csv'):
        print('Pulando dsis - arquivo não existe')
        return
    df = pd.read_csv('dsis.csv')
    for index, row in df.iterrows():
        print(row)
        numero = row['numero']
        try:
            dsi = session.query(DSI).filter(DSI.numero == numero).one_or_none()
        except ValueError:
            dsi = None
        if dsi is None:
            dsi = DSI()
        dsi.numero = numero
        dsi.consignatario = row['consignatario']
        dsi.despachante = row['despachante']
        dsi.descricao = row['descricao']
        dsi.data_registro = row['data_registro']
        dsi.numeroCEmercante = row['nr_conhec_carga'][-15:]
        session.add(dsi)
    session.commit()
    os.remove('dsis.csv')


if __name__ == '__main__':
    engine = create_engine(SQL_URI)
    Session = sessionmaker(bind=engine)
    session = Session()
    opcao = input('Escola (E)xportar ou (I)mportar dados.'
                  'Os arquivos serão criados/procurados no diretório atual.')
    if opcao.lower() == 'i':
        print('Importando viagens...')
        importa_viagens(session)
        print('Importando pessoas físicas...')
        importa_cpfs(session)
        print('Importando pessoas juridicas...')
        importa_cnpjs(session)
        print('Importando DSIs...')
        importa_dsis(session)
    else:
        print('Exportando listas de CFPs de CEs de bagagem sem informação...')
        print('Exportando CPFs/CNPJs sem viagens...')
        pessoa_bagagens_sem_info(engine, 'viagem')
        print('Exportando CNPJs sem nomes...')
        pessoa_bagagens_sem_info(engine, 'empresa')
        print('Exportando CPFs sem nomes...')
        pessoa_bagagens_sem_info(engine, 'pessoa')
        print('Exportando CPFs sem DIRFs...')
        print('Exportando CPFs sem DSIs...')
        pessoa_bagagens_sem_info(engine, 'dsi')
        print('Listas devem ser fornecidas ao notebook que faz as consultas no RD.')
