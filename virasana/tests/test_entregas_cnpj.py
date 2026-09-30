"""Testes da consulta "Entregas por CNPJ" (virasana/entregas): SQL real em SQLite, cenário simulado."""
import io
import os
import unittest
from datetime import date, datetime

import pandas as pd
from flask import Flask
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from virasana.entregas.busca_entregas import ParametrosInvalidos, busca_entregas, planilha_xlsx
from virasana.entregas.views_entregas import bp_entregas

CNPJ = '11222333000181'
INICIO, FIM = date(2026, 5, 1), date(2026, 5, 31)

DDL = [
    'CREATE TABLE conhecimentosresumo (ID INTEGER PRIMARY KEY, numeroCEmercante VARCHAR(15), '
    'consignatario VARCHAR(200), descricao VARCHAR(800), manifestoCE VARCHAR(15), tipoTrafego VARCHAR(10))',
    'CREATE TABLE manifestosresumo (ID INTEGER PRIMARY KEY, numero VARCHAR(15), dataInicioOperacaoDate DATE)',
    'CREATE TABLE itensresumo (ID INTEGER PRIMARY KEY, numeroCEmercante VARCHAR(15), codigoConteiner VARCHAR(11))',
    'CREATE TABLE ncmitemresumo (ID INTEGER PRIMARY KEY, numeroCEMercante CHAR(15), codigoConteiner CHAR(11), '
    'identificacaoNCM VARCHAR(10))',
    'CREATE TABLE apirecintos_acessosveiculo (id INTEGER PRIMARY KEY, numeroConteiner VARCHAR(11), '
    'dataHoraOcorrencia DATETIME, codigoRecinto VARCHAR(7), direcao VARCHAR(1), operacao VARCHAR(1), '
    'tipoOperacao VARCHAR(1), vazioConteiner BOOLEAN, placa VARCHAR(7), placaSemirreboque VARCHAR(7), '
    'cnpjTransportador VARCHAR(15), cpfMotorista VARCHAR(11), nomeMotorista VARCHAR(50), '
    'numeroConhecimento VARCHAR(15))',
    'CREATE TABLE ovr_recintos (id VARCHAR(10) PRIMARY KEY, nome VARCHAR(50), cod_siscomex VARCHAR(20))',
    'CREATE TABLE laudo_empresas (id INTEGER PRIMARY KEY, cnpj VARCHAR(15), nome VARCHAR(255))',
]


def insere(con, tabela, linhas):
    colunas = list(linhas[0])
    con.execute(text(f'INSERT INTO {tabela} ({", ".join(colunas)}) VALUES ({", ".join(":" + c for c in colunas)})'),
                [{c: linha.get(c) for c in colunas} for linha in linhas])


def saida(id_, conteiner, data, **campos):
    return {'id': id_, 'numeroConteiner': conteiner, 'dataHoraOcorrencia': data, 'codigoRecinto': '8931356',
            'direcao': 'S', 'operacao': 'C', 'tipoOperacao': 'I', 'vazioConteiner': 0, 'placa': f'ABC{id_:04d}',
            'placaSemirreboque': None, 'cnpjTransportador': '99888777000166', 'cpfMotorista': '52998224725',
            'nomeMotorista': 'JOAO', 'numeroConhecimento': None, **campos}


def monta_banco(engine):
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        insere(con, 'manifestosresumo', [
            {'numero': 'M0', 'dataInicioOperacaoDate': '2025-06-01'},   # antigo demais
            {'numero': 'M1', 'dataInicioOperacaoDate': '2026-04-20'},
            {'numero': 'M2', 'dataInicioOperacaoDate': '2026-05-10'},
        ])
        insere(con, 'conhecimentosresumo', [
            {'numeroCEmercante': 'CE0', 'consignatario': CNPJ, 'descricao': 'antigo', 'manifestoCE': 'M0',
             'tipoTrafego': '5'},
            {'numeroCEmercante': 'CE1', 'consignatario': CNPJ, 'descricao': 'NOTEBOOKS', 'manifestoCE': 'M1',
             'tipoTrafego': '5'},
            {'numeroCEmercante': 'CE2', 'consignatario': CNPJ, 'descricao': 'exportação', 'manifestoCE': 'M1',
             'tipoTrafego': '7'},
            {'numeroCEmercante': 'CE3', 'consignatario': CNPJ, 'descricao': 'BRINQUEDOS', 'manifestoCE': 'M2',
             'tipoTrafego': '5'},
            {'numeroCEmercante': 'CE4', 'consignatario': '44555666000177', 'descricao': 'outro importador',
             'manifestoCE': 'M1', 'tipoTrafego': '5'},
        ])
        insere(con, 'itensresumo', [
            {'numeroCEmercante': 'CE0', 'codigoConteiner': 'CTRA'},
            {'numeroCEmercante': 'CE1', 'codigoConteiner': 'CTRA'},
            {'numeroCEmercante': 'CE1', 'codigoConteiner': 'CTRB'},
            {'numeroCEmercante': 'CE1', 'codigoConteiner': 'CTRC'},   # não sai no período
            {'numeroCEmercante': 'CE1', 'codigoConteiner': ''},       # carga solta
            {'numeroCEmercante': 'CE2', 'codigoConteiner': 'CTRX'},
            {'numeroCEmercante': 'CE3', 'codigoConteiner': 'CTRA'},   # mesmo contêiner, outra viagem
            {'numeroCEmercante': 'CE4', 'codigoConteiner': 'CTRY'},
        ])
        insere(con, 'ncmitemresumo', [
            {'numeroCEMercante': 'CE1', 'codigoConteiner': 'CTRA', 'identificacaoNCM': '8471.30.12'},
        ])
        insere(con, 'apirecintos_acessosveiculo', [
            saida(1, 'CTRA', datetime(2026, 5, 5, 10)),                                     # -> CE1
            saida(2, 'CTRA', datetime(2026, 5, 25, 9)),                                     # -> CE3
            saida(3, 'CTRB', datetime(2026, 5, 6, 8), numeroConhecimento='CE1'),
            saida(4, 'CTRB', datetime(2026, 5, 6, 8), numeroConhecimento='CE1', tipoOperacao='R',
                  placa='RET1234'),                                                         # retificação
            saida(5, 'CTRB', datetime(2026, 5, 7), direcao='E'),                            # entrada
            saida(6, 'CTRB', datetime(2026, 5, 8), operacao='G'),                           # agendamento
            saida(7, 'CTRA', datetime(2026, 5, 9), vazioConteiner=1),                       # vazio
            saida(8, 'CTRA', datetime(2026, 6, 15)),                                        # fora do período
            saida(9, 'CTRX', datetime(2026, 5, 5)),                                         # CE de exportação
            saida(10, 'CTRY', datetime(2026, 5, 5)),                                        # outro importador
        ])
        insere(con, 'ovr_recintos', [{'id': '70', 'nome': 'SANTOS BRASIL', 'cod_siscomex': '8931356'}])
        insere(con, 'laudo_empresas', [{'cnpj': CNPJ, 'nome': 'IMPORTADORA ALFA LTDA'}])


class EntregasTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        monta_banco(self.engine)
        self.conexao = self.engine.connect()

    def tearDown(self):
        self.conexao.close()


class TestBuscaEntregas(EntregasTestCase):

    def test_saidas_localizadas_e_ligacao_ao_ce(self):
        resultado = busca_entregas(self.conexao, '11.222.333/0001-81', INICIO, FIM)
        linhas = [(lin['conteiner'], lin['ce'], lin['placa'], lin['ligacao']) for lin in resultado['linhas']]
        self.assertEqual(linhas, [
            ('CTRA', 'CE1', 'ABC0001', 'contêiner e data'),
            ('CTRB', 'CE1', 'RET1234', 'confirmada pelo CE'),   # a retificação prevalece
            ('CTRA', 'CE3', 'ABC0002', 'contêiner e data'),     # mesmo contêiner, CE mais recente
        ])

    def test_resumo_e_enriquecimento(self):
        resultado = busca_entregas(self.conexao, CNPJ, INICIO, FIM)
        self.assertEqual(resultado['qtd_ces'], 2)             # CE1 e CE3 (CE0 antigo, CE2 exportação)
        self.assertEqual(resultado['qtd_conteineres'], 3)     # CTRA, CTRB, CTRC
        self.assertEqual(resultado['conteineres_sem_saida'], ['CTRC'])
        self.assertEqual(resultado['empresa'], 'IMPORTADORA ALFA LTDA')
        primeira = resultado['linhas'][0]
        self.assertEqual(primeira['nome_recinto'], 'SANTOS BRASIL')
        self.assertEqual(primeira['mercadoria'], 'NOTEBOOKS')
        self.assertTrue(primeira['ncms'].startswith('84713012 Máquinas automáticas'))
        self.assertEqual(primeira['data_saida'], datetime(2026, 5, 5, 10))
        self.assertEqual(primeira['empresa'], 'IMPORTADORA ALFA LTDA')

    def test_raiz_do_cnpj(self):
        resultado = busca_entregas(self.conexao, '11222333', INICIO, FIM)
        self.assertEqual(len(resultado['linhas']), 3)
        self.assertIsNone(resultado['empresa'])

    def test_parametros_invalidos(self):
        for cnpj, inicio, fim in (('123', INICIO, FIM), (CNPJ, None, FIM), (CNPJ, FIM, INICIO),
                                  (CNPJ, date(2024, 1, 1), date(2026, 1, 1))):
            with self.subTest(cnpj=cnpj, inicio=inicio, fim=fim):
                with self.assertRaises(ParametrosInvalidos):
                    busca_entregas(self.conexao, cnpj, inicio, fim)

    def test_planilha(self):
        resultado = busca_entregas(self.conexao, CNPJ, INICIO, FIM)
        abas = pd.read_excel(io.BytesIO(planilha_xlsx(resultado)), sheet_name=None, engine='openpyxl')
        self.assertEqual(list(abas), ['Entregas', 'Sem saída'])
        self.assertEqual(len(abas['Entregas']), 3)
        self.assertIn('Data de saída', abas['Entregas'].columns)
        self.assertEqual(list(abas['Sem saída']['Contêiner']), ['CTRC'])


class TestTela(EntregasTestCase):

    def setUp(self):
        super().setUp()
        pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        app = Flask(__name__, template_folder=pasta_templates)
        app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=sessionmaker(bind=self.engine)())
        app.register_blueprint(bp_entregas)
        self.client = app.test_client()

    def test_tela_sem_pesquisa(self):
        resposta = self.client.get('/entregas_cnpj/')
        self.assertEqual(resposta.status_code, 200)
        self.assertIn('Entregas por CNPJ', resposta.get_data(as_text=True))

    def test_tela_com_resultado(self):
        html = self.client.get(f'/entregas_cnpj/?cnpj={CNPJ}&inicio=2026-05-01&fim=2026-05-31').get_data(as_text=True)
        self.assertIn('CTRA', html)
        self.assertIn('RET1234', html)
        self.assertIn('SANTOS BRASIL', html)
        self.assertIn('Exportar planilha', html)
        self.assertIn('CTRC', html)  # sem saída

    def test_tela_com_erro(self):
        html = self.client.get('/entregas_cnpj/?cnpj=123&inicio=2026-05-01&fim=2026-05-31').get_data(as_text=True)
        self.assertIn('Informe o CNPJ completo', html)

    def test_planilha(self):
        resposta = self.client.get(f'/entregas_cnpj/planilha?cnpj={CNPJ}&inicio=2026-05-01&fim=2026-05-31')
        self.assertEqual(resposta.status_code, 200)
        self.assertIn('spreadsheetml', resposta.content_type)
        self.assertIn(f'entregas_{CNPJ}_2026-05-01_2026-05-31.xlsx', resposta.headers['Content-Disposition'])
        self.assertEqual(self.client.get('/entregas_cnpj/planilha?cnpj=1').status_code, 400)


if __name__ == '__main__':
    unittest.main()
