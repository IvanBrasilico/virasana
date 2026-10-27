"""Testes da tela /apreensoes_transporte (virasana/apreensoes): mesmo cenário
simulado do script (test_dados_transporte_apreensoes), em SQLite."""
import io
import os
import unittest

import pandas as pd
from flask import Flask
from flask_login import LoginManager, UserMixin
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from virasana.apreensoes.views_apreensoes import bp_apreensoes
from virasana.tests.test_dados_transporte_apreensoes import monta_banco

PERIODO = {'inicio': '2026-03-01', 'fim': '2026-04-30'}


class Usuario(UserMixin):
    id = 'teste'


class TestTelaApreensoes(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        monta_banco(engine)
        self.session = sessionmaker(bind=engine)()
        pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        app = Flask(__name__, template_folder=pasta_templates)
        app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=self.session)
        LoginManager(app).request_loader(lambda req: Usuario())
        app.register_blueprint(bp_apreensoes)
        self.client = app.test_client()

    def tearDown(self):
        self.session.close()

    def html(self, **params):
        resposta = self.client.get('/apreensoes_transporte/', query_string=params)
        self.assertEqual(resposta.status_code, 200)
        return resposta.get_data(as_text=True)

    def test_tela_inicial_sem_pesquisa(self):
        html = self.html()
        self.assertIn('Transporte das apreensões', html)
        self.assertNotIn('Exportar planilha', html)
        self.assertEqual(self.client.get('/apreensoes_transporte').status_code, 200)  # sem a barra

    def test_pesquisa_mostra_resumo_e_conjuntos(self):
        html = self.html(pesquisar=1, **PERIODO)
        self.assertIn('<strong>2</strong>RVFs com apreensão', html)        # RVF 10 e 20
        self.assertIn('<strong>1</strong>com algum acesso localizado', html)
        self.assertIn('<strong>3</strong>conjuntos', html)
        for trecho in ('ABC1D23', 'JOAO', 'conteiner_rvf+placa_texto+cpf_texto', 'nenhum acesso localizado',
                       'Exportar planilha', 'LGPD'):
            self.assertIn(trecho, html)
        self.assertNotIn('PQR1S23', html)  # outro contêiner do CE: só com "outras viagens"

    def test_outras_viagens(self):
        self.assertIn('PQR1S23', self.html(pesquisar=1, outras_viagens=1, **PERIODO))

    def test_parametros_invalidos(self):
        for params, mensagem in (({'inicio': '2026-13-01'}, 'Data inválida'),
                                 ({'inicio': '2026-05-01', 'fim': '2026-04-01'}, 'anterior'),
                                 ({'inicio': '2024-01-01', 'fim': '2026-01-01'}, 'Período máximo'),
                                 ({'dias_antes': '200'}, 'janela'),
                                 ({'dias_depois': 'x'}, 'Número inválido')):
            with self.subTest(params=params):
                self.assertIn(mensagem, self.html(pesquisar=1, **params))

    def test_planilha(self):
        resposta = self.client.get('/apreensoes_transporte/planilha', query_string=PERIODO)
        self.assertEqual(resposta.status_code, 200)
        self.assertIn('transporte_apreensoes_20260301_20260430.xlsx', resposta.headers['Content-Disposition'])
        abas = pd.read_excel(io.BytesIO(resposta.data), sheet_name=None, engine='openpyxl')
        self.assertEqual(list(abas), ['Conjuntos', 'Resumo'])
        self.assertEqual(len(abas['Conjuntos']), 4)   # 3 conjuntos da RVF 10 + "nenhuma" da RVF 20
        self.assertIn('Placa', abas['Conjuntos'].columns)
        self.assertEqual(self.client.get('/apreensoes_transporte/planilha?inicio=x').status_code, 400)

    def test_planilha_com_outras_viagens(self):
        resposta = self.client.get('/apreensoes_transporte/planilha',
                                   query_string={**PERIODO, 'outras_viagens': 1})
        abas = pd.read_excel(io.BytesIO(resposta.data), sheet_name=None, engine='openpyxl')
        self.assertEqual(len(abas['Conjuntos']), 7)   # 6 da RVF 10 + "nenhuma"


if __name__ == '__main__':
    unittest.main()
