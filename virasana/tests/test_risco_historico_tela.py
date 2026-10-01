"""Testes da tela /risco_historico (virasana/risco_historico), com o cenário
simulado da extração (test_dados_risco_perdimentos)."""
import io
import os
import unittest

import pandas as pd
from flask import Flask
from flask_login import LoginManager, UserMixin
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from virasana.risco_historico import views_risco_historico as vrh
from virasana.tests.test_dados_risco_perdimentos import monta_banco


class Usuario(UserMixin):
    id = 'teste'


class TestTelaRiscoHistorico(unittest.TestCase):

    def setUp(self):
        vrh.limpa_cache()
        engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        monta_banco(engine)
        self.session = sessionmaker(bind=engine)()
        pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        app = Flask(__name__, template_folder=pasta_templates)
        app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=self.session)
        LoginManager(app).request_loader(lambda req: Usuario())
        app.register_blueprint(vrh.bp_risco_historico)
        self.client = app.test_client()

    def tearDown(self):
        self.session.close()
        vrh.limpa_cache()

    def html(self, query_string=None):
        resposta = self.client.get('/risco_historico/', query_string=query_string)
        self.assertEqual(resposta.status_code, 200)
        return resposta.get_data(as_text=True)

    def test_tela_inicial(self):
        html = self.html()
        self.assertIn('Risco histórico das Fichas', html)
        self.assertNotIn('Exportar planilha', html)
        self.assertEqual(self.client.get('/risco_historico').status_code, 200)  # sem a barra

    def test_pesquisa_padrao(self):
        html = self.html({'pesquisar': 1})
        self.assertIn('<strong>3</strong>fichas', html)
        self.assertIn('<strong>1</strong>fichas de exportação fora', html)
        self.assertIn('IMPORTADORA ALFA LTDA', html)                # importador em 2 fichas
        self.assertIn('11222333', html)
        self.assertIn('ACME', html)
        self.assertIn('href="/bhadrasana2/ovr?id=2"', html)
        self.assertIn('Nenhum valor em 2 ou mais fichas', html)     # ex.: marcas (1 ficha só)
        self.assertIn('LGPD', html)

    def test_minimo_1_mostra_tudo(self):
        html = self.html({'pesquisar': 1, 'minimo': 1})
        self.assertIn('MARCA X', html)
        self.assertIn('SGSIN', html)

    def test_tipos_e_exportacao(self):
        html = self.html([('pesquisar', 1), ('tipos', 2)])
        self.assertIn('<strong>2</strong>fichas', html)             # só perdimento: fichas 1 e 4
        html = self.html({'pesquisar': 1, 'exportacao': 1, 'minimo': 1})
        self.assertIn('<strong>4</strong>fichas', html)
        self.assertIn('BRSSZ &gt; USNYC', html)

    def test_parametros_invalidos(self):
        self.assertIn('Data inválida', self.html({'pesquisar': 1, 'inicio': '2026-13-01'}))
        self.assertIn('anterior', self.html({'pesquisar': 1, 'inicio': '2026-05-01', 'fim': '2026-01-01'}))
        self.assertIn('Mínimo de fichas inválido', self.html({'pesquisar': 1, 'minimo': 'x'}))

    def test_cache_por_parametros(self):
        self.html({'pesquisar': 1})
        self.html({'pesquisar': 1, 'minimo': 5})   # mínimo não muda a extração
        self.assertEqual(len(vrh._cache), 1)
        self.html([('pesquisar', 1), ('tipos', 2)])
        self.assertEqual(len(vrh._cache), 2)

    def test_planilha(self):
        resposta = self.client.get('/risco_historico/planilha', query_string=[('tipos', 1), ('tipos', 2)])
        self.assertEqual(resposta.status_code, 200)
        self.assertIn('risco_historico_fichas_', resposta.headers['Content-Disposition'])
        abas = pd.read_excel(io.BytesIO(resposta.data), sheet_name=None, engine='openpyxl')
        self.assertEqual(len(abas['Fichas']), 3)
        self.assertIn('Apreensao, Perdimento', abas['Resumo']['Valor'].astype(str).tolist())
        self.assertEqual(self.client.get('/risco_historico/planilha?inicio=x').status_code, 400)


if __name__ == '__main__':
    unittest.main()
