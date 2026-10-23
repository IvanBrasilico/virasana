"""Tela SAÚDE DO SISTEMA (`/saude/`, virasana/saude/views_saude.py, 23/10/2026): só o perfil Supervisor do Fichas
vê e pede rodadas (decisão do usuário); os dados vêm do histórico no Mongo. O comportamento no navegador (painel,
faixa, achados, diálogo, "Rodar agora") foi conferido com servidor de demonstração; aqui, as rotas e o acesso.
"""
from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: ajusta o sys.path
import os  # noqa: I100
import unittest
from datetime import datetime, timedelta
from unittest import mock

import mongomock
from flask import Flask
from flask_login import LoginManager, UserMixin

from virasana.saude import historico, views_saude
from virasana.saude.verificacao import ATENCAO, OK, Resultado


class Usuario(UserMixin):
    id = '12345678901'


def cria_app(mongo=None):
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'))
    app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=cria_sessao(), mongodb=mongo)
    LoginManager(app).request_loader(lambda req: Usuario())
    app.register_blueprint(views_saude.bp_saude)
    return app


class TestTelaSaude(unittest.TestCase):

    def setUp(self):
        self.mongo = mongomock.MongoClient().db
        self.cliente = cria_app(self.mongo).test_client()

    def supervisor(self, sim=True):
        return mock.patch.object(views_saude, 'eh_supervisor', return_value=sim)

    def test_quem_nao_e_supervisor_nao_ve_nem_pede(self):
        with self.supervisor(False):
            pagina = self.cliente.get('/saude/')
            self.assertEqual(pagina.status_code, 403)
            self.assertIn('Acesso restrito ao perfil Supervisor do Fichas', pagina.get_data(as_text=True))
            self.assertNotIn('PAINEL_URL', pagina.get_data(as_text=True))
            for resposta in (self.cliente.get('/saude/api/painel'), self.cliente.get('/saude/api/rodadas'),
                             self.cliente.post('/saude/api/rodar')):
                self.assertEqual(resposta.status_code, 403)
        self.assertIsNone(self.mongo[historico.PEDIDOS].find_one())

    def test_supervisor_ve_o_painel_e_pede_rodada(self):
        inicio = datetime.now() - timedelta(hours=2)
        rodada = historico.grava_rodada(self.mongo, [Resultado('Mercante (carga)', OK, 'em dia'),
                                                     Resultado('API Recintos (carga)', ATENCAO, 'resumo', ['achado'])],
                                        inicio, inicio + timedelta(seconds=80), 'agenda')
        with self.supervisor():
            pagina = self.cliente.get('/saude')   # sem a barra final: sem redirect
            self.assertEqual(pagina.status_code, 200)
            self.assertIn('var PAINEL_URL = "/saude/api/painel"', pagina.get_data(as_text=True))
            painel = self.cliente.get('/saude/api/painel').get_json()
            self.assertEqual((painel['ultima']['id'], painel['ultima']['geral']), (str(rodada), ATENCAO))
            self.assertTrue(painel['ultima']['resultados'][1]['detalhes'][0]['novo'])
            pedido = self.cliente.post('/saude/api/rodar')
            self.assertEqual((pedido.status_code, pedido.get_json()['usuario']), (202, '12345678901'))
            self.assertEqual(self.cliente.get('/saude/api/painel').get_json()['pedido']['estado'], historico.PENDENTE)
            self.assertEqual(self.cliente.get(f'/saude/api/rodada/{rodada}').get_json()['segundos'], 80.0)
            self.assertEqual(self.cliente.get('/saude/api/rodada/inexistente').status_code, 404)
            self.assertEqual(len(self.cliente.get('/saude/api/rodadas').get_json()['rodadas']), 1)
            self.assertEqual(self.cliente.get('/saude/api/rodadas?antes=ontem').status_code, 400)

    def test_sem_o_mongo(self):
        cliente = cria_app(None).test_client()
        with self.supervisor():
            self.assertEqual(cliente.get('/saude/api/painel').status_code, 503)

    def test_supervisor_pela_regra_das_estatisticas(self):
        with cria_app(self.mongo).test_request_context('/saude/'), \
                mock.patch('virasana.navii.estatisticas_uso.acesso_do_usuario') as acesso:
            acesso.return_value.supervisor = True
            self.assertTrue(views_saude.eh_supervisor())
            self.assertEqual(acesso.call_args[0][1], '12345678901')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
