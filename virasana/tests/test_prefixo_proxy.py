"""As telas novas têm de funcionar como no Servidor: app montada no prefixo
/virasana (DispatcherMiddleware de wsgi_production.py) atrás do proxy Apache,
que só encaminha o que está sob o prefixo e repassa um Host interno.

Verifica, para cada tela: abre com e sem barra final SEM redirect (o redirect
automático do Werkzeug sai com o Host interno e quebra atrás do proxy), e toda
URL interna emitida no HTML/JS começa com o prefixo.
"""
import os
import re
import unittest

from flask import Flask, url_for
from flask_login import LoginManager, UserMixin
from flask_nav.elements import Navbar
from flask_nav.renderers import SimpleRenderer
from werkzeug.exceptions import NotFound
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.test import Client

from virasana.entregas.views_entregas import bp_entregas
from virasana.navii.menu import ENTRADAS_NAVII, URL_MANUAL, subgrupo_navii
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import cria_sessao
from virasana.tests.test_navii_filtros_salvos import BancoFalso

PREFIXO = '/virasana'
HOST_INTERNO = '127.0.0.1:5001'

# (endereço sem o prefixo, trecho esperado na página)
TELAS = [
    ('/navii', 'NAVI-I'),
    ('/entregas_cnpj', 'Entregas por CNPJ'),
]

URL_EM_ATRIBUTO = re.compile(r'''(?:href|src|action)\s*=\s*["'](/[^"']*)''')
URL_EM_STRING_JS = re.compile(r'''=\s*"(/[^"]*)"''')  # var X = "/..." (saída do | tojson)


class Usuario(UserMixin):
    id = 'teste'


def cria_app():
    pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
    app = Flask(__name__, template_folder=pasta_templates)
    app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=cria_sessao(), mongodb=BancoFalso())
    LoginManager(app).request_loader(lambda req: Usuario())

    @app.route('/mini1/<_id>')  # mesma rota/endpoint de virasana/views.py
    def mini(_id):
        return ''

    app.register_blueprint(bp_navii)
    app.register_blueprint(bp_entregas)
    return app


def urls_internas(html):
    return URL_EM_ATRIBUTO.findall(html) + URL_EM_STRING_JS.findall(html)


class TestPrefixoProxy(unittest.TestCase):

    def setUp(self):
        # o "Apache": fora do prefixo, nada responde
        self.client = Client(DispatcherMiddleware(NotFound(), {PREFIXO: cria_app()}))

    def get(self, caminho):
        return self.client.get(PREFIXO + caminho, environ_base={'HTTP_HOST': HOST_INTERNO})

    def test_telas_abrem_com_e_sem_barra_final_sem_redirect(self):
        for caminho, trecho in TELAS:
            for variante in (caminho, caminho + '/'):
                with self.subTest(url=variante):
                    resposta = self.get(variante)
                    self.assertEqual(resposta.status_code, 200, resposta.headers.get('Location'))
                    self.assertIn(trecho, resposta.get_data(as_text=True))

    def test_urls_internas_do_html_e_do_js_tem_o_prefixo(self):
        for caminho, _ in TELAS:
            html = self.get(caminho + '/').get_data(as_text=True)
            urls = urls_internas(html)
            with self.subTest(tela=caminho):
                self.assertTrue(urls, 'nenhuma URL interna encontrada — o teste não está olhando nada')
                for url in urls:
                    self.assertTrue(url.startswith(PREFIXO + '/'), f'URL sem o prefixo: {url}')

    def test_navii_js_recebe_as_urls_das_apis_com_prefixo(self):
        html = self.get('/navii/').get_data(as_text=True)
        for esperado in ('/virasana/navii/api/ces', '/virasana/navii/api/sugestoes',
                         '/virasana/navii/api/filtros', '/virasana/mini1/imagem_id'):
            self.assertIn(f'"{esperado}"', html)

    def test_apis_respondem_sob_o_prefixo(self):
        for caminho in ('/navii/api/sugestoes?campo=porto&q=BRSSZ', '/navii/api/filtros'):
            with self.subTest(url=caminho):
                self.assertEqual(self.get(caminho).status_code, 200)

    def test_fora_do_prefixo_nao_responde(self):
        # garante que o teste simula o proxy de verdade
        self.assertEqual(self.client.get('/navii/').status_code, 404)

    def test_formulario_e_planilha_das_entregas(self):
        html = self.get('/entregas_cnpj/?cnpj=123&inicio=2026-05-01&fim=2026-05-31').get_data(as_text=True)
        self.assertIn(f'action="{PREFIXO}/entregas_cnpj/"', html)


class TestMenuNavii(unittest.TestCase):
    """O submenu NAVII é renderizado em TODAS as páginas do virasana: um
    endpoint errado derrubaria o sistema inteiro."""

    def test_cada_entrada_monta_com_o_prefixo(self):
        app = cria_app()
        with app.test_request_context('/', base_url=f'http://{HOST_INTERNO}{PREFIXO}/'):
            html = SimpleRenderer().visit(Navbar('AJNA', subgrupo_navii())).render()
            for _, endpoint in ENTRADAS_NAVII:
                with self.subTest(endpoint=endpoint):
                    url = url_for(endpoint)
                    self.assertTrue(url.startswith(PREFIXO + '/'), url)
                    self.assertIn(f'href="{url}"', html)
        self.assertIn('NAVII', html)
        self.assertIn(f'href="{URL_MANUAL}"', html)


if __name__ == '__main__':
    unittest.main()
