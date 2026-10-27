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
from unittest import mock

from flask import Flask, url_for
from flask_login import LoginManager, UserMixin
from flask_nav.elements import Navbar
from flask_nav.renderers import SimpleRenderer
from werkzeug.exceptions import NotFound
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.test import Client

from virasana.apreensoes.views_apreensoes import bp_apreensoes
from virasana.completude.views_completude import bp_completude
from virasana.entregas.views_entregas import bp_entregas
from virasana.risco_historico.views_risco_historico import bp_risco_historico
from virasana.navii.menu import ENTRADAS_NAVII, URL_MANUAL, subgrupo_navii
from virasana.navii.views_navii import bp_navii
from virasana.saude import views_saude
from virasana.tests.test_navii_busca_sqlite import cria_sessao
from virasana.tests.test_navii_filtros_salvos import banco_mongo

PREFIXO = '/virasana'
HOST_INTERNO = '127.0.0.1:5001'

# (endereço sem o prefixo, trecho esperado na página)
TELAS = [
    ('/navii', 'NAVI-I'),
    ('/entregas_cnpj', 'Entregas por CNPJ'),
    ('/apreensoes_transporte', 'Transporte das apreensões'),
    ('/risco_historico', 'Risco histórico das Fichas'),
    ('/risco_historico/parametros', 'Parâmetros do risco por CE'),
    ('/navii/extrato', 'Extrato do CE'),
    ('/navii/texto', 'Busca por texto'),
    ('/navii/fichas', 'Fichas em acompanhamento'),
    ('/navii/estatisticas', 'Estatísticas de uso'),
    ('/completude_escaneamentos', 'Completude de escaneamentos'),
    ('/saude', 'Saúde do sistema'),   # o usuário de teste como Supervisor (setUp)
]

# links para a OUTRA app usam o prefixo dela (ex.: ficha no Fichas)
OUTRAS_APPS = ('/bhadrasana2/',)

URL_EM_ATRIBUTO = re.compile(r'''(?:href|src|action)\s*=\s*["'](/[^"']*)''')
URL_EM_STRING_JS = re.compile(r'''=\s*"(/[^"]*)"''')  # var X = "/..." (saída do | tojson)


class Usuario(UserMixin):
    id = 'teste'


def cria_app():
    pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
    app = Flask(__name__, template_folder=pasta_templates)
    app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=cria_sessao(), mongodb=banco_mongo())
    LoginManager(app).request_loader(lambda req: Usuario())

    @app.route('/mini1/<_id>')  # mesma rota/endpoint de virasana/views.py
    def mini(_id):
        return ''

    app.register_blueprint(bp_navii)
    app.register_blueprint(bp_entregas)
    app.register_blueprint(bp_apreensoes)
    app.register_blueprint(bp_risco_historico)
    app.register_blueprint(bp_completude)
    app.register_blueprint(views_saude.bp_saude)
    return app


def urls_internas(html):
    return URL_EM_ATRIBUTO.findall(html) + URL_EM_STRING_JS.findall(html)


class TestPrefixoProxy(unittest.TestCase):

    def setUp(self):
        # o "Apache": fora do prefixo, nada responde
        self.client = Client(DispatcherMiddleware(NotFound(), {PREFIXO: cria_app()}))
        supervisor = mock.patch.object(views_saude, 'eh_supervisor', return_value=True)   # a tela de saúde
        supervisor.start()
        self.addCleanup(supervisor.stop)

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
                    self.assertTrue(url.startswith((PREFIXO + '/',) + OUTRAS_APPS), f'URL sem o prefixo: {url}')

    def test_navii_js_recebe_as_urls_das_apis_com_prefixo(self):
        html = self.get('/navii/').get_data(as_text=True)
        for esperado in ('/virasana/navii/api/ces', '/virasana/navii/api/sugestoes',
                         '/virasana/navii/api/filtros', '/virasana/navii/api/uso', '/virasana/mini1/imagem_id',
                         '/virasana/navii/api/texto'):
            self.assertIn(f'"{esperado}"', html)

    def test_navii_aberto_por_link_com_filtros_da_url(self):
        html = self.get('/navii/?conteiner=abcu1234567&tipo_janela=prevista&data_inicio=2026-01-05'
                        '&data_fim=2026-01-15').get_data(as_text=True)
        self.assertIn('"filtros": {"conteiner": ["ABCU1234567"]}', html)
        self.assertIn('"periodo": {"fim": "2026-01-15", "inicio": "2026-01-05"}', html)
        self.assertIn('"tipo_janela": "prevista"', html)
        self.assertIn('var NAVII_URL = "/virasana/navii/"', html)
        self.assertIn('var FILTROS_INICIAIS = null', self.get('/navii/').get_data(as_text=True))
        html = self.get('/navii/?ce=152600000000101&tipo_janela=efetiva').get_data(as_text=True)   # link do CE
        self.assertIn('"filtros": {"ce": ["152600000000101"]}', html)
        self.assertIn('"periodo": null', html)
        # data inválida: sem período (vale a sugestão da tela)
        self.assertIn('"periodo": null', self.get('/navii/?conteiner=X1&data_inicio=xx&data_fim=yy')
                      .get_data(as_text=True))

    def test_link_da_completude_para_o_navii_com_o_prefixo(self):
        from datetime import datetime
        from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao
        app = cria_app()
        sessao = app.config['db_session']
        sessao.add(CicloDeVidaImportacao(numeroConteiner='T1', numeroCEmercante='CET1', concluido=True,
                                         codigoRecintoDesembarque='8931356', codigoRecintoArmazenagem='8932000',
                                         data_desembarque=datetime(2026, 3, 1), data_saida=datetime(2026, 3, 1, 20),
                                         data_entrada_seco=datetime(2026, 3, 2)))
        sessao.commit()
        cliente = Client(DispatcherMiddleware(NotFound(), {PREFIXO: app}))
        html = cliente.get(PREFIXO + '/completude_escaneamentos/recinto/8932000?fluxo=tra&inicio=2026-03-01'
                           '&fim=2026-03-07', environ_base={'HTTP_HOST': HOST_INTERNO}).get_data(as_text=True)
        self.assertIn('href="/virasana/navii/?conteiner=T1&amp;tipo_janela=efetiva&amp;data_inicio=2026-02-28'
                      '&amp;data_fim=2026-03-02"', html)

    def test_extrato_do_ce_e_links_para_ele(self):
        from virasana.tests.test_navii_busca_sqlite import CE_EXTRATO, monta_extrato_cenario
        app = cria_app()
        monta_extrato_cenario(app.config['db_session'])
        cliente = Client(DispatcherMiddleware(NotFound(), {PREFIXO: app}))

        def get(caminho):
            return cliente.get(PREFIXO + caminho, environ_base={'HTTP_HOST': HOST_INTERNO}).get_data(as_text=True)
        html = get(f'/navii/extrato?CE={CE_EXTRATO}')
        for trecho in ('EXTRATO DO CE', 'C8', 'C9', 'Desembarque', 'DTA 2612345678', 'Saída do recinto',
                       f'href="/virasana/navii/?ce={CE_EXTRATO}&amp;tipo_janela=efetiva"',
                       'src="/virasana/mini1/5f00000000000000000000c8"'):
            self.assertIn(trecho, html)
        for url in urls_internas(html):
            self.assertTrue(url.startswith((PREFIXO + '/',) + OUTRAS_APPS), f'URL sem o prefixo: {url}')
        self.assertIn('não encontrado no Mercante', get('/navii/extrato?CE=999'))
        self.assertIn('var EXTRATO_URL = "/virasana/navii/extrato"', get('/navii/'))

    def test_script_do_navii_so_chama_funcoes_definidas(self):
        """O JS dos cards vive no template e os testes não o executam: confere, sem strings nem comentários, que
        toda função chamada está definida (em 15/10/2026 uma edição apagou funções do risco e quebrou os cards).
        Vale também para as abas (Fichas em acompanhamento, Estatísticas de uso)."""
        for caminho, esperada in (('/navii/', 'montaSeloRisco'), ('/navii/texto', 'carregarPaginaTexto'),
                                  ('/navii/fichas', 'montaCard'),
                                  ('/navii/estatisticas', 'calculaUniverso'), ('/saude/', 'renderizaPainel')):
            with self.subTest(tela=caminho):
                html = self.get(caminho).get_data(as_text=True)
                script = html[html.index('<script>\n(function'):html.rindex('</script>')]
                codigo = re.sub(r'(^|\s)//[^\n]*', r'\1', script)                               # comentários
                codigo = re.sub(r"'(?:\\.|[^'\\\n])*'|\"(?:\\.|[^\"\\\n])*\"", "''", codigo)   # strings
                codigo = re.sub(r'/(?:\\.|[^/\\\n])+/[gimsuy]*', '//', codigo)                 # expressões regulares
                definidas = (set(re.findall(r'function (\w+)\s*\(', codigo))
                             | set(re.findall(r'var (\w+) = function', codigo)))
                chamadas = set(re.findall(r'(?<![\w.$])([a-z]\w*)\s*\(', codigo))
                nativas = {'if', 'for', 'while', 'switch', 'catch', 'function', 'return', 'typeof', 'fetch',
                           'parseInt', 'parseFloat', 'isNaN', 'encodeURIComponent', 'setTimeout', 'clearTimeout'}
                self.assertEqual(chamadas - definidas - nativas, set())
                self.assertIn(esperada, definidas)

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
