"""Testes da aba "Busca por texto" do NAVI-I (F2): página em modo texto, API /navii/api/texto com um Meilisearch
SIMULADO (o cliente vai em `app.config['navii_meili']`) e o Mercante em SQLite.

CEs: A = 152600000000001 (contêineres C1 e C2), B = 152600000000002 (C3), SEM = 152600000000003 (sem item no
Mercante: sem card). O índice "acha" na ordem que cada teste manda.
"""
import json
import os
import unittest
from datetime import date
from unittest import mock

import mongomock
import requests
from flask import Flask
from flask_login import LoginManager, UserMixin

from virasana.navii import coleta_uso, tabelas_opcionais, texto_navii, views_navii
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import cria_ce, cria_sessao

CE_A, CE_B, CE_SEM = '152600000000001', '152600000000002', '152600000000003'
USUARIO = '12345678901'


class Usuario(UserMixin):
    def __init__(self, id_):
        self.id = id_


class MeiliDeBusca:
    """O Meilisearch na busca: devolve os CEs pedidos (com o trecho do embarcador) e guarda os pedidos."""

    def __init__(self, ces=(), total=None, erro=None):
        self.ces, self.total, self.erro, self.pedidos = list(ces), total, erro, []

    def busca(self, uid, corpo):
        self.pedidos.append((uid, corpo))
        if self.erro:
            raise self.erro
        inicio = corpo['offset']
        hits = [{'ce': ce, 'referencia': 1790812800, '_matchesPosition': {'embarcador': [{}]},
                 '_formatted': {'embarcador': '<b>\x02ACME\x03</b> EXPORT CO'}}
                for ce in self.ces[inicio:inicio + corpo['limit']]]
        return {'hits': hits, 'estimatedTotalHits': self.total if self.total is not None else len(self.ces)}


class TextoTestCase(unittest.TestCase):

    def setUp(self):
        tabelas_opcionais.limpa_cache()
        coleta_uso.limpa_cache()
        self.session = cria_sessao()
        cria_ce(self.session, CE_A, 'M1', {'C1': [], 'C2': []}, embarcador='ACME EXPORT CO')
        cria_ce(self.session, CE_B, 'M1', {'C3': []}, embarcador='ACME EXPORT CO')
        cria_ce(self.session, CE_SEM, 'M1', {}, embarcador='ACME EXPORT CO')
        self.session.commit()
        self.db = mongomock.MongoClient()['virasana']
        self.meili = MeiliDeBusca([CE_B, CE_A, CE_SEM])
        self.app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                                                'templates'))
        self.app.config.update(TESTING=True, db_session=self.session, mongodb=self.db, SECRET_KEY='x',
                               navii_meili=self.meili)
        LoginManager(self.app).request_loader(
            lambda req: Usuario(req.headers['X-Usuario']) if req.headers.get('X-Usuario') else None)

        @self.app.route('/mini1/<_id>')
        def mini(_id):
            return ''
        self.app.register_blueprint(bp_navii)
        self.client = self.app.test_client()

    def tearDown(self):
        self.session.close()

    def api(self, **parametros):
        parametros = {'q': 'acme export', **parametros}
        return self.client.get('/navii/api/texto', query_string=parametros, headers={'X-Usuario': USUARIO})

    def pagina(self, caminho):
        return self.client.get(caminho, headers={'X-Usuario': USUARIO}).get_data(as_text=True)


class TestPagina(TextoTestCase):

    def test_modo_texto(self):
        html = self.pagina('/navii/texto')
        for trecho in ('var MODO_TEXTO = true', 'var TEXTO_URL = "/navii/api/texto"', 'var MINIMO_TEXTO = 5',
                       'id="t-texto"', 'aria-current="page">Busca por texto', 'layout-navii modo-texto',
                       'id="painel-filtros" hidden', 'Nenhum CE encontrado', 'A busca começa a partir de 5 caracteres'):
            self.assertIn(trecho, html)
        self.assertIn('var PERIODO_TEXTO = ' + json.dumps(views_navii.periodo_texto_padrao(), sort_keys=True), html)
        self.assertIn('<title>NAVI-I — Busca por texto</title>', html)

    def test_aba_de_analise_continua_igual(self):
        html = self.pagina('/navii/')
        self.assertIn('var MODO_TEXTO = false', html)
        self.assertIn('aria-current="page">Análise de risco', html)
        self.assertIn('href="/navii/texto">Busca por texto', html)
        self.assertNotIn('id="t-texto"', html)
        self.assertNotIn('id="painel-filtros" hidden', html)
        for tela in ('/navii/fichas', '/navii/estatisticas'):                # a aba nova nas outras telas
            self.assertIn('href="/navii/texto">Busca por texto', self.pagina(tela))
        estatisticas = self.pagina('/navii/estatisticas')                    # origem e "janela" nas estatísticas
        self.assertIn("desfazer: 'Desfazer', texto: 'Busca por texto'", estatisticas)
        self.assertIn("texto: 'Busca por texto' };", estatisticas)
        self.assertIn('texto', coleta_uso.ORIGENS)

    def test_aberta_por_link_com_o_texto_escapado(self):
        html = self.pagina('/navii/texto?q=%22%3E%3Cscript%3Ealert(1)%3C/script%3E')
        self.assertIn('var TEXTO_INICIAL = "\\"\\u003e\\u003cscript\\u003ealert(1)\\u003c/script\\u003e"', html)
        self.assertIn('value="&#34;&gt;&lt;script&gt;alert(1)&lt;/script&gt;"', html)
        self.assertNotIn('<script>alert(1)', html)

    def test_periodo_padrao(self):
        self.assertEqual(views_navii.periodo_texto_padrao(date(2026, 10, 19)),
                         {'inicio': '2026-10-04', 'fim': '2026-11-18'})


class TestApi(TextoTestCase):

    def test_cards_na_ordem_de_relevancia_com_os_trechos(self):
        dados = self.api().get_json()
        self.assertEqual([(c['numeroCEmercante'], c['numeroConteiner']) for c in dados['cards']],
                         [(CE_B, 'C3'), (CE_A, 'C2'), (CE_A, 'C1')])     # dentro do CE: a ordem da busca
        self.meili.ces = [CE_A, CE_B]                                        # relevância contra a ordem dos IDs
        self.assertEqual([(c['numeroCEmercante'], c['numeroConteiner']) for c in self.api().get_json()['cards']],
                         [(CE_A, 'C2'), (CE_A, 'C1'), (CE_B, 'C3')])
        self.assertEqual(dados['cards'][0]['trechos'],
                         [{'campo': 'embarcador', 'html': '&lt;b&gt;<mark>ACME</mark>&lt;/b&gt; EXPORT CO'}])
        self.assertEqual([s['ce'] for s in dados['sem_card']], [CE_SEM])
        self.assertEqual(dados['sem_card'][0]['referencia'], '2026-10-01')
        self.assertEqual((dados['total'], dados['proxima'], dados['avisos']), (3, None, []))
        self.assertEqual(set(dados['tempos']), {'busca', 'consulta', 'cards'})

    def test_pedido_ao_indice(self):
        self.api(de='2026-10-01', ate='2026-10-31')
        uid, corpo = self.meili.pedidos[-1]
        self.assertEqual((uid, corpo['q'], corpo['limit'], corpo['offset']), (texto_navii.INDICE, 'acme export', 10, 0))
        self.assertEqual(corpo['filter'], 'referencia >= 1790812800 AND referencia < 1793491200')
        self.api(q='  acme    export  ')                                     # espaços normalizados, sem período
        self.assertEqual(self.meili.pedidos[-1][1]['q'], 'acme export')
        self.assertNotIn('filter', self.meili.pedidos[-1][1])

    def test_paginas(self):
        self.meili.ces = [f'1526000000001{n:02d}' for n in range(25)]
        primeira = self.api().get_json()
        self.assertEqual((primeira['proxima'], len(primeira['sem_card'])), (1, 10))
        segunda = self.api(pagina=1).get_json()
        self.assertEqual((segunda['proxima'], self.meili.pedidos[-1][1]['offset']), (2, 10))
        ultima = self.api(pagina=2).get_json()
        self.assertEqual((ultima['proxima'], len(ultima['sem_card'])), (None, 5))
        self.meili.ces = self.meili.ces[:20]                                 # exatamente 2 páginas cheias
        self.assertEqual(self.api(pagina=1).get_json()['proxima'], None)

    def test_texto_curto_e_data_invalida(self):
        resposta = self.api(q='acme')
        self.assertEqual((resposta.status_code, resposta.get_json()['erro']), (400, 'Digite pelo menos 5 caracteres.'))
        self.assertEqual(self.api(q='ac me').status_code, 200)                # 5 com o espaço
        self.assertEqual(self.api(de='19/10/2026').status_code, 400)
        self.assertEqual(self.meili.pedidos[-1][1]['q'], 'ac me')            # os inválidos nem chegam ao índice

    def test_meilisearch_fora_do_ar(self):
        for erro in (requests.ConnectionError('recusada'), texto_navii.ErroMeili('HTTP 500')):
            with self.subTest(erro=erro):
                self.meili.erro = erro
                resposta = self.api()
                self.assertEqual(resposta.status_code, 503)
                self.assertIn('indisponível no momento', resposta.get_json()['erro'])

    def test_sem_configuracao(self):
        self.app.config['navii_meili'] = None
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(texto_navii.VARIAVEL_BUSCA, None)
            resposta = self.api()
        self.assertEqual(resposta.status_code, 503)
        self.assertIn('não está configurada', resposta.get_json()['erro'])

    def test_cliente_do_ambiente_com_a_chave_de_busca(self):
        self.app.config['navii_meili'] = None
        with self.app.test_request_context(), mock.patch.dict(os.environ, {texto_navii.VARIAVEL_BUSCA: 'b',
                                                                           'NAVII_MEILI_URL': 'http://127.0.0.1:7701'}):
            cliente = views_navii._cliente_texto()
        self.assertEqual((cliente.url, cliente.cabecalhos, cliente.timeout),
                         ('http://127.0.0.1:7701', {'Authorization': 'Bearer b'}, views_navii.TIMEOUT_TEXTO))

    def test_pagina_com_muitos_conteineres(self):
        with mock.patch.object(views_navii, 'MAX_CARDS_TEXTO', 2):
            dados = self.api().get_json()
        self.assertEqual(len(dados['cards']), 2)
        self.assertIn('muitos contêineres', dados['avisos'][0])

    def test_sem_nada_encontrado(self):
        self.meili.ces = []
        dados = self.api().get_json()
        self.assertEqual((dados['cards'], dados['sem_card'], dados['total'], dados['proxima']), ([], [], 0, None))


class TestColetaDeUso(TextoTestCase):

    def test_pesquisa_e_pagina_seguinte(self):
        self.meili.ces = [CE_B, CE_A] + [f'1526000000001{n:02d}' for n in range(13)]
        dados = self.api(de='2026-10-01', ate='2026-10-31', aba='aba-de-teste-1', origem='texto').get_json()
        doc = coleta_uso.colecao(self.db).find_one()
        self.assertEqual(str(doc['_id']), dados['pesquisa'])
        self.assertEqual((doc['origem'], doc['janela'], doc['filtros'], doc['total']),
                         ('texto', 'texto', {'texto': 'acme export'}, 15))
        self.assertEqual(doc['periodo'], {'inicio': '2026-10-01', 'fim': '2026-10-31'})
        self.assertEqual([c['i'] for c in doc['cards']], [c['item_id'] for c in dados['cards']])
        seguinte = self.api(pagina=1, pesquisa=dados['pesquisa']).get_json()
        self.assertEqual(seguinte['pesquisa'], dados['pesquisa'])
        self.assertEqual(coleta_uso.colecao(self.db).find_one()['paginas'], 2)
        self.assertEqual(coleta_uso.colecao(self.db).count_documents({}), 1)

    def test_sem_periodo_completo_fica_sem_periodo(self):
        self.api(de='2026-10-01')
        self.assertIsNone(coleta_uso.colecao(self.db).find_one()['periodo'])


if __name__ == '__main__':
    unittest.main()
