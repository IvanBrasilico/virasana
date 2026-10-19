"""Filtro por faixa do peso bruto declarado do CONTÊINER (Item.pesoBruto) na aba de análise do NAVI-I (20/10/2026).

Cenário da busca (test_navii_busca_sqlite.monta_cenario), com os pesos trocados — o Mercante guarda texto:
  C1 '25000.5' (ponto)   C2 '30000,7' (vírgula)   C3 '' (vazio)   C6 'abc' (inválido)    -> janela prevista
  C4 ' 5000 ' (espaços)                                                                  -> janela efetiva
"""
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase  # 1º: ajusta o sys.path
import os  # noqa: I100

from flask import Flask

from virasana.navii import filtros_salvos
from virasana.navii.busca_navii import Item, conta_conteineres_navii, normaliza_filtros
from virasana.navii.views_navii import bp_navii

PESOS = {'C1': '25000.5', 'C2': '30000,7', 'C3': '', 'C4': ' 5000 ', 'C6': 'abc'}


class TestFiltroPeso(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        for item in self.session.query(Item):
            if item.codigoConteiner in PESOS:
                item.pesoBruto = PESOS[item.codigoConteiner]
        self.session.commit()

    def prevista(self, **filtros):
        return self.conteineres('prevista', **filtros)

    def test_sem_filtro_todos(self):
        self.assertEqual(self.prevista(), {'C1', 'C2', 'C3', 'C6'})

    def test_minimo_maximo_e_faixa_inclusiva(self):
        self.assertEqual(self.prevista(peso_min=20000), {'C1', 'C2'})
        self.assertEqual(self.prevista(peso_max=26000), {'C1'})              # vazio e inválido ficam de fora
        self.assertEqual(self.prevista(peso_min=25000.5, peso_max=30000.7), {'C1', 'C2'})   # as duas pontas entram
        self.assertEqual(self.prevista(peso_min=25000.6, peso_max=30000.6), set())         # 30000,7 lido inteiro
        self.assertEqual(self.conteineres('efetiva', peso_max='6000'), {'C4'})  # texto com espaços; parâmetro texto

    def test_faixa_invertida_e_valor_invalido(self):
        self.assertEqual(self.prevista(peso_min=30000.7, peso_max=20000), {'C1', 'C2'})
        self.assertEqual(self.prevista(peso_min='abc'), {'C1', 'C2', 'C3', 'C6'})       # inválido: sem o filtro
        self.assertEqual(self.prevista(peso_min=-5), {'C1', 'C2', 'C3', 'C6'})

    def test_contagem_igual_aos_cards(self):
        self.assertEqual(conta_conteineres_navii(self.session, 'prevista', {**PERIODO, 'peso_min': 20000}), 2)

    def test_normalizacao(self):
        self.assertEqual(normaliza_filtros({'peso_min': '25000,5', 'peso_max': '-1'}), {'peso_min': 25000.5})
        self.assertEqual(normaliza_filtros({'peso_min': 30000, 'peso_max': '20000'}),
                         {'peso_min': 20000, 'peso_max': 30000})
        self.assertIsInstance(normaliza_filtros({'peso_min': '30000'})['peso_min'], int)   # inteiro fica inteiro
        for invalido in (True, 'nan', 'inf', None, ''):
            with self.subTest(valor=invalido):
                self.assertEqual(normaliza_filtros({'peso_max': invalido}), {})

    def test_filtro_salvo(self):
        self.assertEqual(filtros_salvos.limpa_filtros({'peso_min': '1000', 'peso_max': 2000.5, 'outro': 1}),
                         {'peso_min': 1000, 'peso_max': 2000.5})

    def test_api_e_tela(self):
        app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'))
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        app.register_blueprint(bp_navii)
        cliente = app.test_client()
        dados = cliente.get('/navii/api/ces', query_string={
            **PERIODO, 'tipo_janela': 'prevista', 'peso_min': '20000', 'peso_max': ''}).get_json()
        self.assertEqual(dados['total'], 2)
        self.assertEqual({c['numeroConteiner'] for c in dados['cards']}, {'C1', 'C2'})
        html = cliente.get('/navii/?tipo_janela=prevista&peso_min=1000,5').get_data(as_text=True)
        for trecho in ('id="f-peso_min"', 'id="f-peso_max"', "peso_min: 'Peso do contêiner ≥'",
                       '"filtros": {"peso_min": 1000.5}'):                 # aberta por link: a faixa da URL
            self.assertIn(trecho, html)
