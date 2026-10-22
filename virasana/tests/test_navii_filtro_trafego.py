"""Filtro por TIPO DE TRÁFEGO do CE (Conhecimento.tipoTrafego) na aba de análise do NAVI-I (23/10/2026).

Cenário da busca (test_navii_busca_sqlite.monta_cenario: C1, C2, C3 e C6 de importação no manifesto M1, atracação
prevista no período) mais, no mesmo manifesto: CEXP exportação (7), CCAB cabotagem (3), CPAS passagem (9) e CNUL sem
tráfego informado. Nenhum tipo marcado = todos (menos a exportação, que o NAVI-I nunca mostra).
"""
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase, cria_ce  # 1º: ajusta o sys.path
import os  # noqa: I100

from flask import Flask

from virasana.navii import filtros_salvos, referencias
from virasana.navii.busca_navii import conta_conteineres_navii, normaliza_filtros
from virasana.navii.views_navii import bp_navii

IMPORTACAO = {'C1', 'C2', 'C3', 'C6'}


class TestFiltroTrafego(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        for numero, conteiner, trafego in (('CEX', 'CEXP', '7'), ('CCB', 'CCAB', '3'), ('CPS', 'CPAS', '9'),
                                           ('CNL', 'CNUL', None)):
            cria_ce(self.session, numero, 'M1', {conteiner: []}, tipo_trafego=trafego)
        self.session.commit()

    def prevista(self, **filtros):
        return self.conteineres('prevista', **filtros)

    def test_sem_filtro_todos_menos_a_exportacao(self):
        self.assertEqual(self.prevista(), IMPORTACAO | {'CCAB', 'CPAS', 'CNUL'})

    def test_um_ou_varios_tipos(self):
        self.assertEqual(self.prevista(trafego=['3']), {'CCAB'})
        self.assertEqual(self.prevista(trafego=['3', '9']), {'CCAB', 'CPAS'})
        self.assertEqual(self.prevista(trafego='5'), IMPORTACAO)       # sem tráfego informado: fora
        self.assertEqual(self.prevista(trafego='3; 9'), {'CCAB', 'CPAS'})   # texto separado, como vem da URL
        self.assertEqual(self.prevista(trafego=['7']), set())             # a exportação nunca entra

    def test_janela_efetiva_e_contagem(self):
        self.assertEqual(self.conteineres('efetiva', trafego=['3']), {'CCAB'})   # sem desembarque: pela previsão
        self.assertEqual(conta_conteineres_navii(self.session, 'prevista', {**PERIODO, 'trafego': ['3', '9']}), 2)

    def test_normalizacao_e_filtro_salvo(self):
        self.assertEqual(normaliza_filtros({'trafego': ' 3 ; 9,3 '}), {'trafego': ['3', '9']})
        self.assertEqual(normaliza_filtros({'trafego': ['x']}), {})
        self.assertEqual(filtros_salvos.limpa_filtros({'trafego': ['5'], 'outro': 1}), {'trafego': ['5']})

    def test_api_e_tela(self):
        app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'))
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        app.register_blueprint(bp_navii)
        cliente = app.test_client()
        dados = cliente.get('/navii/api/ces', query_string=[
            *PERIODO.items(), ('tipo_janela', 'prevista'), ('trafego', '3'), ('trafego', '9')]).get_json()
        self.assertEqual(dados['total'], 2)
        self.assertEqual({c['numeroConteiner'] for c in dados['cards']}, {'CCAB', 'CPAS'})
        self.assertEqual({c['tipoTrafego'] for c in dados['cards']}, {'Cabotagem', 'Passagem'})
        html = cliente.get('/navii/?tipo_janela=prevista&trafego=3').get_data(as_text=True)
        for trecho in ('id="f-trafego-5"', 'id="f-trafego-3"', 'id="f-trafego-9"', 'Importação — longo curso',
                       "trafego: 'Tráfego'", 'function marcaTrafegos(codigos)', '"filtros": {"trafego": ["3"]}'):
            self.assertIn(trecho, html)
        self.assertNotIn('id="f-trafego-7"', html)


class TestReferencia(NaviiSqliteTestCase):

    def test_tipos_trafego_na_ordem_da_tela(self):
        self.assertEqual(referencias.tipos_trafego(), [
            {'codigo': '5', 'nome': 'Importação', 'descricao': 'longo curso'},
            {'codigo': '3', 'nome': 'Cabotagem', 'descricao': 'entre portos brasileiros'},
            {'codigo': '9', 'nome': 'Passagem', 'descricao': 'carga de passagem pelo porto'}])

    def test_rotulo_nas_estatisticas(self):
        caminho = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates', 'navii_estatisticas.html')
        with open(caminho, encoding='utf-8') as arquivo:
            self.assertIn("trafego: 'Tráfego'", arquivo.read())
