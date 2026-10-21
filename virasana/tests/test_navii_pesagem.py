"""Pesagem do contêiner no ciclo, no NAVI-I (pesagem_navii; P3, 20/10/2026) — filtros, card, filtro salvo, API e tela.

Cenário da busca (test_navii_busca_sqlite.monta_cenario) com a tabela das pesagens do ciclo:
  C1 (janela efetiva): principal "divergencia" +45% (razão 1,45) e uma outra pesagem no ciclo (dado bruto)
  C4 (janela efetiva): principal "coerente" −2% (0,98) e uma NÃO principal "divergencia" (não conta no filtro)
  C3 (janela prevista): principal "digitacao" +900% (razão 10)
  C2 (ciclo criado aqui, desembarque em 11/01): principal "moderada" −15% (0,85)
  C6 (sem desembarque; entra nas duas janelas pela atracação prevista): principal "divergencia" −95% (razão 0,05:
     ordem de grandeza para menos)
Janela efetiva: C1, C2, C4, C6. Prevista: C1, C2, C3, C6.
"""
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase, cria_ciclo  # 1º: sys.path
import os  # noqa: I100
from datetime import datetime

from flask import Flask

from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc
from virasana.analytic_scripts.ciclo_de_vida.gravacao_ciclo import PESAGENS
from virasana.navii import filtros_salvos, tabelas_opcionais
from virasana.navii.busca_navii import (
    CicloDeVidaImportacao, busca_ces_navii, conta_conteineres_navii, monta_cards_navii, normaliza_filtros,
)
from virasana.navii.views_navii import bp_navii


def linha(ciclo_id, pesagem_id, principal, diagnostico=None, pct=None, razao=None, operacao=pc.RETIRADA):
    medido = None if razao is None else round(20000 * razao, 2)
    return {'ciclo_de_vida_id': ciclo_id, 'pesagem_id': pesagem_id, 'data': datetime(2026, 1, 13, 8, pesagem_id),
            'codigoRecinto': '8931356', 'placa': 'AAA1A11', 'operacao': operacao, 'principal': principal,
            'peso_bruto': 40000, 'tara_conjunto': 15000, 'origem_tara_conjunto': pc.PAR, 'tara_conteiner': 2200,
            'origem_tara_conteiner': 'informada', 'peso_medido': medido, 'peso_declarado': 20000 if razao else None,
            'origem_declarado': 'item do CE' if razao else None, 'diferenca_pct': pct, 'razao': razao,
            'diferenca_kg': None if razao is None else medido - 20000, 'diagnostico': diagnostico,
            'classificacao': diagnostico, 'captura_automatica': True}


class PesagemTestCase(NaviiSqliteTestCase):
    criar_tabela = True

    def setUp(self):
        super().setUp()
        if not self.criar_tabela:
            return
        PESAGENS.create(self.session.get_bind())
        tabelas_opcionais.limpa_cache()
        cria_ciclo(self.session, 'CE1', 'C2', datetime(2026, 1, 11, 9, 0), '8931356')
        self.session.commit()
        ids = {c.numeroConteiner: c.ID for c in self.session.query(CicloDeVidaImportacao)}
        self.session.execute(PESAGENS.insert(), [
            linha(ids['C1'], 1, True, 'divergencia', 45, 1.45),
            linha(ids['C1'], 2, False, operacao=pc.OUTRA),
            linha(ids['C4'], 3, True, 'coerente', -2, 0.98),
            linha(ids['C4'], 4, False, 'divergencia', 60, 1.6, pc.CHEGADA_AO_SECO),
            linha(ids['C3'], 5, True, 'digitacao', 900, 10),
            linha(ids['C2'], 6, True, 'moderada', -15, 0.85),
            linha(ids['C6'], 7, True, 'divergencia', -95, 0.05)])
        self.session.commit()


class TestFiltrosPesagem(PesagemTestCase):

    def test_por_diagnostico_da_principal(self):
        self.assertEqual(self.conteineres(pesagem='fraude'), {'C1', 'C6'})
        self.assertEqual(self.conteineres(pesagem='coerente'), {'C4'})       # a divergência do C4 não é a principal
        self.assertEqual(self.conteineres(pesagem='comparada'), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres(pesagem='divergente'), {'C1', 'C2', 'C6'})       # a moderada entra
        self.assertEqual(self.conteineres('prevista', pesagem='digitacao'), {'C3'})
        self.assertEqual(self.conteineres('prevista', pesagem='divergente'), {'C1', 'C2', 'C3', 'C6'})

    def test_diferenca_minima_e_ordem_de_grandeza(self):
        self.assertEqual(self.conteineres(dif_pct_min=10), {'C1', 'C2', 'C6'})
        self.assertEqual(self.conteineres(dif_pct_min=2), {'C1', 'C2', 'C4', 'C6'})   # -2%: para menos também
        self.assertEqual(self.conteineres(dif_pct_min=2.5), {'C1', 'C2', 'C6'})
        self.assertEqual(self.conteineres(dif_pct_min=50, pesagem='fraude'), {'C6'})  # -95%
        self.assertEqual(self.conteineres('prevista', ordem_grandeza=True), {'C3', 'C6'})   # 10× e 1/20
        self.assertEqual(self.conteineres(ordem_grandeza=True), {'C6'})

    def test_contagem_igual_aos_cards(self):
        self.assertEqual(conta_conteineres_navii(self.session, 'efetiva', {**PERIODO, 'pesagem': 'comparada'}), 4)

    def test_normalizacao_e_filtro_salvo(self):
        self.assertEqual(normaliza_filtros({'pesagem': 'x', 'dif_pct_min': '12,5', 'ordem_grandeza': 'sim'}),
                         {'dif_pct_min': 12.5, 'ordem_grandeza': True})
        self.assertEqual(normaliza_filtros({'pesagem': 'fraude', 'dif_pct_min': '-1', 'ordem_grandeza': '0'}),
                         {'pesagem': 'fraude'})
        self.assertIsInstance(normaliza_filtros({'dif_pct_min': '20'})['dif_pct_min'], int)
        for invalido in (True, 'nan', 'inf', None, ''):
            with self.subTest(valor=invalido):
                self.assertEqual(normaliza_filtros({'dif_pct_min': invalido}), {})
        self.assertEqual(filtros_salvos.limpa_filtros({'pesagem': 'fraude', 'dif_pct_min': '20', 'ordem_grandeza': '1'}),
                         {'pesagem': 'fraude', 'dif_pct_min': 20, 'ordem_grandeza': True})

    def test_card(self):
        cards = {c['numeroConteiner']: c for c in monta_cards_navii(
            self.session, busca_ces_navii(self.session, 'efetiva', dict(PERIODO), limit=50))}
        c1 = cards['C1']['pesagem']
        self.assertEqual((c1['principal']['diagnostico'], c1['principal']['nomeRecinto']), ('divergencia', 'BTP'))
        self.assertEqual([p['operacao'] for p in c1['pesagens']], [pc.RETIRADA, pc.OUTRA])
        self.assertIsInstance(c1['principal']['pesoMedido'], float)
        self.assertEqual(cards['C4']['pesagem']['principal']['diferencaPct'], -2.0)
        prevista = {c['numeroConteiner']: c for c in monta_cards_navii(
            self.session, busca_ces_navii(self.session, 'prevista', dict(PERIODO), limit=50))}
        self.assertEqual(prevista['C2']['pesagem']['principal']['diagnostico'], 'moderada')

    def test_api_e_tela(self):
        cliente = _cliente(self.session)
        dados = cliente.get('/navii/api/ces', query_string={
            **PERIODO, 'tipo_janela': 'efetiva', 'pesagem': 'fraude', 'dif_pct_min': ''}).get_json()
        self.assertEqual(dados['total'], 2)
        self.assertEqual({c['numeroConteiner']: c['pesagem']['principal']['diagnostico'] for c in dados['cards']},
                         {'C1': 'divergencia', 'C6': 'divergencia'})
        dados = cliente.get('/navii/api/ces', query_string={**PERIODO, 'tipo_janela': 'efetiva',
                                                            'dif_pct_min': '2', 'ordem_grandeza': '1'}).get_json()
        self.assertEqual([c['numeroConteiner'] for c in dados['cards']], ['C6'])
        html = cliente.get('/navii/?tipo_janela=efetiva&pesagem=digitacao').get_data(as_text=True)
        for trecho in ('id="f-pesagem"', 'id="f-dif_pct_min"', 'id="f-ordem_grandeza"', "pesagem: 'Pesagem'",
                       'Possível fraude (divergência relevante)', 'function montaPesagem',
                       '"filtros": {"pesagem": "digitacao"}'):
            self.assertIn(trecho, html)


class TestSemTabela(PesagemTestCase):
    criar_tabela = False

    def test_filtro_avisa_e_card_sem_bloco(self):
        with self.assertRaisesRegex(ValueError, 'pesagem indisponíveis'):
            self.conteineres(pesagem='fraude')
        cards = monta_cards_navii(self.session, busca_ces_navii(self.session, 'efetiva', dict(PERIODO), limit=50))
        self.assertTrue(cards and all(card['pesagem'] is None for card in cards))
        resposta = _cliente(self.session).get('/navii/api/ces', query_string={
            **PERIODO, 'tipo_janela': 'efetiva', 'pesagem': 'fraude'})
        self.assertEqual(resposta.status_code, 400)


def _cliente(session):
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'))
    app.config.update(LOGIN_DISABLED=True, db_session=session, TESTING=True)
    app.register_blueprint(bp_navii)
    return app.test_client()
