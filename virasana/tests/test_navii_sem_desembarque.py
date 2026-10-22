"""Testes ADVERSARIAIS dos cards ainda SEM DESEMBARQUE (ticket dos usuários de 16/10/2026).

Na janela "Desembarque efetivo", o contêiner sem desembarque localizado pelo ciclo de vida entra pela atracação
prevista do navio. Regras:
  - o card diz que está sem desembarque pelo MESMO critério da janela (`_tem_desembarque_localizado`): sem ciclo,
    ciclo sem data de desembarque, e — com a ligação pela família gravada pelo ciclo — nunca o contrário da janela;
  - a data prevista mostrada é a da escala DENTRO do período (a que trouxe o card), não a maior do manifesto;
  - `previsaoAtracacao` diz se a previsão é futura, de hoje ou já vencida (navio previsto e nada de desembarque);
  - o filtro `so_com_desembarque` (caixa "Incluir os ainda sem desembarque" desmarcada) tira esses cards nas duas
    janelas; na efetiva, tira o ramo da atracação prevista da consulta (mais leve).

Cenário: o base de `test_navii_busca_sqlite` (C1 com desembarque em 11/01; C2 sem ciclo; C3 com desembarque em
25/02, fora do período; C6 com ciclo SEM data de desembarque — loop antigo) e mais:
  DM/K20: o ciclo foi gravado com o CE 'DF2' (fora do Mercante), com DM na família gravada pelo ciclo
          (`ciclodevida_ciclo_ce`) — a janela acha o desembarque pela família; o card não acha o ciclo pela família
          do Mercante. O selo NÃO pode dizer "sem desembarque".
  MX: manifesto com duas escalas em Santos — 10/01 (dentro do período) e 20/02 (fora).
"""
import unittest
from datetime import date, datetime
from unittest import mock

from flask import Flask
from sqlalchemy.dialects import mysql

from virasana.navii import busca_navii, filtros_salvos
from virasana.navii.busca_navii import (
    _previsao_atracacao, busca_ces_navii, busca_manifestos_navii, conta_conteineres_navii, monta_cards_navii,
    monta_query_navii,
)
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaCE, CicloDeVidaImportacao
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item, Manifesto
from virasana.navii.views_navii import bp_navii
from virasana.tests import test_navii_filtros_salvos as tela_filtros
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase, cria_ce

HOJE = date(2026, 1, 12)


def monta_cenario_sem_desembarque(session):
    session.add_all([
        Conhecimento(numeroCEmercante='DM', manifestoCE='M1', tipoBLConhecimento='11', tipoTrafego='5'),
        Item(numeroCEmercante='DM', codigoConteiner='K20'),
        Escala(numeroDaEscala='EX1', dataPrevistaAtracacao='2026-01-10', embarcacao='NAVIO X', portoDaEscala='BRSSZ'),
        Escala(numeroDaEscala='EX2', dataPrevistaAtracacao='2026-02-20', embarcacao='NAVIO X', portoDaEscala='BRSSZ'),
        EscalaManifesto(ID=11, manifesto='MX', escala='EX1'),
        EscalaManifesto(ID=12, manifesto='MX', escala='EX2'),
        Manifesto(numero='MX', portoCarregamento='CNSHA'),
    ])
    cria_ce(session, 'CEMX', 'MX', {'K21': []})
    ciclo = CicloDeVidaImportacao(numeroCEmercante='DF2', numeroConteiner='K20',
                                  data_desembarque=datetime(2026, 1, 12, 8), codigoRecintoDesembarque='8931356',
                                  concluido=False, situacao='aberto', fluxo='indefinido', versao_heuristica='v2')
    session.add(ciclo)
    session.flush()
    session.add_all([
        CicloDeVidaCE(ciclo_de_vida_id=ciclo.ID, numeroCEmercante='DF2', papel='filhote', principal=True),
        CicloDeVidaCE(ciclo_de_vida_id=ciclo.ID, numeroCEmercante='DM', papel='master', principal=False),
    ])
    session.commit()


class SemDesembarqueTestCase(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        monta_cenario_sem_desembarque(self.session)

    def cards(self, tipo_janela='efetiva', **filtros):
        """{(CE, contêiner): card} de uma página grande."""
        filtros = {**PERIODO, **filtros}
        resultados = busca_ces_navii(self.session, tipo_janela, filtros, limit=50)
        return {(c['numeroCEmercante'], c['numeroConteiner']): c
                for c in monta_cards_navii(self.session, resultados, hoje=HOJE)}

    def total(self, tipo_janela='efetiva', **filtros):
        return conta_conteineres_navii(self.session, tipo_janela, {**PERIODO, **filtros})


class TestSeloIgualAJanela(SemDesembarqueTestCase):

    def test_quem_entra_pela_previsao_e_exatamente_quem_tem_o_selo(self):
        """Propriedade: na efetiva, os cards que SAEM com `so_com_desembarque` são os que entraram pela previsão — e
        são exatamente os que têm o selo. Na prevista, o selo = não estar entre os com desembarque."""
        for janela in ('efetiva', 'prevista'):
            with self.subTest(janela=janela):
                todos = self.cards(janela)
                com = self.cards(janela, so_com_desembarque=True)
                self.assertTrue(set(com) <= set(todos))
                self.assertTrue(set(todos) - set(com), 'o cenário precisa ter cards sem desembarque')
                for chave, card in todos.items():
                    self.assertEqual(card['semDesembarque'], chave not in com, chave)
                self.assertEqual(self.total(janela) - self.total(janela, so_com_desembarque=True),
                                 sum(c['semDesembarque'] for c in todos.values()))

    def test_casos_conhecidos(self):
        cards = self.cards()
        self.assertFalse(cards[('CE1', 'C1')]['semDesembarque'])                 # desembarque em 11/01
        self.assertTrue(cards[('CE1', 'C2')]['semDesembarque'])                  # sem ciclo
        self.assertFalse(cards[('CE1', 'C2')]['cicloDeVidaLocalizado'])
        self.assertTrue(cards[('CE6', 'C6')]['semDesembarque'])                  # ciclo antigo SEM data
        self.assertTrue(cards[('CE6', 'C6')]['cicloDeVidaLocalizado'])
        self.assertNotIn(('CE2', 'C3'), cards)          # desembarque fora do período: não entra pela previsão
        self.assertFalse(self.cards('prevista')[('CE2', 'C3')]['semDesembarque'])   # ... e não é "sem desembarque"

    def test_familia_gravada_pelo_ciclo_e_a_da_janela(self):
        """A família do Mercante não liga DM a DF2, mas a gravada pelo ciclo liga: a janela acha o desembarque e o
        card acha o ciclo por ela (desde 23/10/2026, C7 — antes o card ficava sem o ciclo) e NÃO diz "sem
        desembarque" — o critério é o da janela."""
        card = self.cards()[('DM', 'K20')]
        self.assertEqual((card['cicloDeVidaLocalizado'], card['cicloPorCE']), (True, 'DF2'))
        self.assertFalse(card['semDesembarque'])
        self.assertIn(('DM', 'K20'), self.cards(so_com_desembarque=True))
        with mock.patch.object(busca_navii, 'LIGACAO_FAMILIA', False):    # sem a ligação, entra pela previsão
            sem_ligacao = self.cards()[('DM', 'K20')]
            self.assertEqual((sem_ligacao['semDesembarque'], sem_ligacao['cicloDeVidaLocalizado']), (True, False))
            self.assertNotIn(('DM', 'K20'), self.cards(so_com_desembarque=True))

    def test_pelo_conteiner(self):
        """Busca dirigida pelo contêiner: a janela vira condição por candidato e também respeita o filtro."""
        self.assertEqual(set(self.cards(conteiner=['C1', 'C2'])), {('CE1', 'C1'), ('CE1', 'C2')})
        self.assertEqual(set(self.cards(conteiner=['C1', 'C2'], so_com_desembarque=True)), {('CE1', 'C1')})
        self.assertEqual(set(self.cards('prevista', conteiner=['C1', 'C2'], so_com_desembarque=True)),
                         {('CE1', 'C1')})
        self.assertTrue(self.cards(conteiner=['C2'])[('CE1', 'C2')]['semDesembarque'])

    def test_so_com_desembarque_tira_o_ramo_da_previsao_da_consulta(self):
        def sql(**filtros):
            q = monta_query_navii(self.session, 'efetiva', {**PERIODO, **filtros})
            return str(q.statement.compile(dialect=mysql.dialect()))
        self.assertIn('escalasresumo', sql())
        self.assertNotIn('escalasresumo', sql(so_com_desembarque=True))


class TestDataPrevista(SemDesembarqueTestCase):

    def test_escala_dentro_do_periodo_vence_a_maior(self):
        self.assertEqual(busca_manifestos_navii(self.session, ['MX'])['MX']['dataPrevistaAtracacao'], '2026-02-20')
        dentro = busca_manifestos_navii(self.session, ['MX'], periodo=('2026-01-05', '2026-01-15'))
        self.assertEqual(dentro['MX']['dataPrevistaAtracacao'], '2026-01-10')
        fora = busca_manifestos_navii(self.session, ['MX'], periodo=('2026-03-01', '2026-03-10'))
        self.assertEqual(fora['MX']['dataPrevistaAtracacao'], '2026-02-20')       # nenhuma dentro: a maior
        card = self.cards('prevista')[('CEMX', 'K21')]
        self.assertEqual(card['dataPrevistaAtracacao'], '2026-01-10')
        self.assertEqual(card['previsaoAtracacao'], {'data': '2026-01-10', 'dias': -2, 'situacao': 'vencida'})

    def test_situacao_da_previsao(self):
        self.assertEqual(_previsao_atracacao('2026-01-13', HOJE), {'data': '2026-01-13', 'dias': 1, 'situacao': 'futura'})
        self.assertEqual(_previsao_atracacao('2026-01-12', HOJE)['situacao'], 'hoje')
        self.assertEqual(_previsao_atracacao('2026-01-11', HOJE), {'data': '2026-01-11', 'dias': -1,
                                                                   'situacao': 'vencida'})
        for invalida in (None, '', '10/01/2026', '2026-13-01', 'xx'):
            self.assertIsNone(_previsao_atracacao(invalida, HOJE), invalida)


class TestTelaEApi(SemDesembarqueTestCase):

    def setUp(self):
        super().setUp()
        app = Flask(__name__)
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        app.register_blueprint(bp_navii)
        self.client = app.test_client()

    def test_api(self):
        dados = self.client.get('/navii/api/ces', query_string={**PERIODO, 'limit': 50}).get_json()
        cards = {(c['numeroCEmercante'], c['numeroConteiner']): c for c in dados['cards']}
        self.assertTrue(cards[('CE1', 'C2')]['semDesembarque'])
        self.assertEqual(cards[('CE1', 'C2')]['previsaoAtracacao']['data'], '2026-01-10')
        so_com = self.client.get('/navii/api/ces', query_string={**PERIODO, 'so_com_desembarque': '1'}).get_json()
        self.assertEqual(so_com['total'], dados['total'] - sum(c['semDesembarque'] for c in dados['cards']))

    def test_filtro_salvo(self):
        col = filtros_salvos.colecao(tela_filtros.banco_mongo())
        salvo = filtros_salvos.salva(col, 'ana', 'só desembarcados', 'efetiva', {'so_com_desembarque': '1'})
        self.assertEqual(salvo['filtros'], {'so_com_desembarque': True})


class TestConteinerConsolidado(SemDesembarqueTestCase):
    """C7 (23/10/2026): dois BLs únicos do mesmo manifesto (BU1, BU2) com o mesmo contêiner K40; o ciclo foi gravado
    com o BU1 e a família gravada tem os dois. O card do BU2 acha o ciclo pela família gravada e não diz "sem
    desembarque" (o selo da página não usa o atalho do BL único). Limitação conhecida: nos filtros verificados por
    candidato (ex.: contêiner + "só com desembarque"), o atalho do BL único ainda deixa o BU2 de fora."""

    def setUp(self):
        super().setUp()
        self.session.add_all([
            Conhecimento(numeroCEmercante='BU1', manifestoCE='M1', tipoBLConhecimento='10', tipoTrafego='5'),
            Conhecimento(numeroCEmercante='BU2', manifestoCE='M1', tipoBLConhecimento='10', tipoTrafego='5'),
            Item(numeroCEmercante='BU1', codigoConteiner='K40'), Item(numeroCEmercante='BU2', codigoConteiner='K40')])
        ciclo = CicloDeVidaImportacao(numeroCEmercante='BU1', numeroConteiner='K40',
                                      data_desembarque=datetime(2026, 1, 11, 8), codigoRecintoDesembarque='8931356',
                                      concluido=False, situacao='aberto', fluxo='indefinido', versao_heuristica='v2')
        self.session.add(ciclo)
        self.session.flush()
        self.session.add_all([
            CicloDeVidaCE(ciclo_de_vida_id=ciclo.ID, numeroCEmercante='BU1', papel='simples', principal=True),
            CicloDeVidaCE(ciclo_de_vida_id=ciclo.ID, numeroCEmercante='BU2', papel='simples', principal=False)])
        self.session.commit()
        self.ciclo_id = ciclo.ID

    def test_card_do_outro_bl_acha_o_ciclo(self):
        cards = self.cards()
        for ce, por_ce in (('BU1', None), ('BU2', 'BU1')):
            with self.subTest(ce=ce):
                card = cards[(ce, 'K40')]
                self.assertEqual((card['ciclo_de_vida_id'], card['cicloPorCE'], card['semDesembarque']),
                                 (self.ciclo_id, por_ce, False))
        self.assertIn(('BU2', 'K40'), self.cards(so_com_desembarque=True))


class TestPagina(unittest.TestCase):

    def setUp(self):
        tela_filtros.TestApi.setUp(self)

    def test_caixa_incluir_sem_desembarque_marcada_por_padrao(self):
        html = self.client.get('/navii/', headers={'X-Usuario': 'ana'}).get_data(as_text=True)
        self.assertRegex(html, r'<input[^>]*id="f-incluir_sem_desembarque"[^>]*checked')


if __name__ == '__main__':
    unittest.main()
