"""Atracação prevista à vista no card e no extrato do CE (pedido do usuário, 21/10/2026): a data prevista da escala e o
PORTO dela (`busca_manifestos_navii`: `portoAtracacaoPrevista` e `escalas`), sempre — também com desembarque.

Cenário: o base de `test_navii_busca_sqlite` + o extrato (CE_EXTRATO no manifesto M1, escala E1 em Santos 10/01) e
uma segunda escala de M1, E9 em Rio Grande 20/01.
"""
from virasana.navii.busca_navii import busca_ces_navii, busca_manifestos_navii, monta_cards_navii  # põe o ajna_commons
from virasana.navii import extrato_navii  # noqa: I100 - depois da busca (caminho do ajna_commons)
from virasana.integracao.mercante.mercantealchemy import Escala, EscalaManifesto  # noqa: I100
from virasana.tests.test_navii_busca_sqlite import CE_EXTRATO, PERIODO, NaviiSqliteTestCase, monta_extrato_cenario

E1 = {'escala': 'E1', 'porto': 'BRSSZ', 'prevista': '2026-01-10', 'efetiva': None, 'embarcacao': 'NAVIO A'}
E9 = {'escala': 'E9', 'porto': 'BRRIG', 'prevista': '2026-01-20', 'efetiva': '2026-01-21', 'embarcacao': 'NAVIO A'}


def acrescenta_escala_rio_grande(session):
    session.add_all([
        Escala(numeroDaEscala='E9', dataPrevistaAtracacao='2026-01-20', dataEfetivaPrimeiraAtracacao='2026-01-21',
               embarcacao='NAVIO A', portoDaEscala='BRRIG'),
        EscalaManifesto(ID=9, manifesto='M1', escala='E9')])
    session.commit()


class TestAtracacaoPrevista(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        monta_extrato_cenario(self.session)
        acrescenta_escala_rio_grande(self.session)

    def test_porto_da_escala_escolhida_e_todas_as_escalas(self):
        dados = busca_manifestos_navii(self.session, ['M1'])['M1']
        self.assertEqual((dados['dataPrevistaAtracacao'], dados['portoAtracacaoPrevista']), ('2026-01-20', 'BRRIG'))
        self.assertEqual(dados['escalas'], [E1, E9])   # em ordem de data prevista

    def test_com_o_porto_so_as_escalas_dele(self):
        dados = busca_manifestos_navii(self.session, ['M1'], 'BRSSZ')['M1']
        self.assertEqual((dados['dataPrevistaAtracacao'], dados['portoAtracacaoPrevista']), ('2026-01-10', 'BRSSZ'))
        self.assertEqual(dados['escalas'], [E1])

    def test_no_periodo_vale_a_escala_de_dentro_com_o_porto_dela(self):
        dados = busca_manifestos_navii(self.session, ['M1'], periodo=('2026-01-05', '2026-01-15'))['M1']
        self.assertEqual((dados['dataPrevistaAtracacao'], dados['portoAtracacaoPrevista']), ('2026-01-10', 'BRSSZ'))

    def test_manifesto_sem_escala(self):
        dados = busca_manifestos_navii(self.session, ['M3'])['M3']
        self.assertEqual((dados['dataPrevistaAtracacao'], dados['portoAtracacaoPrevista'], dados['escalas']),
                         (None, None, []))

    def test_card_com_desembarque_tem_a_data_e_o_porto(self):
        resultados = busca_ces_navii(self.session, 'efetiva', {**PERIODO, 'conteiner': 'C1', 'porto_escala': 'BRSSZ'})
        card = monta_cards_navii(self.session, resultados)[0]
        self.assertFalse(card['semDesembarque'])
        self.assertEqual((card['dataPrevistaAtracacao'], card['portoAtracacaoPrevista']), ('2026-01-10', 'BRSSZ'))

    def test_extrato_traz_as_escalas_e_prefere_a_de_santos_no_cabecalho(self):
        extrato = extrato_navii.monta_extrato(self.session, CE_EXTRATO)
        self.assertEqual(extrato['ce']['escalas'], [E1, E9])
        self.assertEqual((extrato['ce']['dataPrevistaAtracacao'], extrato['ce']['portoAtracacaoPrevista']),
                         ('2026-01-10', 'BRSSZ'))   # a de Santos, embora a de Rio Grande seja mais tarde
        self.assertEqual(extrato['conteineres'][0]['portoAtracacaoPrevista'], 'BRRIG')   # card: sem porto, a maior

    def test_cabecalho_sem_escala_em_santos_fica_com_a_maior(self):
        manifesto = {'dataPrevistaAtracacao': '2026-01-20', 'portoAtracacaoPrevista': 'BRRIG',
                     'escalas': [{**E1, 'porto': 'BRPNG'}, E9]}
        self.assertEqual(extrato_navii.atracacao_do_cabecalho(manifesto), ('2026-01-20', 'BRRIG'))
        dois_em_santos = {'escalas': [E1, {**E9, 'porto': 'BRSSZ'}, {**E1, 'prevista': None}]}
        self.assertEqual(extrato_navii.atracacao_do_cabecalho(dois_em_santos), ('2026-01-20', 'BRSSZ'))
        self.assertEqual(extrato_navii.atracacao_do_cabecalho({}), (None, None))


class TestTelas(NaviiSqliteTestCase):

    def test_extrato_mostra_o_porto_e_a_tabela_de_escalas(self):
        from werkzeug.exceptions import NotFound
        from werkzeug.middleware.dispatcher import DispatcherMiddleware
        from werkzeug.test import Client
        from virasana.tests.test_prefixo_proxy import HOST_INTERNO, PREFIXO, cria_app
        app = cria_app()
        monta_extrato_cenario(app.config['db_session'])
        acrescenta_escala_rio_grande(app.config['db_session'])
        cliente = Client(DispatcherMiddleware(NotFound(), {PREFIXO: app}))
        html = cliente.get(f'{PREFIXO}/navii/extrato?CE={CE_EXTRATO}',
                           environ_base={'HTTP_HOST': HOST_INTERNO}).get_data(as_text=True)
        # o banco da app não tem a escala E1 (do cenário base): só a E9, de Rio Grande — o cabeçalho fica com ela
        for trecho in ('escala em BRRIG', 'Escalas do manifesto <span class="contagem">1</span>',
                       '<td class="mono">E9</td><td>BRRIG</td><td>20/01/2026</td>',
                       'O ciclo de vida usa a atracação prevista das escalas em Santos'):
            self.assertIn(trecho, html)

    def test_card_monta_o_campo(self):
        from virasana.tests.test_prefixo_proxy import cria_app
        html = cria_app().test_client().get('/navii/').get_data(as_text=True)
        self.assertIn('function montaAtracacaoPrevista(card)', html)
        self.assertIn('montaAtracacaoPrevista(card) +', html)
