"""Health check — HISTÓRICO e SERVIÇO (virasana/saude/historico.py e servico_saude.py, 23/10/2026; decisões do
usuário: serviço próprio no supervisor, rodada diária às 7 h e as pedidas pela tela, histórico de 180 dias).

mongomock; o "agora" é passado (relógio fixo); as verificações são simuladas (`rodar`).
"""
from virasana.navii.busca_navii import tabela_recintos  # noqa: F401 — 1º: ajusta o sys.path
import unittest  # noqa: I100
from datetime import datetime, timedelta

import mongomock

from virasana.saude import historico
from virasana.saude.servico_saude import COMPONENTE_GERAL, ServicoSaude
from virasana.saude.verificacao import ATENCAO, FALHA, OK, Contexto, Resultado

DIA = datetime(2026, 10, 23)
API = 'API Recintos (carga)'
RIO = 'embarques/desembarques: recinto 7921303 RIO_Mult_Rio_TII8 — 0 nas últimas 24 h, 3.051 nas mesmas 24 h'
RIO_DEPOIS = 'embarques/desembarques: recinto 7921303 RIO_Mult_Rio_TII8 — 0 nas últimas 24 h, 2.990 nas mesmas 24 h'
ICTSI = 'embarques/desembarques: recinto 7921302 RIO_ICTSI — 0 nas últimas 24 h, 550 nas mesmas 24 h'


def banco():
    return mongomock.MongoClient().db


def resultados(*detalhes, estado=ATENCAO):
    return [Resultado('Mercante (carga)', OK, 'em dia'), Resultado(API, estado, 'resumo', list(detalhes))]


class TestChave(unittest.TestCase):

    def test_numeros_que_variam_saem_e_codigos_ficam(self):
        self.assertEqual(historico.chave(RIO), historico.chave(RIO_DEPOIS))       # 3.051 x 2.990: o mesmo achado
        self.assertNotEqual(historico.chave(RIO), historico.chave(ICTSI))        # recintos diferentes
        self.assertEqual(historico.chave('recinto 3111401: só 278 (21%) dos escaneamentos'),
                         'recinto 3111401: só # (#%) dos escaneamentos')
        self.assertEqual(historico.chave('C4 desembarque em 2026-10-07 10:00:00 do CE 152605300195387'),
                         'C# desembarque em #-#-# #:#:# do CE 152605300195387')


class TestRodadas(unittest.TestCase):

    def setUp(self):
        self.db = banco()

    def grava(self, horas, *detalhes, estado=ATENCAO, origem='agenda'):
        inicio = DIA + timedelta(hours=horas)
        return historico.grava_rodada(self.db, resultados(*detalhes, estado=estado), inicio,
                                      inicio + timedelta(seconds=95), origem)

    def test_novo_e_desde(self):
        self.grava(7, RIO)                          # 1ª: tudo novo
        self.grava(31, RIO_DEPOIS, ICTSI)           # 2ª: RIO desde a 1ª (os números mudaram), ICTSI novo
        ultima = historico.painel(self.db, DIA + timedelta(hours=32), 5)['ultima']
        api = next(r for r in ultima['resultados'] if r['componente'] == API)
        self.assertEqual([(d['novo'], d['desde']) for d in api['detalhes']],
                         [(False, '2026-10-23T07:00:00'), (True, '2026-10-24T07:00:00')])
        self.assertEqual(api['estado_desde'], '2026-10-23T07:00:00')   # em ATENÇÃO desde a 1ª
        # o Mercante está OK desde a 1ª rodada (a tela só mostra o "desde" de quem não está OK)
        self.assertEqual(ultima['resultados'][0]['estado_desde'], '2026-10-23T07:00:00')
        primeira = historico.lista_rodadas(self.db)[-1]
        self.assertIsNone(historico.uma_rodada(self.db, primeira['id'])['resultados'][1]['estado_desde'])  # novo

    def test_sequencia_interrompida_recomeca(self):
        self.grava(7, RIO)
        self.grava(31, estado=OK)                   # sumiu numa rodada
        self.grava(55, RIO)                         # voltou: é novo de novo
        ultima = historico.painel(self.db, DIA + timedelta(hours=56), 5)['ultima']
        api = next(r for r in ultima['resultados'] if r['componente'] == API)
        self.assertTrue(api['detalhes'][0]['novo'])

    def test_painel_faixa_servico_e_pedido(self):
        for horas in (7, 31, 55):
            self.grava(horas, RIO)
        historico.batimento(self.db, DIA + timedelta(hours=56), proxima=DIA + timedelta(hours=79))
        historico.pede_rodada(self.db, '12345678901', DIA + timedelta(hours=56))
        painel = historico.painel(self.db, DIA + timedelta(hours=56, minutes=2), parado_minutos=5)
        self.assertEqual([r['estado'] for r in painel['faixas'][API]], [ATENCAO] * 3)
        self.assertEqual(painel['faixas'][API][0]['inicio'], '2026-10-23T07:00:00')   # da mais antiga à mais nova
        self.assertEqual((painel['servico']['parado'], painel['servico']['proxima']), (False, '2026-10-26T07:00:00'))
        self.assertEqual(painel['pedido']['usuario'], '12345678901')
        parado = historico.painel(self.db, DIA + timedelta(hours=57), parado_minutos=5)
        self.assertTrue(parado['servico']['parado'])

    def test_pedido_nao_empilha_e_lista_e_uma_rodada(self):
        primeiro = historico.pede_rodada(self.db, 'A', DIA)
        segundo = historico.pede_rodada(self.db, 'B', DIA + timedelta(minutes=1))
        self.assertEqual((segundo['_id'], segundo['usuario']), (primeiro['_id'], 'A'))
        ids = [str(self.grava(h, RIO)) for h in (7, 31, 55)]
        lista = historico.lista_rodadas(self.db, limite=2)
        self.assertEqual([r['id'] for r in lista], [ids[2], ids[1]])
        mais = historico.lista_rodadas(self.db, antes=datetime.fromisoformat(lista[-1]['inicio']), limite=2)
        self.assertEqual([r['id'] for r in mais], [ids[0]])
        self.assertEqual(historico.uma_rodada(self.db, ids[0])['geral'], ATENCAO)
        self.assertIsNone(historico.uma_rodada(self.db, 'nao-e-um-id'))

    def test_retencao_por_ttl(self):
        historico.colecoes(self.db)
        indices = self.db[historico.RODADAS].index_information()
        self.assertTrue(any(i.get('expireAfterSeconds') == historico.RETENCAO_DIAS * 86400 for i in indices.values()))


class Relogio:
    def __init__(self, agora):
        self.agora = agora

    def __call__(self):
        return self.agora


class TestServico(unittest.TestCase):

    def setUp(self):
        self.db = banco()
        self.relogio = Relogio(DIA + timedelta(hours=6, minutes=30))
        self.rodadas = []

        def rodar(contexto):
            self.rodadas.append(contexto.agora)
            return resultados(RIO)
        self.servico = ServicoSaude(lambda agora: Contexto(None, agora=agora), self.db, relogio=self.relogio,
                                    rodar=rodar)

    def test_rodada_diaria_as_7h_uma_vez(self):
        self.assertIsNone(self.servico.uma_volta())                       # 6h30: ainda não
        self.assertEqual(self.db[historico.SERVICO].find_one()['proxima'], DIA + timedelta(hours=7))
        self.relogio.agora = DIA + timedelta(hours=7, minutes=1)
        self.assertIsNotNone(self.servico.uma_volta())                    # 7h01: roda
        self.relogio.agora = DIA + timedelta(hours=12)
        self.assertIsNone(self.servico.uma_volta())                       # já rodou hoje
        self.assertEqual(self.db[historico.SERVICO].find_one()['proxima'], DIA + timedelta(days=1, hours=7))
        self.assertEqual(len(self.rodadas), 1)
        self.assertEqual(self.db[historico.RODADAS].find_one()['origem'], 'agenda')

    def test_servico_que_sobe_depois_das_7h_roda_na_hora(self):
        self.relogio.agora = DIA + timedelta(hours=15)
        self.assertIsNotNone(self.servico.uma_volta())

    def test_pedido_da_tela(self):
        historico.pede_rodada(self.db, '12345678901', self.relogio.agora)
        rodada = self.servico.uma_volta()                                 # 6h30, mas há pedido
        documento = self.db[historico.RODADAS].find_one({'_id': rodada})
        self.assertEqual((documento['origem'], documento['usuario']), ('tela', '12345678901'))
        pedido = self.db[historico.PEDIDOS].find_one()
        self.assertEqual((pedido['estado'], pedido['rodada']), (historico.FEITO, rodada))
        self.assertIsNone(self.db[historico.SERVICO].find_one()['rodando_desde'])
        self.assertIsNone(self.servico.uma_volta())                       # pedido atendido; 6h30: nada

    def test_rodada_que_quebra_vira_falha_no_historico(self):
        def quebra(agora):
            raise RuntimeError("Can't connect to MySQL server")
        self.servico.fabrica = quebra
        self.relogio.agora = DIA + timedelta(hours=7)
        with self.assertLogs('saude_navi', 'ERROR'):
            rodada = self.servico.uma_volta()
        documento = self.db[historico.RODADAS].find_one({'_id': rodada})
        self.assertEqual((documento['geral'], documento['resultados'][0]['componente']), (FALHA, COMPONENTE_GERAL))
        self.assertIn("Can't connect to MySQL server", documento['resultados'][0]['resumo'])


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
