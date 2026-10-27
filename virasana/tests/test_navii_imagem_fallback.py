"""Imagem do card pelo contêiner e pela data, sem ciclo de vida (navii/imagem_fallback.py — D2, 20/10/2026).

Caso do usuário: GSLU2053602 (CE 132605338073101, destino BRRIO): D 13/10 e E 15/10 em Santos (passagem), "E" 16/10
no Rio (7921302), duas inspeções cheias 18/10 05:00 no Rio e as imagens no Mongo às 08:00 (+3 h). Depois (para o
teste): saída 25/10, entrada VAZIO 28/10 (outra operação), inspeção 29/10 e imagem 30/10 que não podem entrar.
"""
import unittest
from datetime import datetime

import mongomock
from bson import ObjectId
from flask import Flask

from virasana.tests.test_navii_busca_sqlite import (  # 1º: ajusta o sys.path
    PERIODO, NaviiSqliteTestCase, cria_sessao,
)
from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100
from virasana.navii import imagem_fallback as fb
from virasana.navii.busca_navii import tabela_recintos
from virasana.navii.views_navii import bp_navii

RIO, SANTOS = '7921302', '8931356'
GSLU = 'GSLU2053602'
IMG_RIO_1, IMG_RIO_2, IMG_DEPOIS, IMG_VAZIO = (str(ObjectId()) for _ in range(4))


def ev(data, tipo, vazio=None):
    return fb.EventoDoConteiner(data, tipo, vazio)


def d(dia, hora=0, minuto=0, segundo=0, mes=10):
    return datetime(2026, mes, dia, hora, minuto, segundo)


class TestJanelaECorte(unittest.TestCase):

    def test_janela(self):
        self.assertEqual(fb.janela('2026-10-16'), (d(14), datetime(2026, 11, 15, 23, 59, 59)))
        self.assertEqual(fb.janela(d(16, 15)), fb.janela('2026-10-16'))
        self.assertIsNone(fb.janela(None))
        self.assertIsNone(fb.janela('sem data'))

    def test_embarque_antes_da_saida_nao_corta(self):
        inicio, fim = fb.janela('2026-10-16')
        eventos = [ev(d(13, 10), fb.DESEMBARQUE), ev(d(15, 10), fb.EMBARQUE), ev(d(16, 17), fb.EMBARQUE)]
        self.assertEqual(fb.corta(inicio, fim, eventos), fim)               # passagem em Santos e o "E" do Rio

    def test_depois_da_saida(self):
        inicio, fim = fb.janela('2026-10-16')
        saida = ev(d(25, 9), fb.SAIDA)
        self.assertEqual(fb.corta(inicio, fim, [saida, ev(d(28, 10), fb.ENTRADA, True)]), d(28, 10))   # vazio
        self.assertEqual(fb.corta(inicio, fim, [saida, ev(d(27, 10), fb.ENTRADA, False)]), fim)        # cheio: TRA
        self.assertEqual(fb.corta(inicio, fim, [saida, ev(d(29, 10), fb.EMBARQUE)]), d(29, 10))
        self.assertEqual(fb.corta(inicio, fim, [saida, ev(d(29, 10), fb.DESEMBARQUE)]), d(29, 10))
        self.assertEqual(fb.corta(inicio, fim, [ev(d(12, 9), fb.SAIDA), ev(d(28, 10), fb.ENTRADA, True)]), fim)
        # a saída antes do início da janela é de outra operação: não conta


class TestEscolhe(unittest.TestCase):
    INICIO, FIM = fb.janela('2026-10-16')

    def test_pela_inspecao_cheia(self):
        inspecoes = [fb.Inspecao(d(18, 5), RIO, False), fb.Inspecao(d(18, 5), RIO, False)]
        imagens = [(d(18, 8, 0, 19), IMG_RIO_1), (d(18, 8, 0, 22), IMG_RIO_2)]
        achado = fb.escolhe(self.INICIO, self.FIM, inspecoes, imagens, {RIO: 'MULTIRIO'})
        self.assertEqual(achado, {'imagens': [IMG_RIO_1], 'origem': fb.POR_INSPECAO, 'confianca': fb.MEDIA,
                                  'detalhe': 'inspeção no 7921302 MULTIRIO em 18/10/2026 05:00'})

    def test_so_imagem_sem_a_da_inspecao_vazia(self):
        inspecoes = [fb.Inspecao(d(20, 5), RIO, True)]                         # vazia: não é desta carga
        imagens = [(d(20, 8, 1), IMG_VAZIO), (d(18, 8, 0, 19), IMG_RIO_1), (d(18, 8, 0, 22), IMG_RIO_2)]
        achado = fb.escolhe(self.INICIO, self.FIM, inspecoes, imagens)
        self.assertEqual(achado['imagens'], [IMG_RIO_1, IMG_RIO_2])
        self.assertEqual((achado['origem'], achado['confianca']), (fb.SO_IMAGEM, fb.BAIXA))
        self.assertEqual(achado['detalhe'], 'imagem de 18/10/2026 08:00, sem inspeção transmitida')

    def test_fora_da_janela_nada(self):
        self.assertIsNone(fb.escolhe(self.INICIO, d(25), [fb.Inspecao(d(29, 10), RIO, False)],
                                     [(d(29, 13), IMG_DEPOIS), (d(26), IMG_DEPOIS)]))
        self.assertIsNone(fb.escolhe(self.INICIO, self.FIM, [], []))

    def test_defasagem_medida_do_recinto(self):
        # D1 (20/10/2026): no 7921302 a imagem fica a +3 h da inspeção; uma a +0 h não é dela (vira "só imagem")
        from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import defasagens_do
        self.assertEqual(defasagens_do(RIO), (3,))
        achado = fb.escolhe(self.INICIO, self.FIM, [fb.Inspecao(d(18, 8), RIO, False)], [(d(18, 8, 5), IMG_RIO_1)])
        self.assertEqual(achado['origem'], fb.SO_IMAGEM)

    def test_limite_de_imagens(self):
        imagens = [(d(18, h), str(ObjectId())) for h in range(12)]
        self.assertEqual(len(fb.escolhe(self.INICIO, self.FIM, [], imagens)['imagens']), fb.MAXIMO_IMAGENS)


def monta_eventos(session):
    session.execute(EmbarqueDesembarque.__table__.insert(), [
        {'id': 1, 'codigoRecinto': SANTOS, 'dataHoraOcorrencia': d(13, 10, 27), 'numeroConteiner': GSLU,
         'embarqueDesembarque': 'D', 'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': SANTOS, 'dataHoraOcorrencia': d(15, 10, 25), 'numeroConteiner': GSLU,
         'embarqueDesembarque': 'E', 'tipoOperacao': 'I'},
        {'id': 3, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(16, 17, 56), 'numeroConteiner': GSLU,
         'embarqueDesembarque': 'E', 'tipoOperacao': 'I'}])
    session.execute(AcessoVeiculo.__table__.insert(), [
        {'id': 1, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(25, 9), 'numeroConteiner': GSLU, 'operacao': 'C',
         'direcao': 'S', 'vazioConteiner': False, 'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': '7921999', 'dataHoraOcorrencia': d(28, 10), 'numeroConteiner': GSLU,
         'operacao': 'C', 'direcao': 'E', 'vazioConteiner': True, 'tipoOperacao': 'I'}])
    session.execute(InspecaoNaoInvasiva.__table__.insert(), [
        {'id': 1, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(18, 5), 'numeroConteiner': GSLU, 'vazio': False,
         'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(18, 5, 0, 30), 'numeroConteiner': GSLU,
         'vazio': False, 'tipoOperacao': 'I'},                               # chave única: contêiner + hora
        {'id': 3, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(29, 10), 'numeroConteiner': GSLU, 'vazio': False,
         'tipoOperacao': 'I'}])
    session.execute(tabela_recintos.insert(), [{'id': 9, 'nome': 'MULTIRIO', 'cod_siscomex': RIO}])
    session.commit()


def mongo_com_imagens(*imagens):
    db = mongomock.MongoClient()['virasana']
    db['fs.files'].insert_many([{'_id': ObjectId(id_), 'metadata': {'numeroinformado': conteiner,
                                                                    'dataescaneamento': data}}
                                for id_, conteiner, data in imagens])
    return db


class TestCompletaCards(unittest.TestCase):

    def setUp(self):
        self.session = cria_sessao()
        monta_eventos(self.session)
        self.mongo = mongo_com_imagens((IMG_RIO_1, GSLU, d(18, 8, 0, 19)), (IMG_RIO_2, GSLU, d(18, 8, 0, 22)),
                                       (IMG_DEPOIS, GSLU, d(29, 13)))

    def test_cards(self):
        sem_imagem = {'numeroConteiner': GSLU, 'dataPrevistaAtracacao': '2026-10-16', 'imagens_ids': []}
        com_imagem = {'numeroConteiner': GSLU, 'dataPrevistaAtracacao': '2026-10-16', 'imagens_ids': ['do ciclo']}
        pelo_desembarque = {'numeroConteiner': GSLU, 'imagens_ids': [],
                            'linhaDoTempo': {'desembarque': {'data': d(16, 17)}}}
        sem_referencia = {'numeroConteiner': GSLU, 'imagens_ids': []}
        ganharam = fb.completa_cards(self.session, self.mongo,
                                     [sem_imagem, com_imagem, pelo_desembarque, sem_referencia])
        self.assertEqual(ganharam, 2)
        self.assertEqual(sem_imagem['imagens_ids'], [IMG_RIO_1, IMG_RIO_2])   # uma por inspeção; a do dia 29 não
        self.assertEqual(sem_imagem['imagemFallback'], {'origem': fb.POR_INSPECAO, 'confianca': fb.MEDIA,
                                                        'detalhe': 'inspeção no 7921302 MULTIRIO em 18/10/2026 05:00'})
        self.assertEqual(com_imagem['imagens_ids'], ['do ciclo'])
        self.assertNotIn('imagemFallback', com_imagem)
        self.assertEqual(pelo_desembarque['imagens_ids'], [IMG_RIO_1, IMG_RIO_2])
        self.assertNotIn('imagemFallback', sem_referencia)

    def test_sem_mongo_nada(self):
        card = {'numeroConteiner': GSLU, 'dataPrevistaAtracacao': '2026-10-16', 'imagens_ids': []}
        self.assertEqual(fb.completa_cards(self.session, None, [card]), 0)
        self.assertEqual(card['imagens_ids'], [])


class TestNaApi(NaviiSqliteTestCase):
    """O card C2 (CE1, sem ciclo de vida, previsão 10/01/2026) ganha a imagem da inspeção de 12/01 no 8931356 (+3 h)."""

    def setUp(self):
        super().setUp()
        self.session.execute(InspecaoNaoInvasiva.__table__.insert(), [
            {'id': 50, 'codigoRecinto': SANTOS, 'dataHoraOcorrencia': datetime(2026, 1, 12, 8), 'numeroConteiner': 'C2',
             'vazio': False, 'tipoOperacao': 'I'}])
        self.session.commit()
        self.imagem = str(ObjectId())
        app = Flask(__name__)
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True,
                          mongodb=mongo_com_imagens((self.imagem, 'C2', datetime(2026, 1, 12, 11, 2))))
        app.register_blueprint(bp_navii)
        self.client = app.test_client()

    def test_card_sem_ciclo_ganha_a_imagem_com_a_origem(self):
        dados = self.client.get('/navii/api/ces', query_string={**PERIODO, 'tipo_janela': 'prevista'}).get_json()
        cards = {c['numeroConteiner']: c for c in dados['cards']}
        self.assertEqual(cards['C2']['imagens_ids'], [self.imagem])
        self.assertEqual(cards['C2']['imagemFallback']['detalhe'], 'inspeção no 8931356 BTP em 12/01/2026 08:00')
        self.assertNotIn('imagemFallback', cards['C1'])                     # C1: imagens do ciclo
        self.assertEqual(len(cards['C1']['imagens_ids']), 2)

    def test_falha_no_fallback_nao_derruba_a_tela(self):
        from unittest import mock
        with mock.patch.object(fb, 'completa_cards', side_effect=RuntimeError('Mongo fora')):
            resposta = self.client.get('/navii/api/ces', query_string={**PERIODO, 'tipo_janela': 'prevista'})
        self.assertEqual(resposta.status_code, 200)
        self.assertNotIn('imagemFallback', {c['numeroConteiner']: c for c in resposta.get_json()['cards']}['C2'])


if __name__ == '__main__':
    unittest.main()
