"""Diagnóstico do ciclo de vida fora de Santos (analytic_scripts/ciclo_de_vida/diagnostico_ciclo_outros_portos.py) —
D1, em SQLite e mongomock.

Agora = 20/10/2026 12h, 14 dias. Santos 8931356 (unidade '817800' no cadastro: sem o zero à esquerda), Rio 7921302
(operador, 0717600) e 7921999 (recinto não operador, 0717600).
  GSLU2053602 (caso do usuário): D 13/10 e E 15/10 em Santos; "E" 16/10 no Rio (CE 132605338073101, BRRIO) e duas
     inspeções cheias 18/10 05:00 no Rio — PARECE DESEMBARQUE; imagens 18/10 08:00 no Mongo (+3 h)
  EXPU1111111: entrada no gate do Rio 10/10, E 12/10 — embarque normal
  PASS2222222: D 08/10 e E 09/10 no Rio — passagem
  NADA3333333: E 11/10 no Rio, nada mais
  Acessos no Rio sem contêiner (2) e uma pesagem sem contêiner.
  CEs (ID pela criação): 1 (01/08, fora), 2 = 132605338073101 BRRIO (20/09) com GSLU e ciclo de Santos sem imagem,
  3 BRRIO (25/09) com NOIM4444444 (sem eventos), 4 BRSSZ (26/09: Santos, fora), 5 (15/10, fora). Manifesto M13 com
  escalas BRSSZ 13/10 e BRRIO 16/10 (a do destino é a referência).
"""
import io
import unittest
from datetime import datetime

import mongomock
from bson import ObjectId
from sqlalchemy import text

from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: ajusta o sys.path
from bhadrasana.models.apirecintos import (  # noqa: I100
    AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva, PesagemVeiculo,
)
from virasana.analytic_scripts.ciclo_de_vida import diagnostico_ciclo_outros_portos as dop
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaImportacao
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item

SANTOS, RIO, TRA_RIO = '8931356', '7921302', '7921999'
AGORA = datetime(2026, 10, 20, 12)
CE_RIO = '132605338073101'


def d(dia, hora=0, minuto=0, segundo=0, mes=10):
    return datetime(2026, mes, dia, hora, minuto, segundo)


def ed(id_, recinto, data, conteiner, tipo, ce=None):
    return {'id': id_, 'codigoRecinto': recinto, 'dataHoraOcorrencia': data, 'numeroConteiner': conteiner,
            'embarqueDesembarque': tipo, 'numeroConhecimento': ce, 'tipoOperacao': 'I'}


def monta(session):
    session.execute(text('ALTER TABLE ovr_recintos ADD COLUMN e_operador BOOLEAN'))
    session.execute(text('ALTER TABLE ovr_recintos ADD COLUMN cod_unidade VARCHAR(10)'))
    session.execute(text("INSERT INTO ovr_recintos (id, nome, cod_siscomex, e_operador, cod_unidade) VALUES "
                         f"(1, 'SANTOS BRASIL', '{SANTOS}', 1, '817800'), (2, 'MULTIRIO', '{RIO}', 1, '0717600'), "
                         f"(3, 'TRA RIO', '{TRA_RIO}', 0, '0717600')"))
    session.execute(EmbarqueDesembarque.__table__.insert(), [
        ed(1, SANTOS, d(13, 10), 'GSLU2053602', 'D'), ed(2, SANTOS, d(15, 10), 'GSLU2053602', 'E'),
        ed(3, RIO, d(16, 17, 56), 'GSLU2053602', 'E', CE_RIO), ed(4, RIO, d(12, 9), 'EXPU1111111', 'E'),
        ed(5, RIO, d(8, 9), 'PASS2222222', 'D'), ed(6, RIO, d(9, 9), 'PASS2222222', 'E'),
        ed(7, RIO, d(11, 9), 'NADA3333333', 'E')])
    session.execute(AcessoVeiculo.__table__.insert(), [
        {'id': 1, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(10, 9), 'numeroConteiner': 'EXPU1111111',
         'operacao': 'C', 'direcao': 'E', 'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(19, 9), 'numeroConteiner': '', 'placa': 'AAA1A11',
         'operacao': 'C', 'direcao': 'S', 'tipoOperacao': 'I'},
        {'id': 3, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(19, 10), 'numeroConteiner': None, 'placa': 'BBB1B11',
         'operacao': 'C', 'direcao': 'S', 'tipoOperacao': 'I'}])
    session.execute(InspecaoNaoInvasiva.__table__.insert(), [
        {'id': 1, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(18, 5), 'numeroConteiner': 'GSLU2053602',
         'vazio': False, 'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(18, 5, 0, 30), 'numeroConteiner': 'GSLU2053602',
         'vazio': False, 'tipoOperacao': 'I'}])
    session.execute(PesagemVeiculo.__table__.insert(), [
        {'id': 1, 'codigoRecinto': RIO, 'dataHoraOcorrencia': d(19, 9), 'dataHoraTransmissao': d(19, 9),
         'placa': 'AAA1A11', 'numeroConteiner': '', 'pesoBrutoBalanca': 15000, 'tipoOperacao': 'I'}])
    for id_, ce, criado, destino, conteiner in ((1, 'CE_VELHO', d(1, mes=8), 'BRRIO', 'VELH0000001'),
                                                (2, CE_RIO, d(20, mes=9), 'BRRIO', 'GSLU2053602'),
                                                (3, 'CE_RIO_2', d(25, mes=9), 'BRRIO', 'NOIM4444444'),
                                                (4, 'CE_SANTOS', d(26, mes=9), 'BRSSZ', 'SSZU5555555'),
                                                (5, 'CE_NOVO', d(15), 'BRRIO', 'NOVO0000005')):
        session.add(Conhecimento(ID=id_, numeroCEmercante=ce, create_date=criado, portoDestFinal=destino,
                                 tipoTrafego='5', manifestoCE='M13'))
        session.add(Item(ID=id_, numeroCEmercante=ce, codigoConteiner=conteiner))
    session.add_all([
        Escala(numeroDaEscala='E1', portoDaEscala='BRSSZ', dataPrevistaAtracacao='2026-10-13'),
        Escala(numeroDaEscala='E2', portoDaEscala='BRRIO', dataPrevistaAtracacao='2026-10-16'),
        EscalaManifesto(ID=1, manifesto='M13', escala='E1'), EscalaManifesto(ID=2, manifesto='M13', escala='E2'),
        CicloDeVidaImportacao(ID=1, numeroConteiner='GSLU2053602', numeroCEmercante=CE_RIO, desembarque_id=1,
                              data_desembarque=d(13, 10), codigoRecintoDesembarque=SANTOS)])
    session.commit()


class TestDiagnosticoOutrosPortos(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        session = cria_sessao()
        monta(session)
        mongo = mongomock.MongoClient()['virasana']
        mongo['fs.files'].insert_many([
            {'_id': ObjectId(), 'metadata': {'numeroinformado': 'GSLU2053602', 'dataescaneamento': d(18, 8, 0, 19)}},
            {'_id': ObjectId(), 'metadata': {'numeroinformado': 'GSLU2053602', 'dataescaneamento': d(18, 8, 0, 22)}}])
        saida = io.StringIO()
        cls.diag = dop.DiagnosticoOutrosPortos(session.connection(), saida, d(6, 12), AGORA, mongo, agora=AGORA)
        dop.roda(cls.diag)
        cls.log = saida.getvalue()

    def linha(self, *trechos):
        achadas = [linha for linha in self.log.splitlines() if all(t in linha for t in trechos)]
        self.assertTrue(achadas, f'nenhuma linha com {trechos}')
        return [c.strip() for c in achadas[0].split('|')]

    def test_sem_erro(self):
        self.assertNotIn('!! ERRO', self.log)

    def test_unidades(self):
        self.assertEqual(self.linha('0817800', '| sim')[1:3], ['1', '0'])     # '817800' normalizado
        self.assertEqual(self.linha('0717600', '| não')[1:3], ['1', '1'])

    def test_eventos_por_recinto(self):
        self.assertEqual(self.linha('7921302 MULTIRIO', '0717600')[3:],
                         ['1', '4', '3', '2 (67%)', '2', '0 (0%)', '0 (0%)', '1', '1 (100%)'])
        self.assertEqual(self.linha('SANTOS (0817800, todos)')[3:5], ['1', '1'])
        self.assertFalse([linha for linha in self.log.splitlines() if linha.startswith('8931356 SANTOS BRASIL')])

    def test_embarques(self):
        for classe in (dop.PARECE_DESEMBARQUE, dop.COM_ENTRADA, dop.COM_DESEMBARQUE, dop.SEM_EVENTOS):
            with self.subTest(classe=classe):
                self.assertEqual(self.linha('7921302', classe)[2], '1 (25%)')
        self.assertEqual(self.linha('tráfego 5')[1], '1')
        self.assertEqual(self.linha('7921302', 'BRRIO')[2], '1')
        self.assertIn('GSLU2053602: 13/10 10:00 desembarque 8931356; 15/10 10:00 embarque 8931356; 16/10 17:56 '
                      'embarque 7921302; 18/10 05:00 inspeção 7921302', self.log)

    def test_navii(self):
        self.assertEqual(self.linha('BRRIO', '| 2')[1:],
                         ['2', '1 (50%)', '0 (0%)', '1 (50%)', '0 (0%)', '1 (50%)', '0 (0%)'])
        self.assertNotIn('SSZU5555555', self.log)                           # destino Santos: fora
        self.assertIn(f'CE {CE_RIO} GSLU2053602 previsão 2026-10-16 — inspeção no 7921302 MULTIRIO em 18/10/2026 '
                      '05:00', self.log)
        self.assertIn('CE CE_RIO_2 NOIM4444444 previsão 2026-10-16', self.log)

    def test_defasagem(self):
        self.assertEqual(self.linha('7921302 MULTIRIO', '+3 h')[1:], ['2', '+3 h: 2 (100%)'])

    def test_classifica_embarque(self):
        e = d(16, 12)
        self.assertEqual(dop.classifica_embarque(e, RIO, [(d(15), 'entrada', RIO)]), dop.COM_ENTRADA)
        self.assertEqual(dop.classifica_embarque(e, RIO, [(d(15), 'entrada', SANTOS)]), dop.SEM_EVENTOS)  # outro
        self.assertEqual(dop.classifica_embarque(e, RIO, [(d(18), 'saída', RIO)]), dop.PARECE_DESEMBARQUE)
        self.assertEqual(dop.classifica_embarque(e, RIO, [(d(1, mes=9), 'desembarque', RIO)]), dop.SEM_EVENTOS)


if __name__ == '__main__':
    unittest.main()
