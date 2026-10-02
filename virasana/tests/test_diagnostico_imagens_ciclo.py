"""Testes do DIAGNÓSTICO das imagens que faltam (diagnostico_imagens_ciclo.py): classificação no Mongo
simulado e o relatório inteiro sobre o cenário do serviço (test_servico_ciclo_de_vida)."""
import io
import unittest
from datetime import datetime, timedelta

import mongomock
from bson import ObjectId
from sqlalchemy import text

from virasana.analytic_scripts.ciclo_de_vida import diagnostico_imagens_ciclo as di
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
from virasana.tests.test_servico_ciclo_de_vida import AGORA, BTP, DESDE, Relogio, d, monta_banco

T = datetime(2026, 3, 5, 10)     # inspeções da BTP: imagem esperada 3 h depois


def imagem(db, conteiner, quando):
    db['fs.files'].insert_one({'_id': ObjectId(), 'metadata': {'numeroinformado': conteiner,
                                                               'dataescaneamento': quando}})


class TestClassifica(unittest.TestCase):

    def setUp(self):
        self.db = mongomock.MongoClient()['teste']
        imagem(self.db, 'AAAU1234567', T + timedelta(hours=3, minutes=5))
        imagem(self.db, 'BBBU1234567', T + timedelta(hours=1))
        imagem(self.db, 'CCCU1234567', T + timedelta(days=10))
        imagem(self.db, 'DDDU1234561', T + timedelta(hours=3, minutes=2))      # 1 dígito diferente
        imagem(self.db, 'ZZZU9999999', T + timedelta(days=5, hours=3))         # sozinho no horário

    def classifica(self, conteiner, quando=T):
        return di.classifica(self.db, conteiner, quando, BTP)

    def test_categorias(self):
        self.assertEqual(self.classifica('AAAU1234567')[0], di.EXISTE_ESPERADA)
        self.assertEqual(self.classifica('BBBU1234567'), (di.OUTRA_DEFASAGEM, self.classifica('BBBU1234567')[1]))
        self.assertTrue(self.classifica('BBBU1234567')[1].startswith('+1.0 h'))
        self.assertEqual(self.classifica('CCCU1234567'), (di.SO_LONGE, '1 imagem(ns) em ±30 dias'))
        self.assertEqual(self.classifica('DDDU1234567'), (di.PARECIDO, 'DDDU1234561'))
        self.assertEqual(self.classifica('EEEU1234567', T + timedelta(days=5)),
                         (di.SEM_ESTE, '1 outro(s) contêiner(es) no horário'))
        self.assertEqual(self.classifica('FFFU1234567', T + timedelta(days=20)), (di.NENHUMA, ''))

    def test_distancia(self):
        self.assertEqual(di.distancia('MSCU1234567', 'MSCU1234567'), 0)
        self.assertEqual(di.distancia('MSCU1234567', 'MSCU1234561'), 1)
        self.assertEqual(di.distancia('MSCU1234567', 'MSCU123456'), 1)
        self.assertEqual(di.distancia('MSCU1234567', 'TGHU7654321'), 9)   # o U e o 4 coincidem


class TestRelatorio(unittest.TestCase):
    """CTRA (BTP): a inspeção 301 tem imagem; a 302 (05/03 10h) não — a imagem do CTRA no Mongo é de 02/03."""

    def setUp(self):
        self.engine, self.mongo = monta_banco()
        sv.Servico(self.engine, self.mongo, relogio=Relogio(AGORA)).uma_volta(DESDE)
        with self.engine.begin() as c:
            c.execute(text('INSERT INTO apirecintos_inspecoesnaoinvasivas (id, numeroConteiner, dataHoraOcorrencia, '
                           "codigoRecinto, ocrNumero, contingencia, tipoOperacao, dataHoraTransmissao) VALUES "
                           "(302, 'CTRA', '2026-03-05 10:00:00', '8931359', 0, 0, 'I', '2026-03-05 12:00:00')"))
            c.execute(gc.ESCANEAMENTOS.insert().values(ciclo_de_vida_id=77, escaneamento_id=302, data_escaneamento=d(5, 10),
                                                       codigoRecinto=BTP, confianca='alta'))

    def test_relatorio(self):
        saida = io.StringIO()
        with self.engine.connect() as conexao:
            diag = di.DiagnosticoImagens(conexao, saida, d(1), d(10), [BTP], 10, self.mongo, AGORA)
            di.roda(diag)
        texto = saida.getvalue()
        self.assertNotIn('ERRO', texto)
        self.assertRegex(texto, r'2026-03-02 \| 0 / 1 \(0%\)')
        self.assertRegex(texto, r'2026-03-05 \| 1 / 1 \(100%\)')
        self.assertRegex(texto, r'8931359 \| sem\s+\| 1\s+\| 1 ')                 # sem OCR
        self.assertRegex(texto, r'8931359 \| 3\) imagem do contêiner só a mais de 48 h\s+\| 1\s+\| 2.0 h')
        self.assertIn('CTRA inspeção 302 em 05/03 10:00 (ciclo aberto; OCR não) — 1 imagem(ns) em ±30 dias', texto)

    def test_sem_mongo(self):
        saida = io.StringIO()
        with self.engine.connect() as conexao:
            di.roda(di.DiagnosticoImagens(conexao, saida, d(1), d(10), [BTP], 10, None, AGORA))
        self.assertIn('(desligado: --sem-mongo)', saida.getvalue())
