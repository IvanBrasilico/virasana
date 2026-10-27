# coding: utf-8
"""Testes da ferramenta de EVENTOS de contêineres (eventos_conteiner.py) sobre o cenário do serviço do ciclo de vida:
CTRA desembarca na BTP (com pesos), é inspecionado, tem um agendamento de saída, sai e entra no EADI."""
import io
import os
import tempfile
import unittest
from datetime import datetime

from virasana.analytic_scripts.ciclo_de_vida import eventos_conteiner as ec
from virasana.tests.test_servico_ciclo_de_vida import AGORA, d, monta_banco


class TestEventosConteiner(unittest.TestCase):

    def setUp(self):
        self.engine, self.mongo = monta_banco()

    def mostra(self, mongo=None):
        saida = io.StringIO()
        with self.engine.connect() as conexao:
            ec.EventosConteiner(conexao, saida, AGORA, dias=30, mongo_db=mongo).mostra('CTRA', d(1))
        return saida.getvalue()

    def test_eventos_em_ordem_com_intervalo_e_ciclo_do_nucleo(self):
        texto = self.mostra()
        self.assertNotIn('ERRO', texto)
        self.assertIn('02/03 08:00             desembarque 8931359 BTP  balança 25.000 kg, tara 3.800 kg, manifesto '
                      '21.000 kg  CE 152600000000001', texto)
        self.assertIn('02/03 12:00     +4.0 h  inspecao    8931359 BTP  CE 152600000000002', texto)
        self.assertIn('03/03 08:00    +20.0 h  agend. saida 8931359 BTP', texto)
        self.assertIn('04/03 11:00     +3.0 h  entrada     8932000 EADI', texto)
        self.assertIn('>> núcleo atual: desembarque 02/03 08:00 em 8931359 BTP -> tra/aberto — no recinto seco', texto)
        self.assertIn('entrada no seco: 04/03 11:00 8932000 EADI [alta]', texto)
        self.assertNotIn('IMAGEM', texto)

    def test_imagens_do_mongo(self):
        self.assertIn('IMAGEM      Mongo 02/03 15:02 (−3 h = 12:02)', self.mostra(self.mongo))

    def test_conteineres_do_csv(self):
        with tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False, encoding='utf-8-sig') as arquivo:
            arquivo.write('conteiner;data_desembarque;alertas\nAAAU1;2026-09-20 10:00:00;operador 8931318 em papel '
                          'de TRA\nBBBU2;2026-09-21 10:00:00;\nCCCU3;2026-09-22 10:00:00;operador 8931318 em papel '
                          'de TRA\n')
        try:
            self.assertEqual(ec.conteineres_do_csv(arquivo.name, '8931318 em papel', 5),
                             [('AAAU1', datetime(2026, 9, 20, 10)), ('CCCU3', datetime(2026, 9, 22, 10))])
            self.assertEqual(len(ec.conteineres_do_csv(arquivo.name, '', 2)), 2)
        finally:
            os.remove(arquivo.name)


if __name__ == '__main__':
    unittest.main()
