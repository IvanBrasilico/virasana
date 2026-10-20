"""Contêiner do acesso pela placa (analytic_scripts/ciclo_de_vida/acesso_pela_placa.py) — regra pura do D3."""
import unittest
from datetime import datetime

from virasana.analytic_scripts.ciclo_de_vida import acesso_pela_placa as ap

ACESSO = datetime(2026, 10, 14, 10, 0)


def m(minutos, placa, conteiner, fonte=ap.PESAGEM, segundos=0):
    return ap.Marca(datetime(2026, 10, 14, 10, 0) + (datetime(2026, 1, 1, 0, minutos, segundos) - datetime(2026, 1, 1)),
                    placa, conteiner, fonte)


def antes(minutos, placa, conteiner, fonte=ap.PESAGEM):
    return ap.Marca(datetime(2026, 10, 14, 10, 0) - (datetime(2026, 1, 1, 0, minutos) - datetime(2026, 1, 1)),
                    placa, conteiner, fonte)


class TestAcessoPelaPlaca(unittest.TestCase):

    def deduz(self, marcas, placa='AAA1A11', janela=ap.JANELA_MINUTOS):
        return ap.conteiner_do_acesso(ACESSO, placa, ap.indexa(marcas), janela)

    def test_normaliza_placa(self):
        self.assertEqual(ap.normaliza_placa('abc-1d23 '), 'ABC1D23')
        self.assertEqual(ap.normaliza_placa(None), '')

    def test_indexa_so_com_placa_e_conteiner(self):
        indice = ap.indexa([m(1, 'aaa-1a11', 'CTR1'), m(1, '', 'CTR2'), m(1, 'BBB1B11', '')])
        self.assertEqual(list(indice), ['AAA1A11'])

    def test_pela_pesagem_pela_inspecao_e_pelas_duas(self):
        self.assertEqual(self.deduz([m(2, 'AAA1A11', 'CTR1')]), ('CTR1', ap.PELA_PESAGEM, 2.0))
        self.assertEqual(self.deduz([antes(3, 'AAA-1A11', 'CTR1', ap.INSPECAO)]), ('CTR1', ap.PELA_INSPECAO, 3.0))
        self.assertEqual(self.deduz([m(4, 'AAA1A11', 'CTR1'), antes(1, 'AAA1A11', 'CTR1', ap.INSPECAO)]),
                         ('CTR1', ap.PELAS_DUAS, 1.0))

    def test_janela_inclusiva_e_outra_placa(self):
        self.assertEqual(self.deduz([m(5, 'AAA1A11', 'CTR1')])[0], 'CTR1')
        self.assertEqual(self.deduz([m(5, 'AAA1A11', 'CTR1', segundos=1)]), (None, ap.NENHUM, None))
        self.assertEqual(self.deduz([m(5, 'AAA1A11', 'CTR1', segundos=1)], janela=10)[0], 'CTR1')
        self.assertEqual(self.deduz([m(1, 'BBB1B11', 'CTR1')]), (None, ap.NENHUM, None))
        self.assertEqual(self.deduz([antes(30, 'AAA1A11', 'CTR1')]), (None, ap.NENHUM, None))   # longe, antes

    def test_dois_conteineres_ambiguo(self):
        self.assertEqual(self.deduz([m(1, 'AAA1A11', 'CTR1'), m(2, 'AAA1A11', 'CTR2')]), (None, ap.AMBIGUO, 1.0))

    def test_acesso_sem_placa(self):
        self.assertEqual(self.deduz([m(1, 'AAA1A11', 'CTR1')], placa=' '), (None, ap.SEM_PLACA, None))


if __name__ == '__main__':
    unittest.main()
