"""Regras puras das pesagens no Ciclo de Vida (analytic_scripts/ciclo_de_vida/pesagens_ciclo.py)."""
import unittest
from datetime import datetime, timedelta

from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc

T0 = datetime(2026, 10, 18, 10)


def pesagem(horas=0, placa='ABC1D23', semirreboque='', conteiner='', peso=15000.0, recinto='8931356'):
    return {'data': T0 + timedelta(hours=horas), 'placa': placa, 'semirreboque': semirreboque,
            'conteiner': conteiner, 'peso': peso, 'recinto': recinto}


class TestDivisao(unittest.TestCase):

    def test_maior_divisao_com_quase_todos(self):
        self.assertEqual(pc.divisao_da_balanca([25120, 25140, 18000, 30020]), 20)
        self.assertEqual(pc.divisao_da_balanca([25000, 30000, 41500.0]), 100 if 41500 % 100 == 0 else None)
        self.assertEqual(pc.divisao_da_balanca([25070, 30030]), 10)
        self.assertEqual(pc.divisao_da_balanca([35070, 25200]), 70)            # balança antiga de 70 kg
        self.assertEqual(pc.divisao_da_balanca([20 * n for n in range(1000, 1099)] + [25125]), 20)   # 99%: fica

    def test_sem_divisao(self):
        self.assertIsNone(pc.divisao_da_balanca([]))
        self.assertIsNone(pc.divisao_da_balanca([0, None]))
        self.assertIsNone(pc.divisao_da_balanca([1235.5, 2000]))               # fracionado
        self.assertIsNone(pc.divisao_pelas_contagens({100: 5}, 0))
        self.assertEqual(pc.divisao_pelas_contagens({100: 1, 50: 1, 20: 99, 10: 100}, 100), 20)

    def test_fora_da_divisao(self):
        self.assertTrue(pc.fora_da_divisao(1235, 20))
        self.assertFalse(pc.fora_da_divisao(1240, 20))
        self.assertFalse(pc.fora_da_divisao(1235, None))
        self.assertFalse(pc.fora_da_divisao(1235, 1))
        self.assertTrue(pc.fora_da_divisao(1235.5, 1))                         # a balança de 1 kg não mostra
        self.assertFalse(pc.fora_da_divisao(None, 20))


class TestTaraDoConteiner(unittest.TestCase):

    def test_informada_ou_tipica(self):
        self.assertEqual(pc.tara_do_conteiner('2200', '22G1'), (2200.0, 'informada'))
        self.assertEqual(pc.tara_do_conteiner('2.250,0'.replace('.', ''), '22G1'), (2250.0, 'informada'))
        self.assertEqual(pc.tara_do_conteiner('', '45G1'), (3800.0, 'típica do tamanho'))
        self.assertEqual(pc.tara_do_conteiner('99999', 'L5G1'), (4500.0, 'típica do tamanho'))   # implausível
        self.assertEqual(pc.tara_do_conteiner('abc', '22G1'), (2200.0, 'típica do tamanho'))
        self.assertEqual(pc.tara_do_conteiner(None, None), (None, 'sem tara'))


class TestDigitacao(unittest.TestCase):

    def test_ordem_de_grandeza(self):
        self.assertEqual(pc.padrao_digitacao(12350, 1235), 'ordem de grandeza (÷10)')   # exemplo do usuário
        self.assertEqual(pc.padrao_digitacao(2510, 25120), 'ordem de grandeza (×10)')
        self.assertEqual(pc.padrao_digitacao(251, 25140), 'ordem de grandeza (×100)')

    def test_digitos(self):
        self.assertEqual(pc.padrao_digitacao(25100, 21540), 'dígitos vizinhos trocados')   # 25100 -> 21500
        self.assertEqual(pc.padrao_digitacao(25100, 2100), pc.UM_DIGITO)               # sem o 5
        self.assertEqual(pc.padrao_digitacao(2100, 25100), pc.UM_DIGITO)               # do medido ao declarado

    def test_diferenca_real_nao_e_digitacao(self):
        self.assertIsNone(pc.padrao_digitacao(25100, 31000))
        self.assertIsNone(pc.padrao_digitacao(25100, 28100))      # um dígito TROCADO não conta
        self.assertIsNone(pc.padrao_digitacao(0, 1000))
        self.assertIsNone(pc.padrao_digitacao(25100, 25200))      # quase iguais: 25010 (trocados) não explica melhor


class TestClassifica(unittest.TestCase):

    def test_categorias(self):
        self.assertEqual(pc.classifica(25100, 25300)[0], pc.COERENTE)
        self.assertEqual(pc.classifica(25100, 25300), (pc.COERENTE, '+0.8%'))
        self.assertEqual(pc.classifica(12350, 1240)[0], pc.DIGITACAO)
        self.assertEqual(pc.classifica(10000, 20500, tipo_iso='22G1'), (pc.DOIS, 'medido 2.0× o declarado'))
        self.assertEqual(pc.classifica(10000, 20500, tipo_iso='45G1')[0], pc.DIVERGENCIA)   # 40': não é par
        self.assertEqual(pc.classifica(25100, 31000), (pc.DIVERGENCIA, '+24%'))
        self.assertEqual(pc.classifica(None, 31000)[0], pc.SEM_DADO)
        self.assertEqual(pc.classifica(25100, -5)[0], pc.SEM_DADO)

    def test_tolerancia_cresce_com_a_divisao(self):
        self.assertEqual(pc.tolerancia(1000), 300)
        self.assertEqual(pc.tolerancia(30000), 1500)
        self.assertEqual(pc.tolerancia(1000, 100), 500)


class TestParTara(unittest.TestCase):

    def test_mesmo_conjunto_vazio_mais_proximo(self):
        carregada = pesagem(conteiner='GSLU2053602', peso=40000, semirreboque='XYZ9A87')
        candidatas = [
            pesagem(horas=-3, peso=15100, semirreboque='XYZ9A87'),
            pesagem(horas=1, peso=15000, semirreboque='XYZ9A87'),               # a mais próxima
            pesagem(horas=0.5, peso=15050, semirreboque='OUT1A23'),             # outro semirreboque
            pesagem(horas=0.2, peso=15020, conteiner='TGHU1111111'),            # com contêiner
            pesagem(horas=0.1, peso=15010, placa='OUT9Z99'),                    # outra placa
            pesagem(horas=0.3, peso=15300, recinto='7921302'),                  # outro recinto
            pesagem(horas=0.4, peso=0),                                         # sem peso
        ]
        self.assertEqual(pc.pareia_tara(carregada, candidatas, horas=12)['peso'], 15000)
        self.assertIsNone(pc.pareia_tara(carregada, candidatas[:1], horas=2))   # longe demais

    def test_semirreboque_ausente_nao_impede(self):
        carregada = pesagem(conteiner='GSLU2053602', peso=40000)
        self.assertEqual(pc.pareia_tara(carregada, [pesagem(horas=1, semirreboque='XYZ9A87')], 12)['peso'], 15000)


if __name__ == '__main__':
    unittest.main()
