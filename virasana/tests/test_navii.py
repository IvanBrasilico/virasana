"""Unit tests para a camada de dados do NAVI-I (virasana.navii.busca_navii)."""
import unittest
from datetime import datetime, timedelta

from virasana.navii.busca_navii import (
    JANELA_EFETIVA_MAX_DIAS, JANELA_EFETIVA_MIN_DIAS,
    JANELA_PREVISTA_MAX_DIAS, JANELA_PREVISTA_MIN_DIAS,
    _janela_efetiva, _janela_previsao,
)


class TestJanelasNavii(unittest.TestCase):
    """Garante que as janelas de atracação batem com a história de usuário do
    NAVI-I: previsão de 3 a 15 dias à frente, efetiva de 5 a 15 dias atrás."""

    def test_constantes_janela_prevista(self):
        self.assertEqual(JANELA_PREVISTA_MIN_DIAS, 3)
        self.assertEqual(JANELA_PREVISTA_MAX_DIAS, 15)

    def test_constantes_janela_efetiva(self):
        self.assertEqual(JANELA_EFETIVA_MIN_DIAS, 5)
        self.assertEqual(JANELA_EFETIVA_MAX_DIAS, 15)

    def test_janela_previsao_3_a_15_dias_a_frente(self):
        hoje = datetime.now()
        inicio_str, fim_str = _janela_previsao()
        inicio = datetime.strptime(inicio_str, '%Y-%m-%d')
        fim = datetime.strptime(fim_str, '%Y-%m-%d')
        self.assertEqual(inicio.date(), (hoje + timedelta(days=3)).date())
        self.assertEqual(fim.date(), (hoje + timedelta(days=15)).date())

    def test_janela_efetiva_5_a_15_dias_atras(self):
        hoje = datetime.now()
        inicio, fim = _janela_efetiva()
        self.assertAlmostEqual(inicio, hoje - timedelta(days=15), delta=timedelta(seconds=5))
        self.assertAlmostEqual(fim, hoje - timedelta(days=5), delta=timedelta(seconds=5))


if __name__ == '__main__':
    unittest.main()
