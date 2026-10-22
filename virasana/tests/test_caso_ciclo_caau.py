"""Regressão do caso REAL CAAU5545018 (21/10/2026; CICLO_DE_VIDA.md, "Caso CAAU5545018").

Dados extraídos de produção por `caso_ciclo.py` (`dados/caso_ciclo_CAAU5545018.json`: só as colunas que o ciclo de
vida lê, sem CPF, nome ou transportador; placas trocadas por apelidos). Reprocessa com o serviço do ciclo de vida
(código atual) num SQLite em memória.

O CE 152605316468892 (importação EGDAM -> BRSSZ) tem os dois contêineres e é declarado no desembarque de cada um em
07/10/2026 na Santos Brasil (e na entrada, inspeção e saída do CLIA). O manifesto dele não tem escala ligada no
Mercante. O CE 072603341989805 é de cabotagem que SAI de Santos (BRSSZ -> BRSUA; escala de carregamento em Santos em
15/10) e também tem o CAAU5545018 nos itens.
  - FANU1174955: fica com o CE de importação (o único candidato que não é exportação).
  - CAAU5545018: até 22/10/2026 a cabotagem era a única família com escala na janela e vencia sem declaração nenhuma
    (confiança média), contra o CE declarado 4 vezes. Correção (porto do desembarque — `ces_do_ciclo`): a cabotagem
    tem a carga SAINDO de Santos e é descartada; o manifesto sem escala ligada fica com o início da operação
    (07/10) — o CE de importação vence com confiança ALTA (escala próxima e declarado).
"""
import json
import unittest
from pathlib import Path

from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import Saida, reprocessa

CASO = Path(__file__).parent / 'dados' / 'caso_ciclo_CAAU5545018.json'
IMPORTACAO, CABOTAGEM = '152605316468892', '072603341989805'
DESEMBARQUE_FANU, DESEMBARQUE_CAAU = 12212278, 12212834


class TestCasoCaau(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.caso = json.loads(CASO.read_text(encoding='utf-8'))
        cls.saida = Saida()
        cls.ciclos = reprocessa(cls.caso, cls.saida)

    def test_os_dois_desembarques_viram_ciclo(self):
        self.assertEqual({DESEMBARQUE_FANU, DESEMBARQUE_CAAU} - set(self.ciclos), set())
        self.assertEqual(self.ciclos[DESEMBARQUE_CAAU]['conteiner'], 'CAAU5545018')

    def test_fanu_fica_com_o_ce_de_importacao(self):
        self.assertEqual(self.ciclos[DESEMBARQUE_FANU]['principal'], IMPORTACAO)

    def test_caau_fica_com_o_ce_de_importacao_e_nao_com_a_cabotagem_que_sai_de_santos(self):
        ciclo = self.ciclos[DESEMBARQUE_CAAU]
        self.assertEqual((ciclo['principal'], ciclo['confianca']), (IMPORTACAO, 'alta'))
        self.assertNotIn(CABOTAGEM, ciclo['familia'])

    def test_explicacao_mostra_o_porto_e_o_descarte_da_cabotagem(self):
        texto = '\n'.join(self.saida.linhas)
        self.assertIn('porto do desembarque: BRSSZ', texto)
        self.assertIn(f'candidato {CABOTAGEM}: master None tráfego 3 origem BRSSZ -> destino BRSUA', texto)
        self.assertIn('1 CE(s) com a carga saindo de BRSSZ descartado(s)', texto)
        self.assertIn('desembarque 12212834 CAAU5545018: DIFERENTE — atual 152605316468892 (alta', texto)


if __name__ == '__main__':
    unittest.main()
