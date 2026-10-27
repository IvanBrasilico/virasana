"""Testes da conferência da coleta de uso (analytic_scripts/diagnostico_coleta_uso.py), com pesquisas gravadas pela
própria coleta (navii/coleta_uso.py) no mongomock.

ANA, aba A1: p1 09:00 (link; NCM 8517; total 30; C1..C4) vê C1 (5 s, imagem) e C2 (3 s) e clica no extrato do
             C3 sem 1 s na tela (visto e examinado assim mesmo: quem clicou viu);
             p2 09:05 (card; + porto: mais critérios; total 10; C5, C1, C2 — o C1 em OUTRA posição) vê C1 de novo
             (4 s) e C5 (2 s, Abrir Ficha) e abre a Ficha 77 — última gravação 09:06;
             p3 10:00 (pausa > 30 min: outra sessão; só NCM: menos critérios; total 30) não vê nada.
ANA, aba A2: p5 09:02 (outra aba ao mesmo tempo: outra sessão; total 5; C6) não vê nada.
BETO, aba B1: p4 09:30 (prevista, sem período, total 0).
Fora do período: uma pesquisa da véspera.
"""
import io
import unittest
from datetime import datetime

import mongomock

from virasana.analytic_scripts import diagnostico_coleta_uso as dcu
from virasana.navii import coleta_uso

ANA, BETO = '52998224725', '11144477735'


def h(hora, minuto=0, dia=17):
    return datetime(2026, 10, dia, hora, minuto)


def cards(*conteineres):
    return [{'item_id': int(c[1:]), 'numeroConteiner': c, 'numeroCEmercante': 'CE1'} for c in conteineres]


def monta(col):
    def pesquisa(usuario, aba, quando, origem, filtros, total, conteineres, anterior=None, janela='efetiva',
                 periodo=('2026-10-01', '2026-10-15')):
        return coleta_uso.registra_pesquisa(col, usuario, '2', aba, anterior, origem, janela, periodo, filtros, total,
                                            {}, cards(*conteineres), agora=quando)
    p1 = pesquisa(ANA, 'aba-ana-0001', h(9), 'link', {'ncm': ['8517']}, 30, ('C1', 'C2', 'C3', 'C4'))
    coleta_uso.registra_vistos(col, p1, ANA, [{'item': 1, 's': 5, 'x': ['imagem']}, {'item': 2, 's': 3},
                                              {'item': 3, 's': 0, 'x': ['extrato']}], h(9, 4))
    pesquisa(ANA, 'aba-ana-0002', h(9, 2), 'tela', {}, 5, ('C6',))
    p2 = pesquisa(ANA, 'aba-ana-0001', h(9, 5), 'card', {'ncm': ['8517'], 'porto_origem': ['CNSHA']}, 10,
                  ('C5', 'C1', 'C2'), anterior=p1)
    coleta_uso.registra_vistos(col, p2, ANA, [{'item': 1, 's': 4}, {'item': 5, 's': 2, 'x': ['ficha']}], h(9, 6))
    coleta_uso.registra_acao(col, p2, ANA, {'t': 'ficha', 'ficha': 77, 'resultado': 'nova'}, agora=h(9, 6))
    pesquisa(ANA, 'aba-ana-0001', h(10), 'tela', {'ncm': ['8517']}, 30, ('C1', 'C2'), anterior=p2)
    pesquisa(BETO, 'aba-beto-0001', h(9, 30), 'tela', {}, 0, (), janela='prevista', periodo=(None, None))
    pesquisa(ANA, 'aba-ana-0000', h(18, dia=16), 'tela', {}, 5, ('C9',))


class TestDiagnosticoColeta(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        col = coleta_uso.colecao(mongomock.MongoClient()['virasana'])
        monta(col)
        saida = io.StringIO()
        cls.diag = dcu.DiagnosticoColeta(col, saida, h(0), h(23, 59))
        cls.diag.roda()
        cls.log = saida.getvalue()

    def celulas(self, inicio):
        linha = next(linha for linha in self.log.splitlines() if linha.startswith(inicio))
        return [c.strip() for c in linha.split('|')]

    def test_volume_e_sessoes(self):
        self.assertEqual(len(self.diag.docs), 5)                                   # sem a da véspera
        self.assertEqual(self.celulas('pesquisas  ')[1], '5')
        self.assertEqual(self.celulas('usuários')[1], '2')
        self.assertEqual(self.celulas('sessões de análise')[1], '4')                # a pausa de 10:00 e a 2ª aba
        self.assertEqual(self.celulas('pesquisas sem período')[1], '1')
        self.assertEqual(self.celulas('tela ')[1:], ['3', '60%'])

    def test_cards_distintos_tiram_a_repeticao_do_refinamento(self):
        self.assertEqual(self.celulas('enviados'), ['enviados', '10', '-'])
        self.assertEqual(self.celulas('vistos  '), ['vistos', '5', '4'])            # C1 visto em p1 e em p2
        self.assertEqual(self.celulas('examinados'), ['examinados', '3', '3'])
        self.assertIn('Vistos / enviados: 50%; repetição pelo refinamento: 1 card(s)', self.log)
        self.assertIn('Profundidade (vistos / total da pesquisa): mediana 5.0%', self.log)   # 10%, 20%, 0% e 0%
        self.assertIn('Segundos por card visto: mediana 3.5 s', self.log)          # só os com segundos
        self.assertEqual(self.celulas('imagem'), ['imagem', '1'])
        self.assertEqual(self.celulas('extrato'), ['extrato', '1'])

    def test_transicoes(self):
        self.assertEqual(self.celulas('mais critérios'), ['mais critérios', '1'])
        self.assertEqual(self.celulas('menos critérios'), ['menos critérios', '1'])

    def test_por_usuario_mascarado(self):
        # usuário | pesquisas | sessões | minutos | vistos | examinados | vistos/hora | Fichas
        self.assertEqual(self.celulas('529.'), ['529.***.***-25', '4', '3', '6', '4', '3', '40', '1'])
        self.assertEqual(self.celulas('111.'), ['111.***.***-35', '1', '1', '0', '0', '0', '-', '0'])
        self.assertNotIn(ANA, self.log)
        self.assertNotIn(BETO, self.log)

    def test_acoes(self):
        self.assertEqual(self.celulas('nova'), ['nova', '1'])


class TestAuxiliares(unittest.TestCase):

    def test_transicao(self):
        base = {'janela': 'efetiva', 'periodo': {'inicio': 'a', 'fim': 'b'}, 'filtros': {'ncm': ['1']}}
        self.assertEqual(dcu.transicao(base, dict(base)), 'repetida')
        self.assertEqual(dcu.transicao(base, {**base, 'filtros': {'ncm': ['2']}}), 'mudança de critérios')
        self.assertEqual(dcu.transicao(base, {**base, 'janela': 'prevista'}), 'outra janela ou período')
        self.assertEqual(dcu.quartis([]), '-')

    def test_sem_pesquisas(self):
        saida = io.StringIO()
        dcu.DiagnosticoColeta(coleta_uso.colecao(mongomock.MongoClient()['v']), saida, h(0), h(1)).roda()
        self.assertIn('0 pesquisas', saida.getvalue())


if __name__ == '__main__':
    unittest.main()
