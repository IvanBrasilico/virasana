"""Testes do `--alterados` do gate (`qualidade_navii.py`, 22/10/2026): escolher só os testes afetados.

Num repositório de brinquedo (pasta temporária):
  base.py <- meio.py <- views.py (cita tela.html, que inclui parte.html); relativo.py importa base por import
  relativo; test_base importa base, test_meio importa meio, test_views importa views, test_relativo importa
  relativo, test_corpus lê a pasta dados/corpus com glob.
"""
import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

from coverage import CoverageData

import qualidade_navii as q

ARQUIVOS = {
    'virasana/pkg/__init__.py': '',
    'virasana/pkg/base.py': 'CONSTANTE = 1\n\n\ndef f():\n    return CONSTANTE\n',
    'virasana/pkg/meio.py': 'from virasana.pkg import base\n',
    'virasana/pkg/relativo.py': 'from .base import f\n',
    'virasana/pkg/views.py': "from virasana.pkg import meio\nTELA = 'tela.html'\n",
    'virasana/templates/tela.html': '{% include "parte.html" %}\n',
    'virasana/templates/parte.html': '<p>parte</p>\n',
    'virasana/tests/test_base.py': 'from virasana.pkg.base import CONSTANTE\n',
    'virasana/tests/test_meio.py': 'from virasana.pkg import meio\n',
    'virasana/tests/test_views.py': 'from virasana.pkg import views\n',
    'virasana/tests/test_relativo.py': 'from virasana.pkg import relativo\n',
    'virasana/tests/test_corpus.py': "CORPUS = 'virasana/tests/dados/corpus'\n",
    'virasana/tests/dados/corpus/caso1.json': '{}\n',
}
BASE, PARTE = 'virasana/pkg/base.py', 'virasana/templates/parte.html'
TESTES = [f'virasana/tests/test_{nome}.py' for nome in ('base', 'meio', 'views', 'relativo', 'corpus')]
T_BASE, T_MEIO, T_VIEWS, T_RELATIVO, T_CORPUS = TESTES
MODULOS = ['virasana/pkg']


class RepositorioDeBrinquedo(unittest.TestCase):
    def setUp(self):
        self.anterior = os.getcwd()
        self.raiz = tempfile.mkdtemp()
        for caminho, conteudo in ARQUIVOS.items():
            os.makedirs(os.path.join(self.raiz, os.path.dirname(caminho)), exist_ok=True)
            with open(os.path.join(self.raiz, caminho), 'w', encoding='utf-8') as arquivo:
                arquivo.write(conteudo)
        os.chdir(self.raiz)

    def tearDown(self):
        os.chdir(self.anterior)
        shutil.rmtree(self.raiz, ignore_errors=True)

    def selecao(self, mudancas, mapa=None):
        """(módulos, testes, parcial?, o que foi impresso) com as mudanças e o mapa dados."""
        saida = io.StringIO()
        with mock.patch.object(q, 'alterados', return_value=mudancas), \
                mock.patch.object(q, '_le_mapa', return_value=(mapa, '2026-10-22T10:00' if mapa else None)), \
                redirect_stdout(saida):
            modulos, testes, parcial = q.selecao(MODULOS, TESTES)
        return modulos, testes, parcial, saida.getvalue()


class TestSelecao(RepositorioDeBrinquedo):
    def test_sem_mapa_vale_quem_importa_direta_ou_indiretamente(self):
        modulos, testes, parcial, saida = self.selecao({BASE: False})
        self.assertEqual(testes, [T_BASE, T_MEIO, T_VIEWS, T_RELATIVO])  # relativo: pelo import relativo
        self.assertEqual(modulos, [BASE])
        self.assertTrue(parcial)
        self.assertIn('SEM mapa', saida)

    def test_com_mapa_vale_quem_executou_e_quem_importa_direto(self):
        """Pelo mapa: test_relativo executou f(); test_base só importa a constante (direto). test_meio e test_views
        só importam base de passagem — ficam de fora."""
        _, testes, _, saida = self.selecao({BASE: False}, mapa={BASE: [T_RELATIVO]})
        self.assertEqual(testes, [T_BASE, T_RELATIVO])
        self.assertIn('mapa da rodada inteira de 2026-10-22T10:00', saida)

    def test_arquivo_fora_do_mapa_vale_pelos_imports(self):
        _, testes, _, _ = self.selecao({BASE: False}, mapa={'virasana/pkg/meio.py': [T_MEIO]})
        self.assertEqual(testes, [T_BASE, T_MEIO, T_VIEWS, T_RELATIVO])

    def test_template_incluido_vale_pela_view_que_cita_o_template_que_o_inclui(self):
        modulos, testes, _, _ = self.selecao({PARTE: False})
        self.assertEqual(testes, [T_VIEWS])
        self.assertEqual(modulos, [])  # template: sem flake8/bandit/radon

    def test_dado_lido_com_glob_vale_pela_pasta(self):
        _, testes, _, _ = self.selecao({'virasana/tests/dados/corpus/caso1.json': False})
        self.assertEqual(testes, [T_CORPUS])

    def test_teste_alterado_roda_ele_mesmo(self):
        _, testes, _, _ = self.selecao({T_MEIO: False}, mapa={BASE: [T_RELATIVO]})
        self.assertEqual(testes, [T_MEIO])

    def test_mudanca_geral_ou_py_removido_vale_o_gate_inteiro(self):
        for mudancas in ({'tox.ini': False}, {'virasana/tests/conftest.py': False}, {'virasana/pkg/velho.py': True}):
            with self.subTest(mudancas=mudancas):
                modulos, testes, parcial, saida = self.selecao(mudancas)
                self.assertEqual((modulos, testes, parcial), (MODULOS, TESTES, False))
                self.assertIn('vale o gate inteiro', saida)

    def test_avisa_o_que_nenhum_teste_alcanca_e_o_teste_fora_da_lista(self):
        _, testes, _, saida = self.selecao({'README.md': False, 'virasana/tests/test_novo.py': False})
        self.assertEqual(testes, [])
        self.assertIn('Nenhum teste do gate alcança: README.md', saida)
        self.assertIn('fora da lista TESTES do gate (teste novo entra lá): virasana/tests/test_novo.py', saida)


class TestMapa(RepositorioDeBrinquedo):
    def test_grava_os_testes_que_executaram_cada_arquivo_sem_a_importacao(self):
        cobertura = CoverageData(basename=q.COBERTURA)
        cobertura.set_context('')  # a importação, na coleta
        cobertura.add_lines({os.path.abspath(BASE): [1, 4]})
        cobertura.set_context(f'{T_RELATIVO}::TestF::test_f|run')
        cobertura.add_lines({os.path.abspath(BASE): [5]})
        cobertura.set_context(f'{T_BASE}::TestC::test_c|run')
        cobertura.add_lines({os.path.abspath(T_BASE): [1]})
        del cobertura
        with redirect_stdout(io.StringIO()):
            q._grava_mapa()
        with open(q.MAPA, encoding='utf-8') as entrada:
            self.assertEqual(json.load(entrada)['arquivos'], {BASE: [T_RELATIVO], T_BASE: [T_BASE]})
        mapa, gravado = q._le_mapa()
        self.assertEqual(mapa[BASE], [T_RELATIVO])
        self.assertTrue(gravado.startswith('20'))

    def test_sem_mapa(self):
        self.assertEqual(q._le_mapa(), (None, None))

    def test_sem_cobertura_nao_apaga_o_mapa_anterior(self):
        with open(q.MAPA, 'w', encoding='utf-8') as saida:
            json.dump({'gravado': '2026-10-21T09:00', 'arquivos': {BASE: [T_BASE]}}, saida)
        with redirect_stdout(io.StringIO()):
            q._grava_mapa()  # sem .cobertura_gate
        self.assertEqual(q._le_mapa(), ({BASE: [T_BASE]}, '2026-10-21T09:00'))


class TestAlterados(unittest.TestCase):
    def test_le_o_status_e_o_diff_desde_um_commit(self):
        saidas = {
            'diff': ['M\tvirasana/a.py', 'D\tvirasana/b.py', 'R100\tvirasana/c.py\tvirasana/d.py'],
            'status': [' M virasana/e.py', '?? virasana/novo.py', 'R  virasana/f.py -> virasana/g.py',
                       ' D virasana/h.py', '?? "virasana/com espaço.py"'],
        }
        with mock.patch.object(q, '_git', side_effect=lambda comando, *_: saidas[comando]):
            self.assertEqual(q.alterados('ORIG_HEAD'), {
                'virasana/a.py': False, 'virasana/b.py': True, 'virasana/d.py': False, 'virasana/e.py': False,
                'virasana/novo.py': False, 'virasana/g.py': False, 'virasana/h.py': True,
                'virasana/com espaço.py': False})

    def test_sem_desde_so_o_status(self):
        with mock.patch.object(q, '_git', return_value=[' M virasana/e.py']) as git:
            self.assertEqual(q.alterados(), {'virasana/e.py': False})
        git.assert_called_once_with('status', '--porcelain', '--untracked-files=all')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
