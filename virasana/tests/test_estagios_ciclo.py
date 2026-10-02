"""Testes dos ESTÁGIOS do ciclo de vida (estagios_ciclo.py): o critério SQL e a classificação em Python
precisam dar o mesmo resultado para cada combinação de situação, fluxo e datas — e as colunas do ORM têm que
bater com as tabelas gravadas pelo serviço (gravacao_ciclo)."""
import unittest
from datetime import datetime
from itertools import product

from virasana.analytic_scripts.ciclo_de_vida import estagios_ciclo as ec
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaCE, CicloDeVidaImportacao as Ciclo, CicloDeVidaImportacaoEscaneamentos,
)
from virasana.tests.test_navii_busca_sqlite import cria_sessao

DATA = datetime(2026, 3, 1)
SITUACOES = (None, ldt.ABERTO, ldt.CONCLUIDO, ldt.INCOMPLETO, ldt.ABANDONO)
FLUXOS = (None, ldt.OPERADOR, ldt.TRA, ldt.PASSAGEM, ldt.BALDEACAO, ldt.INDEFINIDO)


class TestEstagios(unittest.TestCase):

    def setUp(self):
        self.session = cria_sessao()
        for i, (situacao, fluxo, saida, entrada, concluido) in enumerate(
                product(SITUACOES, FLUXOS, (None, DATA), (None, DATA), (False, True))):
            self.session.add(Ciclo(ID=i + 1, numeroConteiner=f'C{i}', situacao=situacao, fluxo=fluxo,
                                   data_saida=saida, data_entrada_seco=entrada, concluido=concluido))
        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_sql_e_python_classificam_igual(self):
        ciclos = self.session.query(Ciclo).all()
        for codigo in ec.ESTAGIOS:
            with self.subTest(estagio=codigo):
                pelo_sql = {c.ID for c in self.session.query(Ciclo).filter(ec.criterio(codigo))}
                pelo_python = {c.ID for c in ciclos if ec.estagio_do_ciclo(c) == codigo}
                self.assertEqual(pelo_sql, pelo_python)
                self.assertTrue(pelo_sql)

    def test_cada_ciclo_em_no_maximo_um_estagio(self):
        vistos = []
        for codigo in ec.ESTAGIOS:
            vistos += [c.ID for c in self.session.query(Ciclo).filter(ec.criterio(codigo))]
        self.assertEqual(len(vistos), len(set(vistos)))

    def test_casos(self):
        def estagio(**campos):
            return ec.estagio_do_ciclo(Ciclo(**campos))
        self.assertEqual(estagio(situacao='aberto'), ec.NO_OPERADOR)
        self.assertEqual(estagio(situacao='aberto', data_saida=DATA), ec.EM_TRANSITO)
        self.assertEqual(estagio(situacao='aberto', data_saida=DATA, data_entrada_seco=DATA), ec.NO_SECO)
        self.assertEqual(estagio(situacao='concluido', fluxo='tra'), ec.ENTREGUE)
        self.assertEqual(estagio(situacao='concluido', fluxo='baldeacao'), ec.EMBARCADO)
        self.assertEqual(estagio(situacao='abandono', fluxo='operador'), ec.INCOMPLETO)
        self.assertEqual(estagio(concluido=True), ec.ENTREGUE)   # loop antigo
        self.assertIsNone(estagio(concluido=False))
        self.assertIsNone(ec.estagio_do_ciclo(None))
        with self.assertRaises(ValueError):
            ec.criterio('xyz')

    def test_lista_e_descricao(self):
        self.assertEqual([e['codigo'] for e in ec.lista()],
                         ['no_operador', 'em_transito', 'no_seco', 'entregue', 'embarcado', 'incompleto'])
        self.assertEqual(ec.descreve('entregue')['rotulo'], 'Entregue')
        self.assertIsNone(ec.descreve(None))


class TestOrmBateComAsTabelasDoServico(unittest.TestCase):
    """As telas leem pelo ORM (colunas v2 carregadas sob demanda); o serviço grava pelas tabelas Core de
    gravacao_ciclo, conferidas contra a DDL (test_servico_ciclo_de_vida)."""

    def test_mesmas_colunas(self):
        for orm, core in ((Ciclo, gc.CICLOS), (CicloDeVidaImportacaoEscaneamentos, gc.ESCANEAMENTOS),
                          (CicloDeVidaCE, gc.CICLO_CE)):
            with self.subTest(tabela=core.name):
                self.assertEqual(orm.__table__.name, core.name)
                self.assertEqual(set(orm.__table__.columns.keys()), set(core.columns.keys()))

    def test_colunas_v2_carregadas_sob_demanda(self):
        sessao = cria_sessao()
        sessao.add(Ciclo(ID=1, numeroConteiner='C1', situacao='aberto'))
        sessao.commit()
        sessao.expunge_all()
        ciclo = sessao.query(Ciclo).one()
        self.assertNotIn('situacao', ciclo.__dict__)        # a consulta das telas antigas não as seleciona
        self.assertEqual(ciclo.situacao, 'aberto')            # ... e elas vêm quando usadas
