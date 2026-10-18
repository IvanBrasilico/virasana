"""Testes da janela por ID no Mercante e do limite de tempo por consulta (navii/mercante_por_id.py).

IDs seguidos, como na inclusão do Mercante; a `create_date` é a da ÚLTIMA ALTERAÇÃO:
  ID 1 01/09/2025;  ID 2 criado em 09/2025 e ALTERADO em 15/10/2026;  ID 3 20/10/2025;  ID 4 10/05/2026;
  ID 5 01/07/2026;  ID 6 01/08/2026.
"""
import unittest
from datetime import datetime

from sqlalchemy import create_engine, func, select

from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: o busca_navii ajusta o sys.path
from virasana.integracao.mercante.mercantealchemy import Conhecimento  # noqa: I100
from virasana.navii import mercante_por_id as mpi

DATAS = {1: datetime(2025, 9, 1), 2: datetime(2026, 10, 15), 3: datetime(2025, 10, 20), 4: datetime(2026, 5, 10),
         5: datetime(2026, 7, 1), 6: datetime(2026, 8, 1)}


class TestJanelaPorId(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.session = cria_sessao()
        for id_, data in DATAS.items():
            cls.session.add(Conhecimento(ID=id_, numeroCEmercante=f'CE{id_}', create_date=data))
        cls.session.commit()

    def test_o_ce_antigo_alterado_nao_puxa_a_janela(self):
        # pela create_date, o "primeiro CE desde 01/11/2025" seria o ID 2 (alterado em 2026)
        self.assertEqual(self.session.execute(select(func.min(Conhecimento.ID)).where(
            Conhecimento.create_date >= datetime(2025, 11, 1))).scalar(), 2)
        self.assertEqual(mpi.id_na_data(self.session, datetime(2025, 11, 1), bloco=2), 4)
        self.assertEqual(mpi.id_na_data(self.session, datetime(2025, 11, 1), bloco=1), 2)   # sem o bloco, erra

    def test_fronteiras(self):
        self.assertEqual(mpi.id_na_data(self.session, datetime(2026, 7, 1), bloco=2), 5)    # a data exata entra
        self.assertEqual(mpi.id_na_data(self.session, datetime(2020, 1, 1), bloco=2), 1)    # antes de tudo
        self.assertEqual(mpi.id_na_data(self.session, datetime(2026, 8, 2), bloco=2), 7)    # nenhum: o último + 1
        self.assertEqual(mpi.id_na_data(self.session, datetime(2025, 11, 1), bloco=2, a_partir=5), 5)
        self.assertEqual(mpi.id_na_data(self.session, datetime(2026, 7, 1), bloco=2, limites=(1, 6)), 5)

    def test_criacao_do_id(self):
        self.assertEqual(mpi.criacao_do_id(self.session, 2, bloco=2), datetime(2025, 10, 20))  # o vizinho
        self.assertEqual(mpi.criacao_do_id(self.session, 6, bloco=2), datetime(2026, 8, 1))
        self.assertIsNone(mpi.criacao_do_id(self.session, 7, bloco=2))
        self.assertEqual(mpi.limites_dos_ids(self.session), (1, 6))

    def test_sem_ces(self):
        self.assertIsNone(mpi.id_na_data(cria_sessao(), datetime(2026, 1, 1)))


class TestLimiteDeTempo(unittest.TestCase):

    def test_so_no_mariadb(self):
        self.assertFalse(mpi.limita_tempo(create_engine('sqlite://'), 120))
        self.assertFalse(mpi.limita_tempo(create_engine('mysql+pymysql://u:s@127.0.0.1/x'), 0))
        self.assertTrue(mpi.limita_tempo(create_engine('mysql+pymysql://u:s@127.0.0.1/x'), 120))

    def test_ouvinte_fixa_o_limite_da_conexao(self):
        comandos = []

        class Cursor:
            def execute(self, sql, parametros):
                comandos.append((sql, parametros))

            def close(self):
                comandos.append('fechado')

        class Conexao:
            def cursor(self):
                return Cursor()

        mpi._ouvinte_limite(120)(Conexao(), None)
        self.assertEqual(comandos, [('SET SESSION max_statement_time = %s', (120.0,)), 'fechado'])


if __name__ == '__main__':
    unittest.main()
