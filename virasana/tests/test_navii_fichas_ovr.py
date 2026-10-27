"""Testes das Fichas OVR no NAVI-I e na completude (virasana/navii/fichas_ovr.py), em SQLite.

Cenário (sobre o do test_navii_busca_sqlite):
  ficha 1: CE1, repressão na importação, ATIVA
  ficha 2: CE1, encerrada (concluída) — aparece pelo CE
  ficha 3: FMA (abandono) 123/2026 do CE3, iniciada
  ficha 4: outro CE, ABERTA, com RVF do contêiner C4 — aparece no C4 pelo contêiner
  ficha 5: outro CE, ARQUIVADA, com RVF do contêiner C2 — não aparece (pelo contêiner, só abertas)
"""
import unittest
from datetime import datetime

from virasana.navii import fichas_ovr as fo
from virasana.navii.busca_navii import busca_ces_navii, monta_cards_navii
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase


def cria_fichas(session):
    session.execute(fo.tabela_ovrs.insert(), [
        dict(id=1, numero='77', ano='2026', tipooperacao=1, numeroCEmercante='CE1', fase=1,
             datahora=datetime(2026, 1, 12)),
        dict(id=2, numero='12', ano='2025', tipooperacao=3, numeroCEmercante='CE1', fase=3,
             datahora=datetime(2025, 6, 1)),
        dict(id=3, numero='123', ano='2026', tipooperacao=0, numeroCEmercante='CE3', fase=0,
             datahora=datetime(2026, 1, 13)),
        dict(id=4, numero='5', ano='2026', tipooperacao=5, numeroCEmercante='OUTRO', fase=0,
             datahora=datetime(2026, 1, 14)),
        dict(id=5, numero='6', ano='2024', tipooperacao=5, numeroCEmercante='VELHO', fase=4,
             datahora=datetime(2024, 1, 1)),
    ])
    session.execute(fo.tabela_rvfs.insert(), [dict(id=1, ovr_id=4, numerolote='C4'),
                                               dict(id=2, ovr_id=5, numerolote='C2')])
    session.commit()


class TestFichasOvr(NaviiSqliteTestCase):

    def setUp(self):
        super().setUp()
        cria_fichas(self.session)

    def test_por_ce_todas_e_por_conteiner_so_abertas(self):
        por_ce, por_conteiner = fo.busca_fichas(self.session, ['CE1', 'CE3', ''], ['C2', 'C4', None])
        self.assertEqual({ce: sorted(f[0] for f in fichas) for ce, fichas in por_ce.items()},
                         {'CE1': [1, 2], 'CE3': [3]})
        self.assertEqual({c: [f[0] for f in fichas] for c, fichas in por_conteiner.items()}, {'C4': [4]})

    def test_fichas_do_card_sem_repetir_abertas_primeiro(self):
        por_ce, por_conteiner = fo.busca_fichas(self.session, ['CE1', 'OUTRO'], ['C4'])
        fichas = fo.fichas_do('CE1', ' c4 ', por_ce, por_conteiner, familia={'CE1', 'OUTRO'})   # normaliza
        self.assertEqual([(f['id'], f['via'], f['aberta']) for f in fichas],
                         [(4, 'família', True), (1, 'CE', True), (2, 'CE', False)])
        self.assertEqual(fichas[1]['url'], '/bhadrasana2/ovr?id=1')
        self.assertEqual((fichas[1]['numero'], fichas[1]['fase'], fichas[1]['tipo']),
                         ('77/2026', 'Ativa', 'Repressão mediante análise de risco na importação'))

    def test_fma_e_abandono(self):
        por_ce, _ = fo.busca_fichas(self.session, ['CE3'])
        [fma] = fo.fichas_do('CE3', None, por_ce, {})
        self.assertEqual((fma['numero'], fma['abandono'], fma['tipo'], fma['fase']),
                         ('00123/2026', True, 'Mercadoria Abandonada', 'Iniciada'))
        self.assertEqual(fo.descreve(9, None, None, 99, 'X', 7, None, 'CE')['numero'], '9')   # sem número/tipo

    def test_card_do_navii(self):
        resultados = busca_ces_navii(self.session, 'efetiva', PERIODO, limit=50)
        cards = {c['numeroConteiner']: c for c in monta_cards_navii(self.session, resultados)}
        self.assertEqual([f['id'] for f in cards['C1']['fichasOvr']], [1, 2])
        self.assertEqual([f['id'] for f in cards['C2']['fichasOvr']], [1, 2])   # mesmo CE; a 5 (arquivada) não
        self.assertEqual([(f['id'], f['via']) for f in cards['C4']['fichasOvr']], [(4, 'contêiner'), (3, 'CE')])
        self.assertEqual(cards['C6']['fichasOvr'], [])


class TestColunasEListasBatemComOBhadrasana(unittest.TestCase):

    def test_colunas(self):
        from bhadrasana.models.ovr import OVR  # só as tabelas: não configura mappers
        from bhadrasana.models.rvf import RVF
        for leve, real in ((fo.tabela_ovrs, OVR.__table__), (fo.tabela_rvfs, RVF.__table__)):
            with self.subTest(tabela=real.name):
                self.assertEqual(leve.name, real.name)
                self.assertTrue(set(leve.columns.keys()) <= set(real.columns.keys()))

    def test_tipos_e_fases(self):
        from bhadrasana.models.ovr import faseOVR, tipoOperacao
        self.assertEqual(fo.TIPOS_OPERACAO, tipoOperacao)
        self.assertEqual(fo.FASES, faseOVR)
