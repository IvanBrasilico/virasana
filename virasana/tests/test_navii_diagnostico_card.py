"""Testes do diagnóstico "por que o card não aparece" (`virasana/navii/diagnostico_card.py`, 21/10/2026).

Cenário: o base de `test_navii_busca_sqlite` (período 05/01 a 15/01; C1/CE1 com desembarque em 11/01; C2/CE1 sem
ciclo, escala prevista em 10/01 em Santos; C3/CE2 com desembarque em 25/02, FORA do período) e mais:
  CX1/CEX: CE de exportação com desembarque no período;
  K30/BU: CE de BL único; o ciclo foi gravado com o CE 'OUTRO' e o BU só está na família gravada pelo ciclo.
"""
import json
import os
import tempfile
import unittest
from datetime import date, datetime

from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import CicloDeVidaCE, CicloDeVidaImportacao
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item
from virasana.navii import diagnostico_card
from virasana.navii.diagnostico_card import Cenario, Saida, avalia_cenario, cenarios, diagnostica
from virasana.tests.test_navii_busca_sqlite import PERIODO, NaviiSqliteTestCase, cria_ce, cria_ciclo

HOJE = date(2026, 3, 1)
CE_NUMERICO = '152600000000040'


class DiagnosticoCardTestCase(NaviiSqliteTestCase):

    def roda(self, conteiner, ces=(), periodo=PERIODO, extras=None, caminho_json=None):
        saida = Saida()
        resultado = diagnostica(self.session, conteiner, list(ces), saida, hoje=HOJE, periodo=periodo,
                                extras=extras, caminho_json=caminho_json)
        return resultado, '\n'.join(saida.linhas)

    def item_id(self, conteiner):
        return self.session.query(Item.ID).filter(Item.codigoConteiner == conteiner).scalar()

    @staticmethod
    def busca(resultado, comeco):
        """O resultado do cenário cuja descrição começa com `comeco`."""
        return next(v for k, v in resultado['buscas'].items() if k.startswith(comeco))


class TestMotivos(DiagnosticoCardTestCase):

    def test_desembarque_fora_do_periodo_entra_no_largo_e_pelo_ce(self):
        # o filtro `ce` guarda só os dígitos: o CE do cenário precisa ser numérico, como no Mercante
        cria_ce(self.session, CE_NUMERICO, 'M1', {'K40': []})
        cria_ciclo(self.session, CE_NUMERICO, 'K40', datetime(2026, 2, 25, 8))
        self.session.commit()
        resultado, texto = self.roda('K40', [CE_NUMERICO])
        item = self.item_id('K40')
        entra, motivo = self.busca(resultado, 'período informado, qualquer porto')[item]
        self.assertFalse(entra)
        self.assertIn('FORA do período 2026-01-05 a 2026-01-15', motivo)
        self.assertEqual(self.busca(resultado, 'período largo')[item], (True, 'entra pelo DESEMBARQUE no período'))
        self.assertEqual(self.busca(resultado, f'pelo CE {CE_NUMERICO}')[item],
                         (True, 'sem limite de data: a janela aceita'))
        self.assertIn('desembarque 2026-02-25 08:00:00', texto)

    def test_ce_que_nao_devolve_o_item_e_apontado(self):
        resultado, _ = self.roda('C3', ['CE2'])   # 'CE2' vira '2' na normalização: a busca pelo CE não acha
        self.assertEqual(self.busca(resultado, 'pelo CE CE2')[self.item_id('C3')],
                         (False, 'sem limite de data: a janela aceita; a busca não o devolve por outra combinação '
                                 'dos filtros'))

    def test_sem_ciclo_entra_pela_previsao_e_sai_com_outro_porto(self):
        resultado, _ = self.roda('C2', extras={'porto_escala': 'BRRIO'})
        item = self.item_id('C2')
        self.assertEqual(self.busca(resultado, 'período informado, qualquer porto')[item],
                         (True, 'sem desembarque localizado; entra pela atracação PREVISTA no período'))
        entra, motivo = self.busca(resultado, 'filtros informados')[item]
        self.assertFalse(entra)
        self.assertIn('em outro porto (filtro "Porto de atracação" = BRRIO)', motivo)

    def test_exportacao_nunca_vira_card(self):
        cria_ce(self.session, 'CEX', 'M1', {'CX1': []}, tipo_trafego='7')
        cria_ciclo(self.session, 'CEX', 'CX1', datetime(2026, 1, 11, 8))
        self.session.commit()
        resultado, texto = self.roda('CX1')
        entra, motivo = self.busca(resultado, 'período largo')[self.item_id('CX1')]
        self.assertFalse(entra)
        self.assertIn('EXPORTAÇÃO', motivo)
        self.assertIn('(EXPORTAÇÃO: nunca vira card)', texto)

    def test_filtro_que_tira_e_apontado(self):
        resultado, _ = self.roda('C1', extras={'ncm': '6109', 'produto': 'NOTEBOOK'})
        entra, motivo = self.busca(resultado, 'filtros informados')[self.item_id('C1')]
        self.assertFalse(entra)
        self.assertTrue(motivo.startswith('entra pelo DESEMBARQUE no período'))
        self.assertIn('o(s) filtro(s) ncm tira(m)', motivo)

    def test_so_com_desembarque_tira_o_que_entraria_pela_previsao(self):
        resultado, _ = self.roda('C2', extras={'so_com_desembarque': True})
        entra, motivo = self.busca(resultado, 'filtros informados')[self.item_id('C2')]
        self.assertFalse(entra)
        self.assertEqual(motivo, 'sem desembarque localizado, e o filtro "só com desembarque" tira')

    def test_conteiner_fora_do_mercante(self):
        resultado, texto = self.roda('ZZZU0000000')
        self.assertEqual(resultado['itens'], [])
        self.assertIn('NENHUM item com este número exato', texto)
        self.assertIn('NENHUM ciclo', texto)

    def test_erro_da_busca_vira_motivo(self):
        cenario = Cenario('pesagem sem tabela', 'efetiva', {'conteiner': ['C1'], **PERIODO, 'pesagem': 'fraude'})
        item = self.session.query(Item).filter(Item.codigoConteiner == 'C1').one()
        entra, motivo = avalia_cenario(self.session, cenario, [item])[item.ID]
        self.assertFalse(entra)
        self.assertIn('a busca responde com erro', motivo)


class TestBlUnicoPelaFamilia(DiagnosticoCardTestCase):

    def setUp(self):
        super().setUp()
        self.session.add_all([
            Conhecimento(numeroCEmercante='BU', manifestoCE='M3', tipoBLConhecimento='10', tipoTrafego='5'),
            Item(numeroCEmercante='BU', codigoConteiner='K30'),
        ])
        ciclo = CicloDeVidaImportacao(numeroCEmercante='OUTRO', numeroConteiner='K30',
                                      data_desembarque=datetime(2026, 1, 11, 8), versao_heuristica='v2')
        self.session.add(ciclo)
        self.session.flush()
        self.session.add_all([
            CicloDeVidaCE(ciclo_de_vida_id=ciclo.ID, numeroCEmercante='OUTRO', papel='master', principal=True),
            CicloDeVidaCE(ciclo_de_vida_id=ciclo.ID, numeroCEmercante='BU', papel='filhote', principal=False),
        ])
        self.session.commit()

    def test_aviso_e_motivo(self):
        resultado, texto = self.roda('K30')
        self.assertIn('ATENÇÃO: o CE é BL único (tipo 10) e só liga pela família gravada', texto)
        self.assertIn('família gravada: OUTRO (master, principal), BU (filhote)', texto)
        entra, motivo = self.busca(resultado, 'período informado, qualquer porto')[self.item_id('K30')]
        self.assertFalse(entra)
        self.assertIn('sem desembarque localizado; nenhuma escala do manifesto', motivo)


class TestCenarios(unittest.TestCase):

    def test_periodo_sugerido_e_o_da_tela(self):
        lista = cenarios('C1', ['CE1'], HOJE)
        self.assertEqual(lista[0].filtros, {'conteiner': ['C1'], 'data_inicio': '2026-02-14',
                                            'data_fim': '2026-02-24', 'porto_escala': ['BRSSZ']})
        self.assertEqual([c.janela for c in lista], ['efetiva', 'efetiva', 'efetiva', 'prevista', 'efetiva'])
        self.assertEqual(lista[-1].filtros, {'ce': ['CE1']})

    def test_filtros_informados_tem_o_conteiner_e_podem_trocar_o_periodo(self):
        extras = {'data_inicio': '2026-01-01', 'data_fim': '2026-01-31', 'conteiner': ['OUTRO'], 'ncm': '84'}
        cenario = cenarios('C1', [], HOJE, periodo=PERIODO, extras=extras, janela_extras='prevista')[-1]
        self.assertEqual(cenario.janela, 'prevista')
        self.assertEqual(cenario.filtros, {'data_inicio': '2026-01-01', 'data_fim': '2026-01-31',
                                           'conteiner': ['C1'], 'ncm': '84'})

    def test_argumentos(self):
        args = diagnostico_card._argumentos(['--conteiner', 'hmmu 547150-7', '--ce', '1526.0533.2273.790'])
        self.assertEqual(diagnostico_card._numero(args.conteiner), 'HMMU5471507')
        self.assertEqual(diagnostico_card._numero(args.ce[0], digitos=True), '152605332273790')
        with self.assertRaises(SystemExit):
            diagnostico_card._argumentos(['--conteiner', 'X', '--inicio', '2026-01-01'])


class TestCards(DiagnosticoCardTestCase):

    def test_card_gravado_como_a_api(self):
        with tempfile.TemporaryDirectory() as pasta:
            caminho = os.path.join(pasta, 'card.json')
            resultado, texto = self.roda('C3', caminho_json=caminho)
            with open(caminho, encoding='utf-8') as arquivo:
                cards = json.load(arquivo)
        self.assertEqual([c['item_id'] for c in cards], [self.item_id('C3')])
        self.assertEqual(cards[0]['linhaDoTempo']['desembarque']['data'], '2026-02-25 08:00:00')
        self.assertIn('cards gravados em', texto)
        self.assertEqual(len(resultado['cards']), 1)

    def test_falha_ao_montar_aparece_com_traceback(self):
        def quebra(*args, **kwargs):
            raise RuntimeError('quebrou no card')
        original = diagnostico_card.monta_cards_navii
        diagnostico_card.monta_cards_navii = quebra
        try:
            resultado, texto = self.roda('C1')
        finally:
            diagnostico_card.monta_cards_navii = original
        self.assertEqual(resultado['cards'], [])
        self.assertIn('FALHA ao montar ou serializar o card', texto)
        self.assertIn('RuntimeError: quebrou no card', texto)


if __name__ == '__main__':
    unittest.main()
