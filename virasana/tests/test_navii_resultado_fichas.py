"""Testes ADVERSARIAIS da F3 — Fichas e resultado no período (navii/resultado_fichas.py), com as tabelas REAIS do
Fichas no SQLite (as do teste do "Abrir Ficha") e a coleta de uso no mongomock.

Agora = 17/10/2026 12h. Setores 2 EQRIMP e 15 SACIT. Fichas de repressão (tipo 1):
  101 EQRIMP ANA 02/03 flag NAVI-I (e a ANA viu o CE antes); verificação +5 d; TG +40 d (R$ 1.000)
                                                              -> pelo card (não vira provável); 30 d e 180 d ok
  102 EQRIMP ANA 03/03; ANA viu o CE 10 dias antes; verificação +45 d; arquivada  -> provável; verificação tardia
  103 EQRIMP BETO 04/03, flag "Operação X", concluída; a ANA viu o CE antes e o BETO só DEPOIS de abrir;
      perdimento +200 d (R$ 5.000)                             -> demais; resultado depois de 180 d; encerrada COM
  104 EQRIMP ANA 05/03; ANA viu o CE 32 dias antes (dentro da busca, fora da janela da Ficha); numa pesquisa de
      25/02 o card do CE foi ENVIADO mas não visto; verificação EXCLUÍDA          -> demais; sem verificação
  105 SACIT 06/03 arquivada                                                       -> demais
  109 EQRIMP sem datahora, create_date 08/03; Crédito +190 d (R$ 300)            -> entra pelo create_date; o crédito
                                                                                     não é perdimento
  110 EQRIMP 31/03 23:59:59 (entra; evento especial 1 — não é verificação); 111 em 01/04 00:00 (fora);
      106 FMA (tipo 0, fora); 108 em fev (fora)
  107 EQRIMP ANA 10/10 flag NAVI-I                                                -> mês em curso: em maturação
"""
import io
import unittest
from datetime import date, datetime, timedelta

import mongomock
from openpyxl import load_workbook

from virasana.navii import coleta_uso, estatisticas_uso, resultado_fichas as rf, tabelas_opcionais
from virasana.tests.test_navii_busca_sqlite import cria_sessao
from virasana.tests.test_navii_ficha_card import ANALISTA, cria_sessao_com_fichas

from bhadrasana.models import Setor, Usuario  # noqa: I100
from bhadrasana.models.ovr import EventoOVR, Flag, OVR, ResultadoOVR, TGOVR, TipoEventoOVR
from bhadrasana.models.rvf import metadata as metadata_fichas

AGORA = datetime(2026, 10, 17, 12)
ANA, BETO, SAC = ANALISTA, '22222222222', '33333333333'
CE = '1526000000001{:02d}'


def novo(classe, **campos):
    objeto = classe()
    for nome, valor in campos.items():
        setattr(objeto, nome, valor)
    return objeto


def d(dia, mes=3, hora=10):
    return datetime(2026, mes, dia, hora)


def monta(session, col):
    session.add_all([novo(Setor, id='15', nome='SACIT'), novo(Usuario, cpf=BETO, nome='Beto', setor_id='2'),
                     novo(Usuario, cpf=SAC, nome='Sac', setor_id='15')])
    especial = {t.eventoespecial: t.id for t in session.query(TipoEventoOVR) if t.eventoespecial}
    flag, operacao = novo(Flag, nome='NAVI-I'), novo(Flag, nome='Operação X')
    session.add_all([flag, operacao])
    for id_, setor, usuario, quando, campos in (
            (101, '2', ANA, d(2), {}), (102, '2', ANA, d(3), {'fase': 4}), (103, '2', BETO, d(4), {'fase': 3}),
            (104, '2', ANA, d(5), {}), (105, '15', SAC, d(6), {'fase': 4}), (106, '2', ANA, d(7), {'tipooperacao': 0}),
            (107, '2', ANA, d(10, 10), {}), (108, '2', BETO, d(10, 2), {}),
            (109, '2', BETO, None, {'create_date': d(8)}), (110, '2', BETO, datetime(2026, 3, 31, 23, 59, 59), {}),
            (111, '2', BETO, datetime(2026, 4, 1), {})):
        session.add(novo(OVR, id=id_, tipooperacao=campos.pop('tipooperacao', 1), fase=campos.pop('fase', 1),
                         setor_id=setor, user_name=usuario, numeroCEmercante=CE.format(id_ - 100), datahora=quando,
                         **campos))
    session.flush()
    guardados = {id_: session.get(OVR, id_) for id_ in (101, 103, 107)}   # referências até o commit (identity map)
    for id_ in (101, 107):
        guardados[id_].flags.append(flag)
    guardados[103].flags.append(operacao)
    for ovr_id, tipo, quando, excluido in (
            (101, 2, d(2) + timedelta(days=5), False), (102, 2, d(3) + timedelta(days=45), False),
            (104, 2, d(5) + timedelta(days=3), True), (110, 1, d(1, 4), False)):
        session.add(novo(EventoOVR, ovr_id=ovr_id, tipoevento_id=especial[tipo], motivo='evento', create_date=quando,
                         excluido=excluido))
    session.add(novo(TGOVR, ovr_id=101, numerolote='C1', descricao='mercadoria', valor=1000,
                     create_date=d(2) + timedelta(days=40)))
    session.add_all([
        novo(ResultadoOVR, ovr_id=103, tipo_resultado=2, valor=5000, create_date=d(4) + timedelta(days=200)),
        novo(ResultadoOVR, ovr_id=109, tipo_resultado=3, valor=300, create_date=d(8) + timedelta(days=190))])
    session.commit()
    for usuario, quando, item, ce, segundos, cliques in (
            (ANA, d(20, 2), 2, CE.format(2), 3, ['imagem']), (ANA, d(1), 3, CE.format(3), 3, ['imagem']),
            (ANA, d(1, 2), 4, CE.format(4), 3, ['imagem']), (ANA, d(28, 2), 1, CE.format(1), 3, []),
            (BETO, d(10), 33, CE.format(3), 3, []), (ANA, d(25, 2), 54, CE.format(4), None, None)):
        cards = [{'item_id': item, 'numeroConteiner': f'C{item}', 'numeroCEmercante': ce}]
        id_ = coleta_uso.registra_pesquisa(col, usuario, '2', f'aba-{usuario}', None, 'tela', 'efetiva', (None, None),
                                           {}, 1, {}, cards, agora=quando)
        if segundos:                                                     # sem segundos: o card foi só ENVIADO
            coleta_uso.registra_vistos(col, id_, usuario, [{'item': item, 's': segundos, 'x': cliques}], quando)


class CenarioTestCase(unittest.TestCase):

    def setUp(self):
        tabelas_opcionais.limpa_cache()
        self.session = cria_sessao_com_fichas()
        self.db = mongomock.MongoClient()['virasana']
        self.col = coleta_uso.colecao(self.db)
        monta(self.session, self.col)

    def tearDown(self):
        self.session.close()

    def resumo(self, inicio=date(2026, 3, 1), fim=date(2026, 3, 31)):
        return rf.resumo(self.session, self.col, inicio, fim, AGORA, {'2': 'EQRIMP', '15': 'SACIT'})


class TestFichasEOrigem(CenarioTestCase):

    def test_quais_fichas(self):
        fichas = {f.id: f for f in rf.fichas_do_intervalo(self.session, date(2026, 3, 1), date(2026, 3, 31))}
        self.assertEqual(sorted(fichas), [101, 102, 103, 104, 105, 109, 110])
        self.assertTrue(fichas[101].navii and not fichas[102].navii)
        self.assertIsNone(fichas[104].verificacao)                         # a verificação foi excluída
        self.assertEqual((fichas[101].tg, fichas[101].valor_tg), (d(2) + timedelta(days=40), 1000.0))
        self.assertEqual((fichas[103].resultado, fichas[103].valor_perdimento), (d(4) + timedelta(days=200), 5000.0))
        self.assertEqual(fichas[109].aberta, d(8))                          # sem datahora: o create_date

    def test_atribuicao_provavel(self):
        fichas = rf.fichas_do_intervalo(self.session, date(2026, 3, 1), date(2026, 3, 31))
        origem = rf.atribui(fichas, self.col)
        self.assertEqual({i: origem[i] for i in (101, 102, 103, 104)},
                         {101: 'navii', 102: 'provavel', 103: 'demais', 104: 'demais'})
        self.assertEqual(rf.atribui(fichas, None)[102], 'demais')          # sem a coleta, nada é provável

    def test_tabelas_declaradas_existem_nos_models_do_fichas(self):
        for tabela in (rf.OVRS, rf.TGS, rf.RESULTADOS):
            modelo = metadata_fichas.tables[tabela.name]
            self.assertTrue({c.name for c in tabela.columns} <= {c.name for c in modelo.columns}, tabela.name)


class TestIndicadores(CenarioTestCase):

    def test_por_setor_e_origem(self):
        resumo = self.resumo()
        self.assertEqual([s['nome'] for s in resumo['setores']], ['EQRIMP', 'SACIT'])   # o maior primeiro
        eqrimp = resumo['setores'][0]
        navii, provavel, demais = (eqrimp['origens'][o] for o in rf.ORIGENS)
        self.assertEqual((navii['abertas'], navii['verificacao'], navii['resultado'], navii['valorTG']),
                         (1, {'chegaram': 1, 'elegiveis': 1, 'percentual': 100.0},
                          {'chegaram': 1, 'elegiveis': 1, 'percentual': 100.0}, 1000.0))
        self.assertEqual((provavel['abertas'], provavel['verificacao']['percentual'], provavel['comVerificacao'],
                          provavel['encerradasSemResultado']), (1, 0.0, 1, 1))
        self.assertEqual((demais['abertas'], demais['verificacao'], demais['resultado']['chegaram'],
                          demais['comResultado'], demais['encerradasSemResultado'], demais['emAndamento'],
                          demais['valorPerdimento']),
                         (4, {'chegaram': 0, 'elegiveis': 4, 'percentual': 0.0}, 0, 2, 0, 2, 5000.0))   # sem o crédito
        total = eqrimp['total']
        self.assertEqual((total['abertas'], total['verificacao']['percentual'], total['comResultado'],
                          total['encerradasSemResultado']), (6, 16.7, 3, 1))
        self.assertEqual((resumo['total']['abertas'], resumo['origens']['demais']['abertas']), (7, 5))

    def test_mes_em_curso_em_maturacao(self):
        outubro = self.resumo(date(2026, 10, 1), date(2026, 10, 17))
        navii = outubro['origens']['navii']
        self.assertEqual((navii['abertas'], navii['verificacao'], navii['resultado']['percentual']),
                         (1, {'chegaram': 0, 'elegiveis': 0, 'percentual': None}, None))

    def test_coorte_no_limite_do_prazo(self):
        ficha = rf.Ficha(1, '2', ANA, 'CE', datetime(2026, 1, 1), 1, verificacao=datetime(2026, 1, 31))
        self.assertEqual(rf.coorte([ficha], lambda f: f.verificacao, 30, datetime(2026, 1, 31)),
                         {'chegaram': 1, 'elegiveis': 1, 'percentual': 100.0})       # 30 dias exatos: conta
        self.assertEqual(rf.coorte([ficha], lambda f: f.verificacao, 30, datetime(2026, 1, 30, 23))['elegiveis'], 0)
        tarde = rf.Ficha(2, '2', ANA, 'CE', datetime(2026, 1, 1), 1, verificacao=datetime(2026, 1, 31, 0, 0, 1))
        self.assertEqual(rf.coorte([tarde], lambda f: f.verificacao, 30, AGORA)['chegaram'], 0)

    def test_sem_as_tabelas_do_fichas(self):
        tabelas_opcionais.limpa_cache()
        self.assertIsNone(rf.resumo(cria_sessao(), self.col, date(2026, 3, 1), date(2026, 3, 31), AGORA))


class TestNaAba(CenarioTestCase):

    def test_no_json_e_na_planilha(self):
        dados = estatisticas_uso.estatisticas(self.session, self.db, ANA, 'mes', '2026-03', agora=AGORA)
        fichas = dados['fichas']
        self.assertEqual((fichas['atual']['total']['abertas'], fichas['anterior']['total']['abertas']), (7, 1))
        self.assertEqual(fichas['prazos'], {'verificacao': 30, 'resultado': 180})
        self.assertEqual(fichas['porMilExaminados'], 1000.0)               # 1 Ficha pelo card / 1 card examinado
        livro = load_workbook(io.BytesIO(estatisticas_uso.planilha_xlsx(dados)))
        linhas = list(livro['Fichas'].iter_rows(values_only=True))
        self.assertIn(('EQRIMP', 'Pelo card (NAVI-I)', 1, 1, 1, 100, 1, 1, 100, 1, 0, 0, 1000, 0), linhas)
        self.assertEqual(linhas[-1][:3], ('Todos', 'Total', 7))


if __name__ == '__main__':
    unittest.main()
