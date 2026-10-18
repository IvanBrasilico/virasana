"""Testes do diagnóstico das estatísticas de uso (analytic_scripts/diagnostico_estatisticas_uso.py) — fase 0.

Fichas (agora = 17/10/2026; universo: tipo 1 dos últimos 24 meses):
  F1 01/08 aberta, flag NAVI-I, só RVF de inspeção não invasiva, sem verificação: o dashboard do Fichas diz
     "aguardando saneamento"; a estatística, "aguardando verificação física"
  F2 01/03 RVF de INI e verificação +10 d (não é "só INI"), TG +20 d, perdimento +100 d, encerrada (+120 d)
  F3 10/03 encerrada (arquivada) sem TG nem resultado
  F4 10/10 nova (7 dias): fora das coortes de 30 dias ou mais
  F5 FMA (tipo 0): fora
  F6 15/03 verificação EXCLUÍDA (não conta) e TG +5 d: aberta com TG
  F7 01/04 TG +10 d e encerrada exatamente no 30º dia: encerrada COM resultado só pelo TG (conta "em até 30 d")
  F8 01/05 multa (resultado) +15 d, sem TG nem RVF: aberta com resultado; 169 dias, fora da coorte de 180
Universo: o cenário da busca do NAVI-I (desembarques em 11 e 12/01; atracação prevista em 10/01).
"""
import io
import re
import unittest
from datetime import date, datetime, timedelta
from unittest import mock

from sqlalchemy import create_engine, text

from virasana.analytic_scripts import diagnostico_estatisticas_uso as deu
from virasana.navii import busca_navii
from virasana.tests.test_diagnostico_fichas_repressao import insere
from virasana.tests.test_navii_busca_sqlite import cria_sessao, monta_cenario

AGORA = datetime(2026, 10, 17, 12)
CPF = '52998224725'
DDL = [
    'CREATE TABLE ovr_tiposevento (id INTEGER PRIMARY KEY, nome VARCHAR(50), fase INTEGER, eventoespecial INTEGER, '
    'ordem INTEGER)',
    'CREATE TABLE ovr_setores (id CHAR(15) PRIMARY KEY, nome CHAR(50))',
    'CREATE TABLE ovr_ovrs (id INTEGER PRIMARY KEY, numero VARCHAR(10), ano VARCHAR(4), tipooperacao INTEGER, '
    'fase INTEGER, tipoevento_id INTEGER, setor_id CHAR(15), numeroCEmercante VARCHAR(15), cnpj_fiscalizado '
    'VARCHAR(15), user_name VARCHAR(14), responsavel_cpf VARCHAR(15), cpfauditorresponsavel VARCHAR(15), '
    'datahora TIMESTAMP, create_date TIMESTAMP, observacoes VARCHAR(1000))',
    'CREATE TABLE ovr_eventos (id INTEGER PRIMARY KEY, ovr_id INTEGER, create_date TIMESTAMP, tipoevento_id INTEGER, '
    'meramente_informativo BOOLEAN, excluido BOOLEAN, motivo VARCHAR(200), user_name VARCHAR(14))',
    'CREATE TABLE ovr_verificacoesfisicas (id INTEGER PRIMARY KEY, ovr_id INTEGER, numerolote VARCHAR(20), '
    'numeroCEmercante VARCHAR(15), inspecaonaoinvasiva BOOLEAN, datahora TIMESTAMP)',
    'CREATE TABLE ovr_imagensrvf (id INTEGER PRIMARY KEY, rvf_id INTEGER)',
    'CREATE TABLE ovr_flags (id INTEGER PRIMARY KEY, nome VARCHAR(100))',
    'CREATE TABLE ovr_flags_ovr (rvf_id INTEGER, flag_id INTEGER)',
    'CREATE TABLE ovr_tgovr (id INTEGER PRIMARY KEY, ovr_id INTEGER, create_date TIMESTAMP, valor NUMERIC(12, 2))',
    'CREATE TABLE ovr_resultados (id INTEGER PRIMARY KEY, ovr_id INTEGER, create_date TIMESTAMP, '
    'tipo_resultado INTEGER, valor NUMERIC(12, 2))',
]


def d(dia, mes, mais=0):
    return datetime(2026, mes, dia, 9) + timedelta(days=mais)


def monta_banco(engine):
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        insere(con, 'ovr_tiposevento', [
            dict(id=22, nome='Verificação física informada', fase=1, eventoespecial=2),
            dict(id=40, nome='Inspeção não Invasiva', fase=1, eventoespecial=7),
            dict(id=50, nome='Encerramento com resultado', fase=3, eventoespecial=8),
            dict(id=15, nome='Encerramento sem resultado', fase=4, eventoespecial=9)])
        base = dict(tipooperacao=1, fase=1, setor_id='1', user_name=CPF, responsavel_cpf=CPF)
        insere(con, 'ovr_ovrs', [
            dict(base, id=1, datahora=d(1, 8)), dict(base, id=2, datahora=d(1, 3), fase=3),
            dict(base, id=3, datahora=d(10, 3), fase=4), dict(base, id=4, datahora=d(10, 10)),
            dict(base, id=5, datahora=d(1, 3), tipooperacao=0), dict(base, id=6, datahora=d(15, 3)),
            dict(base, id=7, datahora=d(1, 4), fase=3), dict(base, id=8, datahora=d(1, 5))])
        insere(con, 'ovr_eventos', [
            dict(id=1, ovr_id=1, create_date=d(1, 8), tipoevento_id=40),
            dict(id=2, ovr_id=2, create_date=d(1, 3, 10), tipoevento_id=22),
            dict(id=3, ovr_id=2, create_date=d(1, 3, 120), tipoevento_id=50),
            dict(id=4, ovr_id=3, create_date=d(10, 3, 40), tipoevento_id=15),
            dict(id=5, ovr_id=6, create_date=d(15, 3, 2), tipoevento_id=22, excluido=True),
            dict(id=6, ovr_id=7, create_date=d(1, 4, 30), tipoevento_id=50)])
        insere(con, 'ovr_verificacoesfisicas', [dict(id=1, ovr_id=1, numerolote='C1', inspecaonaoinvasiva=True),
                                                dict(id=2, ovr_id=2, numerolote='C2', inspecaonaoinvasiva=True)])
        insere(con, 'ovr_flags', [dict(id=1, nome='NAVI-I'), dict(id=2, nome='Operação X')])
        insere(con, 'ovr_flags_ovr', [dict(rvf_id=1, flag_id=1), dict(rvf_id=6, flag_id=2)])
        insere(con, 'ovr_tgovr', [dict(id=1, ovr_id=2, create_date=d(1, 3, 20), valor=1000),
                                  dict(id=2, ovr_id=6, create_date=d(15, 3, 5), valor=250.5),
                                  dict(id=3, ovr_id=5, create_date=d(1, 3, 1), valor=99),
                                  dict(id=4, ovr_id=7, create_date=d(1, 4, 10), valor=300)])
        insere(con, 'ovr_resultados', [dict(id=1, ovr_id=2, create_date=d(1, 3, 100), tipo_resultado=2, valor=5000),
                                       dict(id=2, ovr_id=8, create_date=d(1, 5, 15), tipo_resultado=6, valor=800)])


class TestFichas(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        engine = create_engine('sqlite://')
        monta_banco(engine)
        saida = io.StringIO()
        with engine.connect() as conexao:
            cls.diag = deu.DiagnosticoEstatisticas(conexao, saida, agora=AGORA)
            cls.diag.roda()
        cls.log = saida.getvalue()

    def linha(self, inicio):
        return next(linha for linha in self.log.splitlines() if linha.startswith(inicio))

    def celulas(self, inicio):
        return [c.strip() for c in self.linha(inicio).split('|')]

    def test_sem_erro_e_sem_universo(self):
        self.assertNotIn('!! ERRO', self.log)
        self.assertIn('sem sessão do NAVI-I', self.log)
        self.assertEqual(sorted(self.diag.fichas), [1, 2, 3, 4, 6, 7, 8])

    def test_por_mes(self):
        # mês | abertas | NAVI-I | RVF | só INI | verificação | TG | resultado | encerradas | sem resultado
        self.assertEqual(self.celulas('2026-03'), ['2026-03', '3', '0', '1', '0', '1', '2', '1', '2', '1'])
        self.assertEqual(self.celulas('2026-08'), ['2026-08', '1', '1', '1', '1', '0', '0', '0', '0', '0'])
        self.assertEqual(self.celulas('2026-04'), ['2026-04', '1', '0', '0', '0', '0', '1', '0', '1', '0'])
        self.assertEqual(self.celulas('2026-05'), ['2026-05', '1', '0', '0', '0', '0', '0', '1', '0', '0'])
        self.assertEqual(self.celulas('total'), ['total', '7', '1', '2', '1', '1', '3', '2', '3', '1'])

    def test_status_do_dashboard_x_proposto(self):
        self.assertRegex(self.log, r'Aguardando saneamento\(s\)\s+\| aguardando verificação física\s+\| 1 ')
        self.assertRegex(self.log, r'Concluída\s+\| encerrada com resultado\s+\| 2 ')      # F2 e F7 (só o TG)
        self.assertRegex(self.log, r'Arquivada\s+\| encerrada sem resultado\s+\| 1 ')
        self.assertRegex(self.log, r'Mercadorias já informadas\s+\| aberta com TG\s+\| 1 ')   # F6: excluída não vale
        self.assertRegex(self.log, r'Selecionado ainda sem aberturas\s+\| aguardando verificação física\s+\| 1 ')
        self.assertRegex(self.log, r'Selecionado ainda sem aberturas\s+\| aberta com resultado\s+\| 1 ')   # F8

    def test_resultados_e_tg_so_das_fichas_do_universo(self):
        self.assertRegex(self.log, r'Perdimento \| 1\s+\| 1\s+\| 5,000.00')
        self.assertRegex(self.log, r'Multa RFB  \| 1\s+\| 1\s+\| 800.00')
        self.assertIn('Com TG: 3 (43%); valor dos TGs: 1,550.50', self.log)            # sem o TG da FMA

    def test_maturacao_por_coorte(self):
        # elegíveis: 30 d = F1 (77 d), F2, F3, F6, F7, F8; 90 d = sem a F1; 180 d = sem a F8 (169 d); 365 d = nenhuma;
        # F4 (7 dias) nunca
        celulas = self.celulas('verificação informada')
        self.assertEqual(celulas[1], '1 (14%)')
        self.assertEqual(celulas[3:], ['1/6 (17%)', '1/5 (20%)', '1/4 (25%)', '-'])
        self.assertEqual(self.celulas('TG ou resultado')[3:], ['4/6 (67%)', '4/5 (80%)', '3/4 (75%)', '-'])
        self.assertEqual(self.celulas('resultado (ovr_resultados)')[3:], ['1/6 (17%)', '1/5 (20%)', '1/4 (25%)', '-'])
        self.assertEqual(self.celulas('encerramento')[3:], ['1/6 (17%)', '2/5 (40%)', '3/4 (75%)', '-'])   # F7: dia 30

    def test_sigilo(self):
        self.assertNotIn(CPF, self.log)


class TestUniverso(unittest.TestCase):

    def setUp(self):
        self.session = cria_sessao()
        monta_cenario(self.session)
        self.session.commit()
        self.saida = io.StringIO()
        self.diag = deu.DiagnosticoEstatisticas(self.session.connection(), self.saida, session=self.session,
                                                dias_universo=15, agora=datetime(2026, 1, 20, 12))

    def test_dia_a_dia_bate_com_a_contagem_da_tela(self):
        self.diag.secao_universo()
        log = self.saida.getvalue()
        self.assertNotIn('!!', log)
        for janela in deu.JANELAS:
            direto = busca_navii.conta_conteineres_navii(
                self.session, janela, {'data_inicio': '2026-01-05', 'data_fim': '2026-01-19'})
            self.assertGreater(direto, 0)
            self.assertRegex(log, rf'Janela "{janela}":(.|\n)*?Soma dos dias: {direto}; contagem do período de uma '
                                  rf'vez: {direto} ')
        self.assertEqual(len(re.findall(r'^\d\d/01/2026', log, re.MULTILINE)), 30)          # 15 dias x 2 janelas

    def test_dupla_contagem_e_erro_aparecem(self):
        def falsa(session, janela, filtros):
            if janela == 'prevista' and filtros['data_fim'] == '2026-01-01':
                raise ValueError('banco fora')            # só um dia falha; o período (até 02/01) conta
            return 10 if filtros['data_inicio'] != filtros['data_fim'] else 1
        with mock.patch.object(busca_navii, 'conta_conteineres_navii', falsa):
            self.diag.universo_da_janela('efetiva', [date(2026, 1, 1) + timedelta(days=n) for n in range(15)])
            self.diag.universo_da_janela('prevista', [date(2026, 1, 1), date(2026, 1, 2)])
        log = self.saida.getvalue()
        self.assertIn('Soma dos dias: 15; contagem do período de uma vez: 10', log)
        self.assertIn('!! DUPLA CONTAGEM: a soma dos dias difere do período em 5', log)
        self.assertIn('!! ERRO na contagem (prevista', log)
        self.assertIn('Soma dos dias: 1; contagem do período de uma vez: 10', log)
        self.assertEqual(log.count('DUPLA CONTAGEM'), 1)          # com um dia em erro, a soma não é comparada


if __name__ == '__main__':
    unittest.main()
