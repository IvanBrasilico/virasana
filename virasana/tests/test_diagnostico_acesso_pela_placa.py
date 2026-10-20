"""D3 — contêiner do acesso pela placa (analytic_scripts/ciclo_de_vida/diagnostico_acesso_pela_placa.py), em SQLite.

Agora = 20/10/2026 12h, 14 dias no ICTSI (7921302, acessos SEM contêiner) e 2 dias no controle BTP (8931359).
ICTSI:
  CTR1: D 10/10; inspeção 11/10; pesagem 14/10 10:00 (placa AAA1A11) e acesso S 10:02 da mesma placa -> saída
        deduzida pela pesagem; outro D em 17/10 (outra operação) e E em 18/10 (é só do 2º D)
  CTR2: D 19/10; inspeção 09:00 (placa BBB2B22) e acesso S 09:03 da placa 'BBB-2B22' -> deduzido pela inspeção
  CTR3: D 12/10; acesso E 13/10 18:00 (JJJ0J00) deduzido pela pesagem — entrada, não saída.  PASS9: D 08/10 e E 09/10 (passagem).  EXP4: E 15/10 sem D, inspeção depois
  acesso E 13/10 07:00 placa CCC3C33 com duas pesagens (CTR5, CTR6) -> ambíguo
  acesso S 13/10 15:00 placa DDD4D44, pesagem 20 min depois -> nenhum em 5 min, deduzido em 30 min
  acesso S 16/10 11:00 placa EEE5E55, pesagem e inspeção do CTR8 -> pelas duas; acesso S 16/10 12:00 sem placa
BTP (controle, acessos COM contêiner): igual (CTRA), diferente (CTRB x CTRZ), sem marca (CTRC) -> precisão 50%
"""
import io
import unittest
from datetime import datetime

from sqlalchemy import text

from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: ajusta o sys.path
from bhadrasana.models.apirecintos import (  # noqa: I100
    AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva, PesagemVeiculo,
)
from virasana.analytic_scripts.ciclo_de_vida import acesso_pela_placa as ap
from virasana.analytic_scripts.ciclo_de_vida import diagnostico_acesso_pela_placa as dap

ICTSI, BTP = '7921302', '8931359'
AGORA = datetime(2026, 10, 20, 12)
INICIO = datetime(2026, 10, 6, 12)


def d(dia, hora=0, minuto=0):
    return datetime(2026, 10, dia, hora, minuto)


def ev(id_, recinto, data, conteiner='', placa=None, **outros):
    return {'id': id_, 'codigoRecinto': recinto, 'dataHoraOcorrencia': data, 'numeroConteiner': conteiner,
            'placa': placa, 'tipoOperacao': 'I', **outros}


def monta(session):
    session.execute(text(f"INSERT INTO ovr_recintos (id, nome, cod_siscomex) VALUES (1, 'RIO_ICTSI', '{ICTSI}'), "
                         f"(2, 'BTP', '{BTP}')"))
    session.execute(EmbarqueDesembarque.__table__.insert(), [
        {'embarqueDesembarque': tipo, **ev(i, ICTSI, data, conteiner)} for i, (conteiner, data, tipo) in enumerate((
            ('CTR1', d(10, 8), 'D'), ('CTR1', d(17, 8), 'D'), ('CTR1', d(18, 8), 'E'), ('CTR2', d(19, 8), 'D'), ('CTR3', d(12, 8), 'D'),
            ('PASS9', d(8, 9), 'D'), ('PASS9', d(9, 9), 'E'), ('EXP4', d(15, 9), 'E')), start=1)])
    acesso = {'operacao': 'C'}
    session.execute(AcessoVeiculo.__table__.insert(), [
        ev(1, ICTSI, d(14, 10, 2), placa='AAA1A11', direcao='S', **acesso),
        ev(2, ICTSI, d(19, 9, 3), placa='BBB-2B22', direcao='S', **acesso),
        ev(3, ICTSI, d(13, 7), placa='CCC3C33', direcao='E', **acesso),
        ev(4, ICTSI, d(13, 15), placa='DDD4D44', direcao='S', **acesso),
        ev(5, ICTSI, d(16, 11), placa='EEE5E55', direcao='S', **acesso),
        ev(6, ICTSI, d(16, 12), placa=None, direcao='S', **acesso),
        ev(7, ICTSI, d(16, 13), placa='ZZZ9Z99', direcao='S', operacao='G'),     # agendamento: fora
        ev(8, ICTSI, d(13, 18), placa='JJJ0J00', direcao='E', **acesso),
        ev(11, BTP, d(19, 8), 'CTRA', 'FFF6F66', direcao='E', **acesso),
        ev(12, BTP, d(19, 9), 'CTRB', 'GGG7G77', direcao='S', **acesso),
        ev(13, BTP, d(19, 10), 'CTRC', 'HHH8H88', direcao='S', **acesso)])
    session.execute(PesagemVeiculo.__table__.insert(), [
        ev(1, ICTSI, d(14, 10), 'CTR1', 'AAA1A11'),
        ev(2, ICTSI, d(13, 7, 1), 'CTR5', 'CCC3C33'), ev(3, ICTSI, d(13, 7, 2), 'CTR6', 'CCC3C33'),
        ev(4, ICTSI, d(13, 15, 20), 'CTR7', 'DDD4D44'),
        ev(5, ICTSI, d(16, 11, 1), 'CTR8', 'EEE5E55'), ev(6, ICTSI, d(13, 18, 1), 'CTR3', 'JJJ0J00'),
        ev(11, BTP, d(19, 8, 1), 'CTRA', 'FFF6F66'), ev(12, BTP, d(19, 9, 2), 'CTRZ', 'GGG7G77')])
    session.execute(InspecaoNaoInvasiva.__table__.insert(), [
        ev(1, ICTSI, d(11, 9), 'CTR1'), ev(2, ICTSI, d(19, 9), 'CTR2', 'BBB2B22'),
        ev(3, ICTSI, d(16, 11, 2), 'CTR8', 'EEE5E55'), ev(4, ICTSI, d(15, 10), 'EXP4')])
    session.commit()


class TestDiagnosticoAcessoPelaPlaca(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.session = cria_sessao()
        monta(cls.session)
        cls.saida = io.StringIO()
        cls.diag = dap.DiagnosticoPlaca(cls.session.connection(), cls.saida, INICIO, AGORA, agora=AGORA)
        dap.roda(cls.diag)
        cls.log = cls.saida.getvalue()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def linha(self, comeco: str) -> str:
        return next(linha for linha in self.log.splitlines() if linha.startswith(comeco))

    def test_deduzidos_por_motivo(self):
        resultado = {(a[1], a[2]): (c, motivo) for a, c, motivo, _ in self.diag.alvo.deduz(5)}
        self.assertEqual(resultado, {
            ('S', 'AAA1A11'): ('CTR1', ap.PELA_PESAGEM), ('S', 'BBB-2B22'): ('CTR2', ap.PELA_INSPECAO),
            ('E', 'CCC3C33'): (None, ap.AMBIGUO), ('S', 'DDD4D44'): (None, ap.NENHUM),
            ('S', 'EEE5E55'): ('CTR8', ap.PELAS_DUAS), ('S', ''): (None, ap.SEM_PLACA),
            ('E', 'JJJ0J00'): ('CTR3', ap.PELA_PESAGEM)})
        self.assertEqual(self.diag.alvo.deduz(30)[3][1], 'CTR7')   # DDD4D44: a pesagem 20 min depois

    def test_preenchimento(self):
        self.assertIn('7921302 | acesso S | 5     | 5 (100%)      | 1 (20%)', self.log)
        self.assertIn('7921302 | acesso E | 2     | 2 (100%)      | 0 (0%)', self.log)
        self.assertIn('8931359 | acesso E | 1     | 0 (0%)', self.log)

    def test_cobertura_por_janela(self):
        self.assertIn('5 min  | S       | 5                     | 3 (60%)   | 0 (0%)', self.log)
        self.assertIn('30 min | S       | 5                     | 4 (80%)', self.log)
        self.assertIn('5 min  | E       | 2                     | 1 (50%)   | 1 (50%)', self.log)

    def test_controle_precisao(self):
        self.assertEqual(self.linha('5 min  | 3').split('|')[-1].strip(), '50.0%')
        self.assertIn('1 (33%)', self.linha('5 min  | 3'))

    def test_ciclo_por_idade(self):
        # 7+ dias: CTR1 (inspeção, pesagem, saída deduzida — só até o 2º D), CTR3 (pesagem; o acesso deduzido é
        # ENTRADA), PASS9 (embarque)
        self.assertEqual([c.strip() for c in self.linha('7 dias ou mais').split('|')],
                         ['7 dias ou mais', '3', '1 (33%)', '2 (67%)', '1 (33%)', '1 (33%)', '0 (0%)'])
        self.assertEqual([c.strip() for c in self.linha('3 a 7 dias').split('|')],   # 2º D do CTR1: o E de 18/10
                         ['3 a 7 dias', '1', '0 (0%)', '0 (0%)', '1 (100%)', '0 (0%)', '0 (0%)'])
        self.assertEqual([c.strip() for c in self.linha('< 3 dias').split('|')],
                         ['< 3 dias', '1', '1 (100%)', '0 (0%)', '0 (0%)', '1 (100%)', '0 (0%)'])

    def test_embarques(self):
        self.assertIn('sem D antes e com inspeção/pesagem depois (parece desembarque) | 1', self.log)
        self.assertIn('com D antes (passagem)                                         | 2', self.log)

    def test_exemplos(self):
        self.assertIn('14/10 10:02 S placa AAA1A11 -> CTR1 (2.0 min)', self.log)
        self.assertIn('13/10 07:00 E placa CCC3C33 -> ? (1.0 min)', self.log)


if __name__ == '__main__':
    unittest.main()
