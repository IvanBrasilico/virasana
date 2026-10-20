"""Diagnóstico das pesagens (analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py) — P0, em SQLite.

Agora = 18/10/2026 12h, 14 dias. Recinto R1 (balança de 20 kg, captura automática), R2 (outro recinto):
  P1 MSCU1111111 bruto 40.120, par na visita 15.000 (30 min antes, mesmo semirreboque), contêiner 2.200 (Mercante)
     -> 22.920 x declarado 22.900: coerente. O CE mais recente é de EXPORTAÇÃO e outro é posterior à pesagem: fora.
  P2 TGHU2222222 bruto 40.700, par 15.000 -> 23.500 x declarado 2.350 (×10): digitação provável
  P3 CAIU3333333 sem pesagem vazia do conjunto, só taraConjunto da API: sem dado (sem tara)
  P4 TCLU4444444 bruto 30.000, vazia 3 dias depois em R2 (não é par: outro recinto) 15.000, contêiner de 45' sem tara
     no Mercante (típica 3.800) -> 11.200 x declarado 20.000: divergência
  P5 MSKU5555555 bruto 41.235 (FORA da divisão de 20 kg: digitado), par 15.000 -> 24.035 x 24.000: coerente
  RTG ZZZU9999999 sem placa, 25.000. Mais 60 pesagens vazias de outros caminhões (múltiplos de 20) em R1.
"""
import io
import unittest
from datetime import datetime

from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: ajusta o sys.path
from bhadrasana.models.apirecintos import AcessoVeiculo, PesagemVeiculo  # noqa: I100
from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_pesagens import DiagnosticoPesagens, roda
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item
from virasana.navii.busca_navii import tabela_recintos

R1, R2 = '8931356', '8931359'
AGORA = datetime(2026, 10, 18, 12)


def pesagem(id_, data, placa, conteiner, peso, semi='', recinto=R1, tara_conjunto=None, captura=True):
    return {'id': id_, 'codigoRecinto': recinto, 'dataHoraOcorrencia': data, 'dataHoraTransmissao': data,
            'placa': placa, 'placaSemirreboque': semi, 'numeroConteiner': conteiner, 'pesoBrutoBalanca': peso,
            'taraConjunto': tara_conjunto, 'capturaAutoPeso': captura, 'tipoOperacao': 'I', 'contingencia': False}


def monta(session):
    session.execute(tabela_recintos.insert(), [{'id': 1, 'nome': 'SANTOS BRASIL', 'cod_siscomex': R1},
                                               {'id': 2, 'nome': 'BTP', 'cod_siscomex': R2}])
    pesagens = [
        pesagem(1, datetime(2026, 10, 10, 10), 'AAA1A11', 'MSCU1111111', 40120, 'SSS1S11'),
        pesagem(2, datetime(2026, 10, 10, 9, 30), 'AAA1A11', '', 15000, 'SSS1S11'),
        pesagem(3, datetime(2026, 10, 11, 14), 'BBB2B22', 'TGHU2222222', 40700),
        pesagem(4, datetime(2026, 10, 11, 13), 'BBB2B22', '', 15000),
        pesagem(5, datetime(2026, 10, 12, 8), 'CCC3C33', 'CAIU3333333', 39000, tara_conjunto=15000),
        pesagem(6, datetime(2026, 10, 13, 8), 'DDD4D44', 'TCLU4444444', 30000),
        pesagem(7, datetime(2026, 10, 16, 8), 'DDD4D44', '', 15000, recinto=R2),
        pesagem(8, datetime(2026, 10, 14, 9), 'EEE5E55', 'MSKU5555555', 41235),
        pesagem(9, datetime(2026, 10, 14, 8), 'EEE5E55', '', 15000),
        pesagem(10, datetime(2026, 10, 15, 9), '', 'ZZZU9999999', 25000),
    ] + [pesagem(100 + n, datetime(2026, 10, 9, 8, n), f'FIL{n:04d}', '', 15000 + 20 * n) for n in range(60)]
    session.execute(PesagemVeiculo.__table__.insert(), pesagens)
    session.execute(AcessoVeiculo.__table__.insert(), [
        {'id': 1, 'codigoRecinto': R1, 'dataHoraOcorrencia': datetime(2026, 10, 10, 10, 1), 'operacao': 'C',
         'direcao': 'S', 'placa': 'AAA1A11', 'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': R1, 'dataHoraOcorrencia': datetime(2026, 10, 11, 14, 5), 'operacao': 'C',
         'direcao': 'S', 'placa': 'BBB2B22', 'tipoOperacao': 'I'},
        {'id': 3, 'codigoRecinto': R1, 'dataHoraOcorrencia': datetime(2026, 10, 12, 8, 1), 'operacao': 'G',
         'direcao': 'S', 'placa': 'CCC3C33', 'tipoOperacao': 'I'}])          # agendamento: não conta
    ces = [('C1', 'MSCU1111111', '22900', '2200', '22G1', '2026-09-20', '5'),
           ('C1X', 'MSCU1111111', '9999', '2200', '22G1', '2026-10-05', '7'),     # exportação: fora
           ('C1D', 'MSCU1111111', '8888', '2200', '22G1', '2026-10-15', '5'),     # depois da pesagem: fora
           ('C2', 'TGHU2222222', '2350', '2200', '22G1', '2026-09-25', '5'),
           ('C3', 'CAIU3333333', '24000', '2200', '22G1', '2026-09-25', '5'),
           ('C4', 'TCLU4444444', '20000', '', '45G1', '2026-09-01', '5'),
           ('C5', 'MSKU5555555', '24000', '2200', '22G1', '2026-09-10', '5')]
    for n, (ce, conteiner, peso, tara, iso, emissao, trafego) in enumerate(ces):
        session.add(Conhecimento(ID=n + 1, numeroCEmercante=ce, dataEmissao=emissao, tipoTrafego=trafego))
        session.add(Item(ID=n + 1, numeroCEmercante=ce, codigoConteiner=conteiner, pesoBruto=peso, tara=tara,
                         isoCode=iso))
    session.commit()


class TestDiagnosticoPesagens(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        session = cria_sessao()
        monta(session)
        saida = io.StringIO()
        cls.diag = DiagnosticoPesagens(session.connection(), saida, datetime(2026, 10, 4, 12), AGORA, amostra=50,
                                       agora=AGORA)
        roda(cls.diag)
        cls.log = saida.getvalue()

    def linha(self, *trechos):
        achadas = [linha for linha in self.log.splitlines() if all(t in linha for t in trechos)]
        self.assertTrue(achadas, f'nenhuma linha com {trechos}')
        return [c.strip() for c in achadas[0].split('|')]

    def test_sem_erro(self):
        self.assertNotIn('!! ERRO', self.log)

    def test_preenchimento(self):
        celulas = self.linha('8931356 SANTOS BRASIL', '| 69 ')
        self.assertEqual(celulas[1:4], ['69', '6 (9%)', '68 (99%)'])     # 6 com contêiner (inclusive o RTG)

    def test_divisao_e_fora_dela(self):
        celulas = self.linha('8931356 SANTOS BRASIL', 'rodoviária', '| sim')
        self.assertEqual(celulas[4:6], ['20 kg', '1 (1%)'])              # só o 41.235
        self.assertEqual(self.diag.divisoes[R1], 20)
        self.assertIn('RTG/sem placa', self.log)

    def test_gate(self):
        self.assertEqual(self.linha('8931356', 'até 2 min (saída)')[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', '2 a 10 min (saída)')[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', 'sem acesso em 6 h')[2], '3 (60%)')   # o agendamento não conta

    def test_tara(self):
        self.assertEqual(self.linha('8931356', 'par na visita')[2], '3 (60%)')
        self.assertEqual(self.linha('8931356', 'vazia em 7 dias')[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', 'só taraConjunto')[2], '1 (20%)')

    def test_rtg(self):
        self.assertEqual(self.linha('8931356 SANTOS BRASIL', '25000')[1:3], ['1', '25000'])

    def test_medido_x_declarado(self):
        self.assertEqual(self.linha('8931356', f'{pc.COERENTE}  ')[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', f'{pc.COERENTE} — pesagem fora da divisão')[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', pc.DIGITACAO)[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', pc.DIVERGENCIA)[2], '1 (20%)')
        self.assertEqual(self.linha('8931356', 'sem dado (sem tara)')[2], '1 (20%)')
        self.assertIn('MSCU1111111 CE C1 em 10/10 10:00: bruto 40120 − conjunto 15000 (par) − contêiner 2200 '
                      '(informada) = 22920 x declarado 22900', self.log)
        self.assertIn('TCLU4444444 CE C4', self.log)
        self.assertIn('(vazia) − contêiner 3800 (típica do tamanho) = 11200 x declarado 20000', self.log)
        self.assertIn('= 23500 x declarado 2350 — ordem de grandeza (×10); +900%', self.log)


if __name__ == '__main__':
    unittest.main()
