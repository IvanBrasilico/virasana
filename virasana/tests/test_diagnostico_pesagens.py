"""Diagnóstico das pesagens (analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py) — P0 e P0b, em SQLite.

Agora = 18/10/2026 12h, 14 dias. Recinto R1 (balança de 20 kg, captura automática), R2 (outro recinto). Cada
contêiner (menos o EXPU) tem um DESEMBARQUE antes da pesagem — a mesma operação (P0b):
  P1 MSCU1111111 D R1; bruto 40.120, par 15.000 (30 min antes, mesmo semirreboque), contêiner 2.200 -> 22.920 x
     22.900: coerente. O CE mais recente é de EXPORTAÇÃO e outro é posterior à pesagem: fora.
  P2 TGHU2222222 D R1; 40.700 − 15.000 − 2.200 = 23.500 x 2.350 (×10): digitação provável
  P3 CAIU3333333 D R1; sem par nem vazia, taraConjunto 15.000 -> 21.800 x 24.000 (−9%): diferença moderada
  P4 TCLU4444444 D em R2, pesagem em R1 (outro recinto); vazia 3 dias depois em R2, 45' típica 3.800 -> 11.200 x
     20.000: divergência
  P5 MSKU5555555 D R1; 41.235 (FORA da divisão de 20 kg) − 15.000 − 2.200 = 24.035 x 24.000: coerente
  P6 EXPU6666666 sem desembarque antes: outra operação, não comparada
  P7 CONS7777777 D R1; consolidado: master M7 (24.000) e filhotes F71 (8.000) e F72 (16.000) -> o do master;
     41.300 − 15.000 − 2.200 = 24.100: coerente (no P0, F72 sozinho: +50%)
  P8 SUMU8888888 D R1; só filhotes F81 (10.000) e F82 (12.000) -> soma 22.000; 39.200 − 15.000 − 2.200: coerente
  P9 VAZU9999999 D R1; 17.200 − 15.000 − 2.200 = 0 x 4.904: contêiner vazio na pesagem
  P10 PICK1010101 D R1; chegada 15.000 COM o número do contêiner e saída 39.000: a chegada é "sem o contêiner" e é o
     par da saída -> 21.800 x 21.800: coerente
  P11 NOTA1111111 D R1; sem par, vazia nem taraConjunto: sem dado (sem tara)
  RTG ZZZU9999999 sem placa, 25.000. Mais 60 pesagens vazias de outros caminhões (múltiplos de 20) em R1.
"""
import io
import unittest
from datetime import datetime

from virasana.tests.test_navii_busca_sqlite import cria_sessao  # 1º: ajusta o sys.path
from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, PesagemVeiculo  # noqa: I100
from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_pesagens import DiagnosticoPesagens, roda
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item
from virasana.navii.busca_navii import tabela_recintos

R1, R2 = '8931356', '8931359'
AGORA = datetime(2026, 10, 18, 12)


def d(dia, hora=0, minuto=0):
    return datetime(2026, 10, dia, hora, minuto)


def pesagem(id_, data, placa, conteiner, peso, semi='', recinto=R1, tara_conjunto=None, captura=True):
    return {'id': id_, 'codigoRecinto': recinto, 'dataHoraOcorrencia': data, 'dataHoraTransmissao': data,
            'placa': placa, 'placaSemirreboque': semi, 'numeroConteiner': conteiner, 'pesoBrutoBalanca': peso,
            'taraConjunto': tara_conjunto, 'capturaAutoPeso': captura, 'tipoOperacao': 'I', 'contingencia': False}


def monta(session):
    session.execute(tabela_recintos.insert(), [{'id': 1, 'nome': 'SANTOS BRASIL', 'cod_siscomex': R1},
                                               {'id': 2, 'nome': 'BTP', 'cod_siscomex': R2}])
    pesagens = [
        pesagem(1, d(10, 10), 'AAA1A11', 'MSCU1111111', 40120, 'SSS1S11'),
        pesagem(2, d(10, 9, 30), 'AAA1A11', '', 15000, 'SSS1S11'),
        pesagem(3, d(11, 14), 'BBB2B22', 'TGHU2222222', 40700),
        pesagem(4, d(11, 13), 'BBB2B22', '', 15000),
        pesagem(5, d(12, 8), 'CCC3C33', 'CAIU3333333', 39000, tara_conjunto=15000),
        pesagem(6, d(13, 8), 'DDD4D44', 'TCLU4444444', 30000),
        pesagem(7, d(16, 8), 'DDD4D44', '', 15000, recinto=R2),
        pesagem(8, d(14, 9), 'EEE5E55', 'MSKU5555555', 41235),
        pesagem(9, d(14, 8), 'EEE5E55', '', 15000),
        pesagem(10, d(15, 9), '', 'ZZZU9999999', 25000),
        pesagem(11, d(15, 9, 30), 'FFF6F66', 'EXPU6666666', 35000),
        pesagem(12, d(15, 10), 'GGG7G77', 'CONS7777777', 41300),
        pesagem(13, d(15, 9), 'GGG7G77', '', 15000),
        pesagem(14, d(16, 10), 'HHH8H88', 'SUMU8888888', 39200),
        pesagem(15, d(16, 9), 'HHH8H88', '', 15000),
        pesagem(16, d(16, 11), 'III9I99', 'VAZU9999999', 17200),
        pesagem(17, d(16, 10), 'III9I99', '', 15000),
        pesagem(18, d(17, 8), 'JJJ0J00', 'PICK1010101', 15000),
        pesagem(19, d(17, 8, 40), 'JJJ0J00', 'PICK1010101', 39000),
        pesagem(20, d(17, 9), 'KKK1K11', 'NOTA1111111', 38000),
    ] + [pesagem(100 + n, datetime(2026, 10, 9, 8, n), f'FIL{n:04d}', '', 15000 + 20 * n) for n in range(60)]
    session.execute(PesagemVeiculo.__table__.insert(), pesagens)
    session.execute(AcessoVeiculo.__table__.insert(), [
        {'id': 1, 'codigoRecinto': R1, 'dataHoraOcorrencia': d(10, 10, 1), 'operacao': 'C',
         'direcao': 'S', 'placa': 'AAA1A11', 'tipoOperacao': 'I'},
        {'id': 2, 'codigoRecinto': R1, 'dataHoraOcorrencia': d(11, 14, 5), 'operacao': 'C',
         'direcao': 'S', 'placa': 'BBB2B22', 'tipoOperacao': 'I'},
        {'id': 3, 'codigoRecinto': R1, 'dataHoraOcorrencia': d(12, 8, 1), 'operacao': 'G',
         'direcao': 'S', 'placa': 'CCC3C33', 'tipoOperacao': 'I'}])          # agendamento: não conta
    session.execute(EmbarqueDesembarque.__table__.insert(), [
        {'id': n, 'codigoRecinto': recinto, 'dataHoraOcorrencia': d(dia, 6), 'numeroConteiner': conteiner,
         'embarqueDesembarque': 'D', 'tipoOperacao': 'I'}
        for n, (conteiner, dia, recinto) in enumerate((
            ('MSCU1111111', 5, R1), ('TGHU2222222', 6, R1), ('CAIU3333333', 7, R1), ('TCLU4444444', 8, R2),
            ('MSKU5555555', 9, R1), ('CONS7777777', 9, R1), ('SUMU8888888', 10, R1), ('VAZU9999999', 10, R1),
            ('PICK1010101', 11, R1), ('NOTA1111111', 11, R1)), start=1)])
    ces = [('C1', None, 'MSCU1111111', '22900', '2200', '22G1', '2026-09-20', '5'),
           ('C1X', None, 'MSCU1111111', '9999', '2200', '22G1', '2026-10-05', '7'),     # exportação: fora
           ('C1D', None, 'MSCU1111111', '8888', '2200', '22G1', '2026-10-15', '5'),     # depois da pesagem: fora
           ('C2', None, 'TGHU2222222', '2350', '2200', '22G1', '2026-09-25', '5'),
           ('C3', None, 'CAIU3333333', '24000', '2200', '22G1', '2026-09-25', '5'),
           ('C4', None, 'TCLU4444444', '20000', '', '45G1', '2026-09-01', '5'),
           ('C5', None, 'MSKU5555555', '24000', '2200', '22G1', '2026-09-10', '5'),
           ('M7', None, 'CONS7777777', '24000', '2200', '22G1', '2026-09-01', '5'),
           ('F71', 'M7', 'CONS7777777', '8000', '2200', '22G1', '2026-09-05', '5'),
           ('F72', 'M7', 'CONS7777777', '16000', '2200', '22G1', '2026-09-06', '5'),
           ('F81', 'M8', 'SUMU8888888', '10000', '2200', '22G1', '2026-09-10', '5'),
           ('F82', 'M8', 'SUMU8888888', '12000', '2200', '22G1', '2026-09-11', '5'),
           ('C9', None, 'VAZU9999999', '4904', '2200', '22G1', '2026-09-20', '5'),
           ('C10', None, 'PICK1010101', '21800', '2200', '22G1', '2026-09-20', '5'),
           ('C11', None, 'NOTA1111111', '24000', '2200', '22G1', '2026-09-20', '5')]
    for n, (ce, master, conteiner, peso, tara, iso, emissao, trafego) in enumerate(ces):
        session.add(Conhecimento(ID=n + 1, numeroCEmercante=ce, numeroCEMaster=master, dataEmissao=emissao,
                                 tipoTrafego=trafego))
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
        celulas = self.linha('8931356 SANTOS BRASIL', '| 79 ')
        self.assertEqual(celulas[1:4], ['79', '13 (16%)', '78 (99%)'])     # 13 com contêiner (inclusive o RTG)

    def test_divisao_e_fora_dela(self):
        celulas = self.linha('8931356 SANTOS BRASIL', 'rodoviária', '| sim')
        self.assertEqual(celulas[4:6], ['20 kg', '1 (1%)'])              # só o 41.235
        self.assertEqual(self.diag.divisoes[R1], 20)
        self.assertIn('RTG/sem placa', self.log)

    def test_gate(self):
        self.assertEqual(self.linha('8931356', 'até 2 min (saída)')[2], '1 (8%)')
        self.assertEqual(self.linha('8931356', '2 a 10 min (saída)')[2], '1 (8%)')
        self.assertEqual(self.linha('8931356', 'sem acesso em 6 h')[2], '10 (83%)')   # o agendamento não conta

    def test_tara(self):
        self.assertEqual(self.linha('8931356', '| par na visita')[2], '7 (58%)')       # inclusive o da retirada
        self.assertEqual(self.linha('8931356', '| vazia em 7 dias')[2], '1 (8%)')
        self.assertEqual(self.linha('8931356', '| só taraConjunto')[2], '1 (8%)')
        self.assertEqual(self.linha('8931356', '| nenhuma')[2], '3 (25%)')

    def test_rtg(self):
        self.assertEqual(self.linha('8931356 SANTOS BRASIL', '25000')[1:3], ['1', '25000'])

    def test_amostra_inclui_os_de_fora_de_santos(self):
        volumes = {R1: (79, 13), '9999999': (10, 5), '7921303': (5, 2), '4931303': (5, 0)}
        self.assertEqual(self.diag.recintos_da_amostra(volumes), [R1, '9999999', '7921303'])

    def test_geral_por_operacao(self):
        geral = {(c[0], c[1]): c[2] for c in (self.linha(op, cat) for op, cat in (
            (pc.NO_OPERADOR, f'| {pc.COERENTE}  '), (pc.NO_OPERADOR, pc.DIGITACAO), (pc.NO_OPERADOR, pc.MODERADA),
            (pc.NO_OPERADOR, pc.VAZIO_NA_PESAGEM), (pc.NO_OPERADOR, 'sem dado (sem tara)'),
            (pc.OUTRO_RECINTO, pc.DIVERGENCIA), (pc.SEM_DESEMBARQUE, 'não comparada')))}
        self.assertEqual(geral, {
            (pc.NO_OPERADOR, pc.COERENTE): '4 (33%)', (pc.NO_OPERADOR, pc.DIGITACAO): '1 (8%)',
            (pc.NO_OPERADOR, pc.MODERADA): '1 (8%)', (pc.NO_OPERADOR, pc.VAZIO_NA_PESAGEM): '1 (8%)',
            (pc.NO_OPERADOR, 'sem dado (sem tara)'): '1 (8%)', (pc.OUTRO_RECINTO, pc.DIVERGENCIA): '1 (8%)',
            (pc.SEM_DESEMBARQUE, 'não comparada'): '1 (8%)'})
        self.assertEqual(self.linha(pc.NO_OPERADOR, 'a carregada é a outra pesagem da visita')[2], '1 (8%)')
        self.assertEqual(self.linha(pc.NO_OPERADOR, f'{pc.COERENTE} — pesagem fora da divisão')[2], '1 (8%)')

    def comeca(self, prefixo):
        achadas = [linha for linha in self.log.splitlines() if linha.startswith(prefixo) and ' | ' in linha]
        self.assertTrue(achadas, f'nenhuma linha começando com {prefixo}')
        return [c.strip() for c in achadas[0].split('|')]

    def test_por_origem_da_tara_e_do_declarado(self):
        # colunas: origem, pesagens, coerente, moderada, digitação, dois, divergência
        self.assertEqual(self.comeca(pc.PAR), [pc.PAR, '6', '5 (83%)', '0 (0%)', '1 (17%)', '0 (0%)', '0 (0%)'])
        self.assertEqual(self.comeca(pc.API), [pc.API, '1', '0 (0%)', '1 (100%)', '0 (0%)', '0 (0%)', '0 (0%)'])
        self.assertEqual(self.comeca(pc.VAZIA), [pc.VAZIA, '1', '0 (0%)', '0 (0%)', '0 (0%)', '0 (0%)', '1 (100%)'])
        self.assertEqual(self.comeca('item do CE')[1], '6')
        self.assertEqual(self.comeca('item do master')[1], '1')
        self.assertEqual(self.comeca('soma de 2 filhotes')[1], '1')

    def test_diferencas(self):
        self.assertIn(f'{pc.OUTRO_RECINTO}: p10 -44 | mediana -44 | p90 -44', self.log)

    def test_exemplos(self):
        for trecho in (
                'MSCU1111111 CE C1 em 10/10 10:00: bruto 40120 − conjunto 15000 (par na visita) − contêiner 2200 '
                '(informada) = 22920 x declarado 22900 (item do CE)',
                'CONS7777777 CE F72 em 15/10 10:00: bruto 41300 − conjunto 15000 (par na visita) − contêiner 2200 '
                '(informada) = 24100 x declarado 24000 (item do master)',
                '= 22000 x declarado 22000 (soma de 2 filhotes)',
                'CAIU3333333 CE C3 em 12/10 08:00: bruto 39000 − conjunto 15000 (taraConjunto da API)',
                '= 21800 x declarado 24000 (item do CE) — -9.2%',
                '(vazia em 7 dias) − contêiner 3800 (típica do tamanho) = 11200 x declarado 20000',
                'PICK1010101 CE C10 em 17/10 08:40: bruto 39000 − conjunto 15000 (par na visita)',
                '= 0 x declarado 4904 (item do CE) — medido 0 kg x declarado 4904 kg',
                '= 23500 x declarado 2350 (item do CE) — ordem de grandeza (×10); +900%'):
            self.assertIn(trecho, self.log)


if __name__ == '__main__':
    unittest.main()
