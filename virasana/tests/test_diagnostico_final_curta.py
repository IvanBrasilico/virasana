# coding: utf-8
"""Testes do DIAGNÓSTICO da "final curta" (diagnostico_final_curta.py) sobre o cenário do operador em papel de TRA
(test_diagnostico_operador_tra), com dois contêineres que passam minutos no ECOPORTO e "somem":

  CFC0001  DI na saída da BTP; 10 min no Ecoporto (número digitado); inspeção com o código do EADI durante a passagem;
           imagem no Mongo 3 h depois; o MESMO caminhão entra no EADI com o número CFC0002 (OCR)          -> H1/H3
  CFC0003  inspecionado na BTP; DUIMP na saída; imagem do escâner da BTP na saída; 6 min no Ecoporto; o caminhão
           não entra em outro recinto; agendamento (G) no EADI; o vazio entra no EADI 4 dias depois     -> H2
Controle: CPAS (40 min no Ecoporto e o mesmo caminhão entra no EADI com o próprio contêiner).
"""
import io
import unittest
from datetime import datetime

from bson import ObjectId
from sqlalchemy import text

from virasana.analytic_scripts.ciclo_de_vida import diagnostico_final_curta as dfc
from virasana.analytic_scripts.ciclo_de_vida import diagnostico_operador_tra as dot
from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
from virasana.tests.test_diagnostico_operador_tra import acrescenta_cenario, linha
from virasana.tests.test_servico_ciclo_de_vida import (
    AGORA, BTP, DESDE, EADI, ECO, Relogio, _evento, _insere, d, monta_banco,
)

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100


def acrescenta_final_curta(engine, mongo):
    acesso = dict(operacao='C', vazioConteiner=False)
    with engine.begin() as c:
        _insere(c, EmbarqueDesembarque, [_evento(511, 'CFC0001', d(11), BTP, embarqueDesembarque='D'),
                                         _evento(512, 'CFC0003', d(11), BTP, embarqueDesembarque='D')])
        _insere(c, AcessoVeiculo, [
            _evento(621, 'CFC0001', d(12, 8), BTP, direcao='S', placa='AAA1A11', tipoDeclaracao='DI', **acesso),
            _evento(622, 'CFC0001', d(12, 9), ECO, direcao='E', placa='AAA1A11', ocrNumero=False, **acesso),
            _evento(623, 'CFC0001', d(12, 9, 10), ECO, direcao='S', placa='AAA1A11', **acesso),
            _evento(624, 'CFC0002', d(12, 10), EADI, direcao='E', placa='AAA1A11', **acesso),   # número lido errado
            _evento(625, 'CFC0003', d(12, 14), BTP, direcao='S', placa='BBB2B22', tipoDeclaracao='DUIMP', **acesso),
            _evento(626, 'CFC0003', d(12, 15), ECO, direcao='E', placa='BBB2B22', **acesso),
            _evento(627, 'CFC0003', d(12, 15, 6), ECO, direcao='S', placa='BBB2B22', **acesso),
            _evento(628, 'CFC0003', d(16), EADI, direcao='E', placa='DDD4D44', operacao='C', vazioConteiner=True),
            _evento(629, 'CFC0003', d(12, 10), EADI, direcao='E', placa='EEE5E55', operacao='G')])
        _insere(c, InspecaoNaoInvasiva, [_evento(711, 'CFC0001', d(12, 9, 5), EADI, vazio=False),
                                         _evento(712, 'CFC0003', d(11, 12), BTP, vazio=False)])
        c.execute(text("UPDATE apirecintos_acessosveiculo SET placa = 'CCC3C33' WHERE id IN (604, 605, 606, 607)"))
    for conteiner, quando in (('CFC0001', d(12, 12, 5)),    # Ecoporto (+3 h): 5 min depois da entrada
                              ('CFC0003', d(12, 17))):      # BTP (+3 h): na saída da BTP, antes do Ecoporto
        mongo['fs.files'].insert_one({'_id': ObjectId(), 'metadata': {'numeroinformado': conteiner,
                                                                      'dataescaneamento': quando}})


class TestDestinoDoCaminhao(unittest.TestCase):

    def test_classes(self):
        def acesso(recinto, numero, direcao='E'):
            return (datetime(2026, 3, 1), recinto, direcao, numero, False)

        self.assertEqual(dfc.destino_do_caminhao([acesso(EADI, 'MSCU1234567')], 'MSCU1234567', ECO),
                         (dfc.COM_ESTE, EADI))
        self.assertEqual(dfc.destino_do_caminhao([acesso(EADI, 'MSCU1234561')], 'MSCU1234567', ECO),
                         (dfc.PARECIDO, EADI))
        self.assertEqual(dfc.destino_do_caminhao([acesso(EADI, 'TGHU7654321')], 'MSCU1234567', ECO), (dfc.OUTRO, EADI))
        self.assertEqual(dfc.destino_do_caminhao([acesso(EADI, '')], 'MSCU1234567', ECO), (dfc.SEM_CONTEINER, EADI))
        # saída, ou entrada no próprio recinto da passagem, não contam
        self.assertEqual(dfc.destino_do_caminhao([acesso(EADI, 'MSCU1234567', 'S'), acesso(ECO, 'MSCU1234567')],
                                                 'MSCU1234567', ECO), (dfc.NAO_ENTROU, None))


class TestMomentoDaImagem(unittest.TestCase):
    """Ecoporto (+3 h): entrada 09:00, saída 09:10; BTP (+3 h): saída 06:00."""

    ESTADA = dot.Estada(ECO, True, d(12, 9), saida=d(12, 9, 10))
    ORIGEM = dot.Estada(BTP, True, d(11), saida=d(12, 6))

    def momento(self, *imagens):
        return dfc.momento_da_imagem(list(imagens), self.ESTADA, self.ORIGEM)

    def test_momentos(self):
        self.assertEqual(self.momento(d(12, 12, 5)), (dfc.DURANTE, 5.0))
        self.assertEqual(self.momento(d(12, 9)), (dfc.NA_ORIGEM, None))            # 06:00 sem a defasagem
        self.assertEqual(self.momento(d(12, 11)), (dfc.OUTRO_MOMENTO, None))       # 08:00: entre as duas
        self.assertEqual(self.momento(d(14, 11)), (dfc.SEM_IMAGEM, None))
        self.assertEqual(self.momento(), (dfc.SEM_IMAGEM, None))


class TestRelatorio(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        engine, mongo = monta_banco()
        acrescenta_cenario(engine)
        acrescenta_final_curta(engine, mongo)
        sv.Servico(engine, mongo, relogio=Relogio(AGORA)).uma_volta(DESDE)
        saida = io.StringIO()
        with engine.connect() as conexao:
            cls.diag = dfc.DiagnosticoFinalCurta(conexao, saida, d(1), d(15), AGORA, mongo_db=mongo)
            dfc.roda(cls.diag)
        cls.texto = saida.getvalue()

    def test_casos(self):
        self.assertNotIn('ERRO', self.texto)
        self.assertEqual(sorted(c.conteiner for c in self.diag.curtas), ['CFC0001', 'CFC0003'])
        self.assertEqual([c.conteiner for c in self.diag.controle], ['CPAS'])
        self.assertIn('Final curta por recinto: 8931318 ECOPORTO 2', self.texto)

    def test_comparacao(self):
        for celulas in (('casos', 2, 1),
                        ('estada em até 30 min', '2 (100%)', '0 (0%)'),
                        ('inspeção na origem antes de sair', '1 (50%)', '0 (0%)'),
                        ('inspeção com o código do recinto da estada', '0 (0%)', '1 (100%)'),
                        ('inspeção com OUTRO código (−2 h a +6 h)', '1 (50%)', '0 (0%)'),
                        ('imagem no Mongo no horário da passagem', '2 (100%)', '0 (0%)'),
                        ('número digitado na entrada (sem OCR)', '1 (50%)', '0 (0%)'),
                        ('mesmo caminhão da saída da origem', '2 (100%)', '1 (100%)'),
                        ('agendamento (G) noutro recinto', '1 (50%)', '0 (0%)'),
                        ('caminhão: entrou com este contêiner', '0 (0%)', '1 (100%)'),
                        ('caminhão: entrou com número parecido (OCR?)', '1 (50%)', '0 (0%)'),
                        ('caminhão: não entrou em recinto da API em 6 h', '1 (50%)', '0 (0%)')):
            with self.subTest(indicador=celulas[0]):
                self.assertTrue(linha(self.texto, *celulas), self.texto)

    def test_momento_da_imagem(self):
        for celulas in (('durante a passagem', '1 (50%)', '0 (0%)'),
                        ('na saída do operador de origem', '1 (50%)', '0 (0%)'),
                        ('sem imagem no horário', '0 (0%)', '1 (100%)'),
                        ('"durante": minutos depois da entrada (mediana)', '+5 min', '-'),
                        ('8931359 BTP → 8931318 ECOPORTO', 2, 1, 1, 0, 0)):
            with self.subTest(momento=celulas[0]):
                self.assertTrue(linha(self.texto, *celulas), self.texto)

    def test_declaracao_caminhao_e_depois(self):
        self.assertTrue(linha(self.texto, 'DI', 1, 0, 0))
        self.assertTrue(linha(self.texto, 'DUIMP', 1, 0, 0))
        self.assertTrue(linha(self.texto, '(nenhuma)', 0, 2, 1))
        self.assertTrue(linha(self.texto, '8932000 EADI', 1, 0, 1, 0, 0))
        self.assertTrue(linha(self.texto, '(nenhum)', 1, 0, 0, 0, 0))
        self.assertTrue(linha(self.texto, 'entrada com o contêiner vazio', 1, '3.7'))
        self.assertTrue(linha(self.texto, 'eventos até hoje', 1, '7.6'))

    def test_ces_e_exemplos(self):
        self.assertIn('1 CEs distintos; 0 com 2 ou mais contêineres', self.texto)
        self.assertTrue(linha(self.texto, 'operador/concluido — entregue pelo operador', 1))
        self.assertIn('--- CFC0001: caminhão entrou com número parecido (OCR?) em 8932000 EADI; declaração na saída da '
                      'origem: DI', self.texto)
        self.assertIn('--- CFC0003: caminhão não entrou em recinto da API em 6 h; declaração na saída da origem: DUIMP; '
                      'agendamento em 8932000', self.texto)


if __name__ == '__main__':
    unittest.main()
