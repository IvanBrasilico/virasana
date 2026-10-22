"""Testes do diagnóstico do porto de cada operador e do CE declarado no desembarque
(`diagnostico_porto_desembarque.py`, 21/10/2026).

Cenário (o caso CAAU5545018 reduzido, mais casos normais):
  SBP 8931356 (operador, cod_carga BRSSZ016), ECO 8931318 (operador, sem cod_carga), MRIO 7921303 (operador,
  cod_carga BRRIO002, sem eventos), CLIA 8933206 (não operador, sem eventos);
  escalas ES (BRSSZ, navio 9605279) e EC (BRSSZ, navio 9612789, a da cabotagem);
  IMP: importação EGDAM -> BRSSZ, manifesto MI SEM escala (navio 9605279, início 07/10), contêineres CAAU e FANU;
  CAB: cabotagem BRSSZ -> BRSUA (manifesto MC na escala EC), contêiner CAAU;
  IMP2: importação com o manifesto MI2 na escala ES, contêiner C1; EXP: exportação BRSSZ -> SGSIN, contêiner C3;
  MASTER e FILHOTE (filhote do MASTER, manifesto MI2): contêiner C8;
  eventos na SBP: desembarques de CAAU, FANU (declaram IMP), C1 (declara IMP2), C2 (sem CE) e C8 (declara o MASTER);
  embarque de C3 (declara EXP); na ECO, dois desembarques sem CE na escala ES;
  ciclos gravados (com a família): CAAU com o CAB (o defeito), FANU com o IMP, C1 com o IMP2, C8 com o FILHOTE
  (família FILHOTE + MASTER: o declarado é da família).
"""
import unittest
from datetime import datetime

from sqlalchemy import create_engine, text

from virasana.analytic_scripts.ciclo_de_vida import diagnostico_porto_desembarque as dp
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import ConsultaSQL
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import Saida
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Escala, EscalaManifesto, Item, Manifesto
from virasana.navii.busca_navii import tabela_recintos

from bhadrasana.models.apirecintos import EmbarqueDesembarque  # noqa: I100

IMP, CAB, IMP2, EXP = '152605316468892', '072603341989805', '152600000000002', '152600000000007'
MASTER, FILHOTE = '152600000000010', '152600000000011'
SBP, ECO, MRIO = '8931356', '8931318', '7921303'


def _insere(conexao, modelo, linhas):
    for linha in linhas:
        conexao.execute(modelo.__table__.insert(), linha)


def _evento(id_, conteiner, dia, recinto, tipo='D', ce=None, escala='ES'):
    return dict(id=id_, numeroConteiner=conteiner, dataHoraOcorrencia=datetime(2026, 10, dia, 16),
                codigoRecinto=recinto, embarqueDesembarque=tipo, numeroConhecimento=ce, escala=escala)


def banco():
    engine = create_engine('sqlite://')
    for modelo in (Conhecimento, Item, Escala, EscalaManifesto, Manifesto, EmbarqueDesembarque):
        modelo.__table__.create(engine)
    tabela_recintos.create(engine)
    gc.metadata.create_all(engine)
    with engine.begin() as c:
        c.execute(text('ALTER TABLE ovr_recintos ADD COLUMN e_operador BOOLEAN'))
        c.execute(text('ALTER TABLE ovr_recintos ADD COLUMN cod_unidade VARCHAR(7)'))
        c.execute(text('INSERT INTO ovr_recintos (id, nome, cod_siscomex, e_operador, cod_unidade, cod_carga) VALUES '
                       "(1, 'SANTOS BRASIL', '8931356', 1, '0817800', 'BRSSZ016'), "
                       "(2, 'ECOPORTO', '8931318', 1, '0817800', NULL), "
                       "(3, 'MULTIRIO', '7921303', 1, '0717600', 'BRRIO002'), "
                       "(4, 'CLIA', '8933206', 0, '0817800', NULL)"))
        _insere(c, Escala, [dict(numeroDaEscala='ES', portoDaEscala='BRSSZ', embarcacao='9605279',
                                 dataPrevistaAtracacao='2026-10-07'),
                            dict(numeroDaEscala='EC', portoDaEscala='BRSSZ', embarcacao='9612789',
                                 dataPrevistaAtracacao='2026-10-15')])
        _insere(c, Conhecimento, [
            dict(numeroCEmercante=IMP, tipoTrafego='5', portoOrigemCarga='EGDAM', portoDestFinal='BRSSZ',
                 manifestoCE='MI'),
            dict(numeroCEmercante=CAB, tipoTrafego='3', portoOrigemCarga='BRSSZ', portoDestFinal='BRSUA',
                 manifestoCE='MC'),
            dict(numeroCEmercante=IMP2, tipoTrafego='5', portoOrigemCarga='CNSHA', portoDestFinal='BRSSZ',
                 manifestoCE='MI2'),
            dict(numeroCEmercante=EXP, tipoTrafego='7', portoOrigemCarga='BRSSZ', portoDestFinal='SGSIN',
                 manifestoCE='ME'),
            dict(numeroCEmercante=MASTER, tipoTrafego='5', portoOrigemCarga='CNSHA', portoDestFinal='BRSSZ',
                 manifestoCE='MI2'),
            dict(numeroCEmercante=FILHOTE, numeroCEMaster=MASTER, tipoTrafego='5', portoOrigemCarga='CNSHA',
                 portoDestFinal='BRSSZ', manifestoCE='MI2')])
        _insere(c, Item, [dict(numeroCEmercante=ce, codigoConteiner=conteiner) for ce, conteiner in (
            (IMP, 'CAAU'), (IMP, 'FANU'), (CAB, 'CAAU'), (IMP2, 'C1'), (EXP, 'C3'), (MASTER, 'C8'), (FILHOTE, 'C8'))])
        _insere(c, Manifesto, [
            dict(numero='MI', portoCarregamento='ESBCN', portoDescarregamento='BRSSZ',
                 codigoTerminalDescarregamento='BRSSZ016', numeroImoDPC='9605279', dataInicioOperacao='2026-10-07'),
            dict(numero='MC', portoCarregamento='BRSSZ', portoDescarregamento='BRSUA',
                 codigoTerminalCarregamento='BRSSZ009', numeroImoDPC='9612789', dataInicioOperacao='2026-10-15'),
            dict(numero='MI2', portoCarregamento='CNSHA', portoDescarregamento='BRSSZ',
                 codigoTerminalDescarregamento='BRSSZ016', numeroImoDPC='9605279', dataInicioOperacao='2026-10-07'),
            dict(numero='ME', portoCarregamento='BRSSZ', portoDescarregamento='SGSIN',
                 codigoTerminalCarregamento='BRSSZ016', dataInicioOperacao='2026-10-08')])
        _insere(c, EscalaManifesto, [dict(ID=1, manifesto='MC', escala='EC'), dict(ID=2, manifesto='MI2', escala='ES'),
                                     dict(ID=3, manifesto='ME', escala='ES')])
        _insere(c, EmbarqueDesembarque, [
            _evento(1, 'CAAU', 7, SBP, ce=IMP), _evento(2, 'FANU', 7, SBP, ce=IMP), _evento(3, 'C1', 7, SBP, ce=IMP2),
            _evento(4, 'C2', 7, SBP), _evento(5, 'C3', 8, SBP, tipo='E', ce=EXP),
            _evento(6, 'C4', 7, ECO), _evento(7, 'C5', 7, ECO),
            _evento(8, 'C9', 1, SBP, ce=IMP),   # antes do período
            _evento(9, 'C8', 7, SBP, ce=MASTER)])
        gravados = ((1, CAB, 'media', [CAB]), (2, IMP, 'media', [IMP]), (3, IMP2, 'alta', [IMP2]),
                    (9, FILHOTE, 'alta', [FILHOTE, MASTER]))
        for id_, (desembarque, ce, confianca, familia) in enumerate(gravados, start=1):
            c.execute(gc.CICLOS.insert(), dict(ID=id_, desembarque_id=desembarque, numeroCEmercante=ce,
                                               confianca_ce=confianca, versao_heuristica='v2'))
            for membro in familia:
                c.execute(gc.CICLO_CE.insert(), dict(ciclo_de_vida_id=id_, numeroCEmercante=membro, papel='simples',
                                                     principal=membro == ce))
    return engine


class TestDiagnosticoPorto(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.saida = Saida()
        with banco().connect() as conexao:
            cls.resultado = dp.diagnostica(ConsultaSQL(conexao), cls.saida, datetime(2026, 10, 5),
                                           datetime(2026, 10, 10), minimo=2)
        cls.texto = '\n'.join(cls.saida.linhas)
        cls.sbp = cls.resultado['contagens'][SBP]

    def test_porto_proposto_por_recinto(self):
        self.assertEqual(self.resultado['portos'], {SBP: 'BRSSZ', ECO: 'BRSSZ', MRIO: 'BRRIO'})
        self.assertIn('PORTO PROPOSTO: BRSSZ — escala (100% de 6), = cod_carga', self.texto)
        self.assertIn('PORTO PROPOSTO: BRSSZ — escala (100% de 2)', self.texto)
        self.assertIn('PORTO PROPOSTO: BRRIO — cod_carga (sem eventos suficientes para conferir)', self.texto)
        self.assertIn('terminal do manifesto: BRSSZ016 100% de 5', self.texto)

    def test_ce_declarado_no_desembarque(self):
        esperado = {'D desembarques': 5, 'D com CE declarado': 4, 'D sem CE declarado': 1, 'D CE no Mercante': 4,
                    'D contêiner nos itens do CE': 4, 'D manifesto SEM escala no Mercante': 2,
                    'D sem escala: navio do manifesto = navio da escala declarada': 2,
                    'D sem escala: início da operação do manifesto a até 3 dias': 2,
                    'D manifesto da escala declarada': 2, 'D ciclo gravado': 4, 'D principal = declarado': 2,
                    'D principal ≠ declarado': 2, 'D declarado na família gravada (master/filhote)': 1,
                    'D declarado FORA da família gravada': 1, 'D principal de SAÍDA do porto': 1,
                    'D principal de SAÍDA e o declarado não (o caso CAAU)': 1}
        self.assertEqual({k: v for k, v in self.sbp.items() if k.startswith('D ')}, esperado)
        self.assertIn('D principal de SAÍDA do porto: 1 (20.0%)', self.texto)

    def test_ce_declarado_no_embarque_e_de_saida(self):
        self.assertEqual(self.sbp['E embarques'], 1)
        self.assertEqual(self.sbp['E CE de SAÍDA do porto (origem = porto do recinto)'], 1)
        self.assertEqual(self.resultado['contagens']['TOTAL']['D desembarques'], 7)

    def test_defeito_do_caso(self):
        self.assertEqual(len(self.resultado['defeitos']), 1)
        defeito = self.resultado['defeitos'][0]
        self.assertEqual((defeito['conteiner'], defeito['principal'], defeito['declarado'], defeito['porto']),
                         ('CAAU', CAB, IMP, 'BRSSZ'))
        self.assertEqual(defeito['categorias'], 'principal de SAÍDA do porto + declarado FORA da família gravada')
        self.assertTrue(defeito['declarado_tem_o_conteiner'])
        self.assertTrue(defeito['manifesto_declarado_sem_escala'])
        self.assertIn('principal de SAÍDA do porto + declarado FORA da família gravada; confiança media; tráfego do '
                      'principal 3; declarado com o contêiner nos itens True: 1', self.texto)


class TestProposta(unittest.TestCase):

    def test_divergencia_com_o_cadastro_e_concordancia_baixa(self):
        from collections import Counter
        evid = {'escala': Counter({'BRRIO': 30}), 'manifesto': Counter()}
        self.assertEqual(dp.proposta({'cod_carga': 'BRSSZ016'}, evid),
                         ('BRSSZ', 'cod_carga, DIVERGE da escala (BRRIO, 100% de 30)'))
        dividido = {'escala': Counter({'BRSSZ': 90, 'BRRIO': 10}), 'manifesto': Counter({'BRSSZ': 25})}
        self.assertEqual(dp.proposta({'cod_carga': ''}, dividido), ('BRSSZ', 'manifesto (100% de 25)'))
        self.assertEqual(dp.proposta({}, {'escala': Counter({'BRSSZ': 5}), 'manifesto': Counter()}),
                         (None, 'indefinido'))

    def test_porto_do_codigo(self):
        self.assertEqual(dp.porto_do_codigo('BRSSZ016'), 'BRSSZ')
        self.assertIsNone(dp.porto_do_codigo('T01'))
        self.assertIsNone(dp.porto_do_codigo(None))


if __name__ == '__main__':
    unittest.main()
