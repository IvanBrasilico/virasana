"""Diagnóstico dos sinais de declaração inventada (analytic_scripts/declaracao/diagnostico_declaracao.py), em SQLite.

Janela: CEs criados de 23/09 a 23/10/2026 (pela janela de ID). Limiares reduzidos para uma base pequena.
  - 40 BLs normais da EMP1 (MAEU..., origem CNSHA, 20' dry, NCM 84713012, ~10 t, 25 m³, embalagem 01) e 35 MBLs da
    HAP (HLCUSHA..., 20' dry, NCM 61091000, ~12 t);
  - CUBA: 41,5 m³ num 20' dry (16a); PESA: 35 t (16b e 18a); TANQ: tanque com máquina (16c); EMBA: embalagem 99
    com máquina (16d); ZZZU: prefixo raro da EMP1 (17a); NGB: HLCU com cidade NGB e origem CNSHA (17b);
  - família fora da janela: o HBL F1 (na janela) e o master M0 (agosto) — o peso do contêiner é o do master;
    cadeia MBL M3 (agosto) > MHBL H3 > HBL F3; uso parcial (fora); um CE de exportação (fora);
  - referências: ficha positiva no PESA e noutra fora da janela (P0, cubagem impossível: 16a); RVF no PESA com a
    infração "Falsa declaração" e peso da RVF 3× o declarado; pesagem "divergencia" no PESA.
"""
from virasana.navii.busca_navii import tabela_recintos  # noqa: F401 — 1º: ajusta o sys.path
import csv  # noqa: I100
import io
import os
import tempfile
import unittest
from datetime import datetime
from unittest import mock

from sqlalchemy import create_engine, text

from virasana.analytic_scripts.ciclo_de_vida.gravacao_ciclo import PESAGENS
from virasana.analytic_scripts.declaracao import diagnostico_declaracao as dd
from virasana.analytic_scripts.declaracao import regras_declaracao as rd
from virasana.integracao.mercante.mercantealchemy import Conhecimento, Item, NCMItem

DESDE, ATE = datetime(2026, 9, 23), datetime(2026, 10, 23)
DDL = [
    'CREATE TABLE ovr_ovrs (id INTEGER PRIMARY KEY, numero VARCHAR(10), datahora TIMESTAMP, tipooperacao INTEGER, '
    'numeroCEmercante VARCHAR(15), cnpj_fiscalizado VARCHAR(15), observacoes VARCHAR(1000))',
    'CREATE TABLE ovr_resultados (id INTEGER PRIMARY KEY, ovr_id INTEGER, tipo_resultado INTEGER)',
    'CREATE TABLE ovr_tgovr (id INTEGER PRIMARY KEY, ovr_id INTEGER, valor NUMERIC)',
    'CREATE TABLE ovr_verificacoesfisicas (id INTEGER PRIMARY KEY, ovr_id INTEGER, numeroCEmercante VARCHAR(15), '
    'peso NUMERIC, volume NUMERIC)',
    'CREATE TABLE ovr_infracoes (id INTEGER PRIMARY KEY, nome VARCHAR(50))',
    'CREATE TABLE ovr_infracoesencontradas (rvf_id INTEGER, infracao_id INTEGER)',
]
LIMIARES = {'MINIMO_DO_CAPITULO': 10, 'FRACAO_PAR_RARO': 0.05, 'MINIMO_DA_EMPRESA': 10, 'PREFIXO_RARO': 0.05,
            'AMOSTRAS_MINIMAS': 5}


def ce(n: int) -> str:
    return f'1526050{n:08d}'


class Base:
    """Monta os CEs na ordem dos IDs (a data de criação cresce com o ID)."""

    def __init__(self, con):
        self.con, self.n = con, 0

    def ce(self, numero, criado, conteineres, tipo_bl='10', master=None, empresa='EMP1', bl=None, origem='CNSHA',
           trafego='5'):
        """conteineres: [(contêiner, tipo, peso, cubagem, [(ncm, embalagem)], uso parcial)]"""
        self.n += 1
        self.con.execute(Conhecimento.__table__.insert(), {
            'numeroCEmercante': numero, 'numeroCEMaster': master, 'tipoBLConhecimento': tipo_bl,
            'tipoTrafego': trafego, 'codigoEmpresaNavegacao': empresa, 'portoOrigemCarga': origem,
            'numConhecimento': bl or f'MAEU{self.n:09d}', 'cubagem': '25', 'create_date': criado})
        for conteiner, tipo, peso, cubagem, ncms, uso in conteineres:
            self.con.execute(Item.__table__.insert(), {
                'numeroCEmercante': numero, 'codigoConteiner': conteiner, 'isoCode': tipo, 'pesoBruto': peso,
                'cubagemM3': cubagem, 'codigoTipoEmbalagem': '', 'indicadorUsoParcial': uso})
            for codigo, embalagem in ncms:
                self.con.execute(NCMItem.__table__.insert(), {
                    'numeroCEMercante': numero, 'codigoConteiner': conteiner, 'identificacaoNCM': codigo,
                    'codigoTipoEmbalagem': embalagem})


def maquina(conteiner, peso='10000', cubagem='25', tipo='22G1', embalagem='01', uso='N'):
    return [(conteiner, tipo, peso, cubagem, [('84713012', embalagem)], uso)]


def monta(engine):
    for tabela in (Conhecimento, Item, NCMItem):
        tabela.__table__.create(engine)
    PESAGENS.create(engine)
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        b, agosto, dia = Base(con), datetime(2026, 8, 1), datetime(2026, 10, 1)
        b.ce(ce(900), agosto, [('MMMU0000001', '22G1', '20000', '30', [], 'N')], tipo_bl='11')        # M0
        b.ce(ce(901), agosto, [('CCCU3333333', '22G1', '22000', '28', [], 'N')], tipo_bl='11')        # M3
        b.ce(ce(902), agosto, maquina('POSU0000002', cubagem='50'))                                    # P0
        for n in range(40):
            b.ce(ce(n), dia, maquina(f'NORU{n:07d}', peso=str(10000 + 10 * n)))
        for n in range(35):
            b.ce(ce(100 + n), dia, [(f'HAPU{n:07d}', '22G1', str(12000 + n), '25', [('61091000', '02')], 'N')],
                 tipo_bl='11', empresa='HAP', bl=f'HLCUSHA{n:06d}')
        b.ce(ce(200), dia, maquina('CUBU0000001', cubagem='41,5'))                                     # CUBA
        b.ce(ce(201), dia, maquina('PESU0000001', peso='35000'))                                       # PESA
        b.ce(ce(202), dia, maquina('TANU0000001', tipo='22K1'))                                        # TANQ
        b.ce(ce(203), dia, maquina('EMBU0000001', embalagem='99'))                                     # EMBA
        b.ce(ce(204), dia, maquina('ZZZU0000001'), bl='ZZZU000000001')                                 # ZZZU
        b.ce(ce(205), dia, [('NGBU0000001', '22G1', '12000', '25', [('61091000', '02')], 'N')],         # NGB
             tipo_bl='11', empresa='HAP', bl='HLCUNGB000001')
        b.ce(ce(206), dia, [('MMMU0000001', '', '8000', '10', [('61091000', '02')], 'N')], tipo_bl='12',
             master=ce(900))                                                                           # F1
        b.ce(ce(207), dia, [('CCCU3333333', '', '22000', '28', [], 'N')], tipo_bl='15', master=ce(901))  # H3
        b.ce(ce(208), dia, [('CCCU3333333', '', '22000', '28', [('61091000', '02')], 'N')], tipo_bl='12',
             master=ce(207))                                                                           # F3
        b.ce(ce(209), dia, maquina('LCLU0000001', uso='S'))                                            # uso parcial
        b.ce(ce(210), dia, maquina('EXPU0000001', cubagem='99'), trafego='7')                          # exportação
        con.execute(text('INSERT INTO ovr_ovrs (id, numero, datahora, tipooperacao, numeroCEmercante) VALUES '
                         f"(1, '1', '2026-10-10', 1, '{ce(201)}'), (2, '2', '2025-01-10', 1, '{ce(902)}')"))
        con.execute(text('INSERT INTO ovr_resultados (id, ovr_id, tipo_resultado) VALUES (1, 1, 2), (2, 2, 6)'))
        con.execute(text('INSERT INTO ovr_verificacoesfisicas (id, ovr_id, numeroCEmercante, peso, volume) VALUES '
                         f"(10, 1, '{ce(201)}', 105000, 25), (11, NULL, '{ce(0)}', 9500, 0)"))
        con.execute(text("INSERT INTO ovr_infracoes (id, nome) VALUES (1, 'Falsa declaração')"))
        con.execute(text('INSERT INTO ovr_infracoesencontradas (rvf_id, infracao_id) VALUES (10, 1)'))
        con.execute(PESAGENS.insert(), {'ciclo_de_vida_id': 1, 'pesagem_id': 1, 'data': dia, 'operacao': 'retirada',
                                        'principal': True, 'numeroCEmercante': ce(201), 'diagnostico': 'divergencia'})


class TestDiagnosticoDeclaracao(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('sqlite://')
        monta(cls.engine)
        cls.saida = io.StringIO()
        with mock.patch.multiple(rd, **LIMIARES), cls.engine.connect() as conexao:
            cls.diag = dd.DiagnosticoDeclaracao(conexao, cls.saida, DESDE, ATE, bloco=20, progresso=lambda _: None)
            cls.csv = os.path.join(tempfile.mkdtemp(), 'declaracao.csv')
            cls.ok = dd.roda(cls.diag, cls.csv)
        cls.log = cls.saida.getvalue()

    def marcados(self, sinal):
        return set(self.diag.marcas[sinal])

    def test_populacao_pela_janela_de_id_sem_exportacao_e_com_as_familias(self):
        self.assertTrue(self.ok)
        self.assertEqual(self.diag.janela[0], 4)                          # os 3 de agosto ficam antes
        self.assertEqual(len(self.diag.populacao), 75 + 10)               # sem a exportação
        self.assertNotIn(ce(210), self.diag.ces)
        self.assertIn(ce(900), self.diag.ces)                             # master fora da janela, lido pela família
        por_conteiner = {c.conteiner: c for c in self.diag.conteineres}
        f1 = por_conteiner['MMMU0000001']
        self.assertEqual((f1.familia, f1.ces, f1.peso, f1.origem_peso), (ce(900), (ce(206), ce(900)), 20000.0,
                                                                         'item do master'))
        cadeia = por_conteiner['CCCU3333333']
        self.assertEqual((cadeia.familia, cadeia.ces), (ce(901), (ce(207), ce(208), ce(901))))
        self.assertTrue(por_conteiner['LCLU0000001'].uso_parcial)

    def test_sinais(self):
        self.assertEqual(self.marcados(rd.OCUPACAO), {ce(200), ce(902)})   # o P0 veio pelas referências
        self.assertEqual(self.marcados(rd.CARGA_MAXIMA), {ce(201)})
        self.assertEqual(self.marcados(rd.TIPO_CARGA), {ce(202)})
        self.assertEqual(self.marcados(rd.EMBALAGEM_CARGA), {ce(203)})
        self.assertEqual(self.marcados(rd.PREFIXO), {ce(204)})
        self.assertEqual(self.marcados(rd.CIDADE), {ce(205)})
        self.assertIn(ce(201), self.marcados(rd.PESO_NCM))
        self.assertNotIn(ce(209), set().union(*(self.marcados(s) for s in rd.SINAIS)))   # uso parcial: fora

    def test_referencias(self):
        refs = self.diag.referencias
        self.assertEqual(refs['fichas positivas'], {ce(201), ce(902)})
        self.assertEqual(refs['RVF: todas'], {ce(201), ce(0)})
        self.assertEqual(refs['RVF com a infração "Falsa declaração"'], {ce(201)})
        self.assertEqual(refs['RVF com peso fora de 0,8–1,2 do declarado'], {ce(201)})   # 105 t × 35 t
        self.assertEqual(refs['pesagem: divergencia'], {ce(201)})
        self.assertEqual(self.diag.razoes_rvf['volume']['sem medida na RVF'], 1)

    def test_relatorio(self):
        for trecho in ('(SHOW INDEX só no MariaDB/MySQL)', '1. POPULAÇÃO, FAMÍLIAS E TEMPO',
                       'estimativa da população em 90 dias', '16. TIPO DO CONTÊINER', 'Cubagens mais frequentes',
                       'Pares raros carga × tipo do contêiner: 1', '17. NÚMERO DO BL', 'HLCU', '18. PESO × VOLUME',
                       'V. VALIDAÇÃO', 'Falsa declaração', 'A. AMOSTRAS', 'C. CONSULTAS MAIS LENTAS'):
            with self.subTest(trecho=trecho):
                self.assertIn(trecho, self.log)
        self.assertNotIn('!! ERRO', self.log)
        linha = next(linha for linha in self.log.splitlines() if linha.startswith('fichas positivas ')
                     and '|' in linha and linha.count('|') == 4)
        self.assertEqual([c.strip() for c in linha.split('|')][1:3], ['2', '1'])   # 16a: só o P0 (o PESA é peso)

    def test_csv(self):
        with open(self.csv, encoding='utf-8') as arquivo:
            linhas = list(csv.DictReader(arquivo, delimiter=';'))
        pesa = [linha for linha in linhas if linha['ce'] == ce(201) and linha['sinal'] == rd.CARGA_MAXIMA]
        self.assertEqual(len(pesa), 1)
        self.assertEqual(pesa[0]['na_janela'], 'sim')
        self.assertIn('fichas positivas', pesa[0]['referencias'])

    def test_para_sem_indice(self):
        diag = dd.DiagnosticoDeclaracao(mock.Mock(), io.StringIO(), DESDE, ATE)
        diag.mysql = True
        diag.consulta = mock.Mock(return_value=[('itensresumo', 1, 'PRIMARY', 1, 'ID')])
        self.assertFalse(dd.roda(diag))
        self.assertIn('o diagnóstico para aqui', diag.saida.getvalue())


class TestFormatos(unittest.TestCase):

    def test_empresa_e_tipo(self):
        self.assertEqual([dd.formato_empresa(v) for v in ('', '12345678000199', '123', 'MSC', 'MSC1')],
                         ['(vazio)', 'CNPJ (14 dígitos)', '3 dígitos', 'só letras', 'letras e dígitos'])
        self.assertEqual([dd.situacao_tipo(v) for v in ('', 'XPTO', '22G1', '22T0')],
                         ['(vazio)', 'não reconhecido', 'reconhecido, com capacidade de referência',
                          'reconhecido, sem capacidade de referência'])


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
