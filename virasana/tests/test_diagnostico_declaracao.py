"""Diagnóstico dos sinais de declaração inventada (analytic_scripts/declaracao/diagnostico_declaracao.py), em SQLite.

Janela: CEs criados de 23/09 a 23/10/2026 (pela janela de ID). Limiares reduzidos para uma base pequena.
  - 40 BLs normais da EMP1 (MAEU..., 20' dry, NCM 84713012, ~10 t, 25 m³) e 35 MBLs da HAP (HLCUSHA..., NCM 61091000,
    ~12 t);
  - CUBA: 41,5 m³ num 20' dry (16a, qualidade); PESA: 25 t e 25,5 m³ (18a+ e 18b+ com cubagem com fração);
    PEZO (o último): 35 t num 20' (16b — peso impossível: fora do 18); REPE: 3 contêineres com o
    mesmo NCM e a mesma cubagem, pesos de 10 a 20 t (16e); ZZZU: prefixo raro na EMP1, de prefixo dominante (17a);
  - família fora da janela: o HBL F1 (na janela) e o master M0 (agosto) — o peso do contêiner é o do master;
    cadeia MBL M3 (agosto) > MHBL H3 > HBL F3; uso parcial (fora); um CE de exportação (fora);
  - referências: ficha positiva no PESA e noutra fora da janela (P0, ID 3, cubagem impossível); RVF no PESA com a
    infração "Falsa declaração" e peso da RVF 3× o declarado, e RVF no NORU 0; pesagem "divergencia" no PESA;
  - controle da mesma época com deslocamentos (+1, +2): P0 (ID 3) -> NORU 0 (é referência: fica fora) e NORU 1;
    PESA (ID 80) -> REPE e ZZZU;
  - SIMB (o último): 1 m³ num 20' (cubagem simbólica: 18b+ redondo, fora da lista de revisão);
  - lista de revisão: o 18b+ com cubagem em m³ inteiro e plausível — a cadeia M3 (28 m³, 786 kg/m³ × 480 da HAP).
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
LIMIARES = {'MINIMO_DA_EMPRESA': 10, 'PREFIXO_RARO': 0.05, 'AMOSTRAS_MINIMAS': 5}


def ce(n: int) -> str:
    return f'1526050{n:08d}'


class Base:
    """Monta os CEs na ordem dos IDs (a data de criação cresce com o ID)."""

    def __init__(self, con):
        self.con, self.n = con, 0

    def ce(self, numero, criado, conteineres, tipo_bl='10', master=None, empresa='EMP1', bl=None, trafego='5'):
        """conteineres: [(contêiner, tipo, peso, cubagem, [ncm], uso parcial)]"""
        self.n += 1
        self.con.execute(Conhecimento.__table__.insert(), {
            'numeroCEmercante': numero, 'numeroCEMaster': master, 'tipoBLConhecimento': tipo_bl,
            'tipoTrafego': trafego, 'codigoEmpresaNavegacao': empresa, 'portoOrigemCarga': 'CNSHA',
            'numConhecimento': bl or f'MAEU{self.n:09d}', 'cubagem': '25.000', 'create_date': criado,
            'consignatario': '11222333000181' if empresa == 'EMP1' else '44555666000199'})
        for conteiner, tipo, peso, cubagem, ncms, uso in conteineres:
            self.con.execute(Item.__table__.insert(), {
                'numeroCEmercante': numero, 'codigoConteiner': conteiner, 'isoCode': tipo, 'pesoBruto': peso,
                'cubagemM3': cubagem, 'codigoTipoEmbalagem': '', 'indicadorUsoParcial': uso})
            for codigo in ncms:
                self.con.execute(NCMItem.__table__.insert(), {
                    'numeroCEMercante': numero, 'codigoConteiner': conteiner, 'identificacaoNCM': codigo,
                    'codigoTipoEmbalagem': '99', 'qtdeVolumes': '10.000'})


def maquina(conteiner, peso='10000.000', cubagem='25.000', tipo='22G1', uso='false'):
    return [(conteiner, tipo, peso, cubagem, ['84713012'], uso)]


def monta(engine):
    for tabela in (Conhecimento, Item, NCMItem):
        tabela.__table__.create(engine)
    PESAGENS.create(engine)
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        b, agosto, dia = Base(con), datetime(2026, 8, 1), datetime(2026, 10, 1)
        b.ce(ce(900), agosto, [('MMMU0000001', '22G1', '20000.000', '30.000', [], 'false')], tipo_bl='11')   # M0
        b.ce(ce(901), agosto, [('CCCU3333333', '22G1', '22000.000', '28.000', [], 'false')], tipo_bl='11')   # M3
        b.ce(ce(902), agosto, maquina('POSU0000002', cubagem='50.000'))                               # P0, ID 3
        for n in range(40):
            b.ce(ce(n), dia, maquina(f'NORU{n:07d}', peso=f'{10000 + 10 * n}.000'))
        for n in range(35):
            b.ce(ce(100 + n), dia, [(f'HAPU{n:07d}', '22G1', f'{12000 + n}.000', '25.000', ['61091000'], 'false')],
                 tipo_bl='11', empresa='HAP', bl=f'HLCUSHA{n:06d}')
        b.ce(ce(200), dia, maquina('CUBU0000001', cubagem='41.500'))                                  # CUBA
        b.ce(ce(201), dia, maquina('PESU0000001', peso='25000.000', cubagem='25.500'))                # PESA, ID 80
        b.ce(ce(202), dia, [(f'REPU000000{n}', '45G1', peso, '68.000', ['39011000'], 'false')          # REPE
                            for n, peso in enumerate(('10000.000', '15000.000', '20000.000'))])
        b.ce(ce(203), dia, maquina('ZZZU0000001'), bl='ZZZU000000001')                                # ZZZU
        b.ce(ce(204), dia, [('MMMU0000001', '', '8000.000', '10.000', ['61091000'], 'false')], tipo_bl='12',
             master=ce(900))                                                                          # F1
        b.ce(ce(205), dia, [('CCCU3333333', '', '22000.000', '28.000', [], 'false')], tipo_bl='15',
             master=ce(901))                                                                          # H3
        b.ce(ce(206), dia, [('CCCU3333333', '', '22000.000', '28.000', ['61091000'], 'false')], tipo_bl='12',
             master=ce(205))                                                                          # F3
        b.ce(ce(207), dia, maquina('LCLU0000001', uso='true'))                                        # uso parcial
        b.ce(ce(208), dia, maquina('EXPU0000001', cubagem='99.000'), trafego='7')                     # exportação
        b.ce(ce(209), dia, maquina('PEZU0000001', peso='35000.000'))                                  # PEZO
        b.ce(ce(210), dia, maquina('SIMU0000001', cubagem='1.000'))                                   # SIMB
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
            cls.diag = dd.DiagnosticoDeclaracao(conexao, cls.saida, DESDE, ATE, bloco=20, progresso=lambda _: None,
                                                deslocamentos=(1, 2))
            pasta = tempfile.mkdtemp()
            cls.csv, cls.revisao = os.path.join(pasta, 'declaracao.csv'), os.path.join(pasta, 'revisao.csv')
            cls.ok = dd.roda(cls.diag, cls.csv, cls.revisao)
        cls.log = cls.saida.getvalue()

    def marcados(self, sinal):
        return set(self.diag.marcas[sinal])

    def test_populacao_pela_janela_de_id_sem_exportacao_e_com_as_familias(self):
        self.assertTrue(self.ok)
        self.assertEqual(self.diag.janela[0], 4)                          # os 3 de agosto ficam antes
        self.assertEqual(len(self.diag.populacao), 75 + 10)               # sem a exportação
        self.assertNotIn(ce(208), self.diag.ces)
        self.assertIn(ce(900), self.diag.ces)                             # master fora da janela, lido pela família
        por_conteiner = {c.conteiner: c for c in self.diag.conteineres}
        f1 = por_conteiner['MMMU0000001']
        self.assertEqual((f1.familia, f1.ces, f1.peso, f1.origem_peso),
                         (ce(900), (ce(204), ce(900)), 20000.0, 'item do master'))
        cadeia = por_conteiner['CCCU3333333']
        self.assertEqual((cadeia.familia, cadeia.ces), (ce(901), (ce(205), ce(206), ce(901))))
        self.assertTrue(por_conteiner['LCLU0000001'].uso_parcial)

    def test_sinais(self):
        self.assertEqual(self.marcados(rd.OCUPACAO), {ce(200), ce(902)})   # o P0 veio pelas referências
        self.assertEqual(self.marcados(rd.CARGA_MAXIMA), {ce(209)})
        self.assertNotIn(ce(209), self.marcados('18a+') | self.marcados('18b+'))   # peso impossível: fora do 18
        self.assertEqual(self.marcados(rd.CUBAGEM_REPETIDA), {ce(202)})
        self.assertEqual(self.diag.avaliaveis[rd.CUBAGEM_REPETIDA], {ce(202)})
        self.assertEqual(self.marcados(rd.PREFIXO), {ce(203)})
        self.assertIn(ce(201), self.marcados('18a+'))
        self.assertIn(ce(201), self.marcados('18b+'))
        self.assertNotIn(ce(207), set().union(*(self.marcados(s) for s in rd.DESCRICOES)))   # uso parcial: fora

    def test_referencias(self):
        refs = self.diag.referencias
        self.assertEqual(refs['fichas positivas'], {ce(201), ce(902)})
        self.assertEqual(refs['RVF: todas'], {ce(201), ce(0)})
        self.assertEqual(refs['RVF com a infração "Falsa declaração"'], {ce(201)})
        self.assertEqual(refs['RVF com peso fora de 0,8–1,2 do declarado'], {ce(201)})   # 105 t × 25 t
        self.assertEqual(refs['pesagem: divergencia'], {ce(201)})
        self.assertEqual(self.diag.razoes_rvf['volume']['sem medida na RVF'], 1)

    def test_controle_da_mesma_epoca(self):
        controle = self.diag._controle(self.diag.referencias['fichas positivas'])
        self.assertEqual(controle, {ce(1), ce(202), ce(203)})             # o NORU 0 é referência (RVF): fora
        self.assertEqual(self.diag._taxa('18b+', controle), (3, 0))
        self.assertEqual(self.diag._taxa('18b+', self.diag.referencias['fichas positivas']), (2, 1))

    def test_combinacoes_do_18(self):
        derivados = {codigo: ces for codigo, _, ces in self.diag._derivados()}
        self.assertEqual(derivados['18b+ com fração'], {ce(201)})                  # 25,5 m³
        self.assertEqual(derivados['18b+ redonda'], {ce(205), ce(206), ce(901), ce(210)})   # 28 m³ e o SIMB
        self.assertTrue({ce(201), ce(204)} <= derivados['18+'])                    # F1: só o peso (20 t × 12 t)
        self.assertEqual(derivados['18a+ e 18b+'], {ce(201), ce(205), ce(206), ce(901)})
        self.assertTrue({ce(0), ce(205), ce(206)} <= derivados['18 cubagem redonda'])   # 25 e 28 m³
        self.assertNotIn(ce(201), derivados['18 cubagem redonda'])                   # 25,5 m³
        self.assertEqual(derivados['18b+ redonda simbólica'], {ce(210)})             # 1 m³ num 20'
        self.assertEqual(derivados['18b+ redonda plausível'], {ce(205), ce(206), ce(901)})
        self.assertEqual(derivados['18 cubagem simbólica'], {ce(210)})

    def test_combinacoes_tem_os_avaliaveis_do_18(self):
        avaliaveis = len(self.diag.avaliaveis['18'] & self.diag.populacao)
        for codigo, _, marcados in self.diag._derivados():
            with self.subTest(codigo=codigo):
                self.assertEqual(self.diag._taxa(codigo, self.diag.populacao, marcados)[0], avaliaveis)
                bloco = self.log.split(f'\n{codigo} — ')[1]
                linha = next(linha for linha in bloco.splitlines() if linha.startswith('população (janela)'))
                self.assertEqual(int(linha.split('|')[1]), avaliaveis)

    def test_revisao_so_densidade_acima_com_cubagem_redonda(self):
        self.assertEqual([numero for numero, _ in self.diag.candidatos_revisao()], [ce(206)])   # PESA: fração; SIMB
        with open(self.revisao, encoding='utf-8-sig') as arquivo:              # para o Excel
            linhas = list(csv.DictReader(arquivo, delimiter=';'))
        self.assertEqual((linhas[0]['ce'], linhas[0]['ncm'], linhas[0]['cubagem_m3'], linhas[0]['avaliacao']),
                         (ce(206), '61091000', '28,0', ''))
        self.assertIn("786 kg/m³; típico do NCM 61091000 em contêiner de 20'", linhas[0]['motivo'])

    def test_concentracao_do_candidato(self):
        self.assertEqual(self.diag.ces[ce(205)].importador, '11222333')    # raiz do CNPJ (a cadeia é da EMP1)
        self.assertEqual(dd.raiz_importador('123.456.789-09'), '12345678909')
        trecho = self.log.split('Concentração do candidato')[1].split('V. VALIDAÇÃO')[0]
        self.assertIn('importador (raiz do CNPJ): 2 CEs marcados, de 1 diferentes', trecho)   # 205 e 206
        self.assertIn('11222333', trecho)
        self.assertIn("20'", trecho)

    def test_refaz_a_conexao_que_caiu(self):
        engine = create_engine('sqlite://')
        caida = mock.Mock(invalidated=True, dialect=engine.dialect)
        caida.execute.side_effect = Exception('Lost connection to MySQL server during query')
        diag = dd.DiagnosticoDeclaracao(caida, io.StringIO(), DESDE, ATE, engine=engine)
        self.assertEqual(diag.consulta('SELECT 1'), [(1,)])
        self.assertIn('conexão perdida: refeita', diag.saida.getvalue())

    def test_relatorio(self):
        for trecho in ('(SHOW INDEX só no MariaDB/MySQL)', '1. POPULAÇÃO, FAMÍLIAS E TEMPO',
                       'estimativa da população em 90 dias', '16. CONTÊINER × CUBAGEM', 'Cubagens mais frequentes',
                       '16e cubagem repetida (m³)', '17. NÚMERO DO BL', 'dominante', '18. PESO × VOLUME',
                       'V. VALIDAÇÃO', 'Evidência FRACA', 'vezes o controle', 'Falsa declaração', 'A. AMOSTRAS',
                       '3 contêineres com NCM 39011000 e a mesma cubagem (68,0 m³)', 'C. CONSULTAS MAIS LENTAS',
                       '18+ — peso OU densidade acima', '18b+ com fração — densidade acima', 'os dois',
                       '18 cubagem redonda — cubagem em m³ inteiro, sem olhar a densidade',
                       '18b+ redonda plausível — densidade acima', '... simbólica (abaixo de 10%)',
                       'R. REVISÃO'):
            with self.subTest(trecho=trecho):
                self.assertIn(trecho, self.log)
        self.assertNotIn('!! ERRO', self.log)
        self.assertNotIn('16a — ', self.log.split('V. VALIDAÇÃO')[1].split('A. AMOSTRAS')[0])  # qualidade: sem V

    def test_csv(self):
        with open(self.csv, encoding='utf-8') as arquivo:
            linhas = list(csv.DictReader(arquivo, delimiter=';'))
        pesa = [linha for linha in linhas if linha['ce'] == ce(201) and linha['sinal'] == '18a+']
        self.assertEqual(len(pesa), 1)
        self.assertEqual((pesa[0]['na_janela'], pesa[0]['controle_de']), ('sim', ''))
        self.assertIn('fichas positivas', pesa[0]['referencias'])
        repe = next(linha for linha in linhas if linha['sinal'] == rd.CUBAGEM_REPETIDA)
        self.assertEqual(repe['controle_de'], ce(201))                    # REPE é vizinho (controle) do PESA

    def test_para_sem_indice(self):
        diag = dd.DiagnosticoDeclaracao(mock.Mock(), io.StringIO(), DESDE, ATE)
        diag.mysql = True
        diag.consulta = mock.Mock(return_value=[('itensresumo', 1, 'PRIMARY', 1, 'ID')])
        self.assertFalse(dd.roda(diag))
        self.assertIn('o diagnóstico para aqui', diag.saida.getvalue())


class TestFormatos(unittest.TestCase):

    def test_empresa_tipo_e_base_do_sinal(self):
        self.assertEqual([dd.formato_empresa(v) for v in ('', '12345678000199', '123', 'MSC', 'MSC1')],
                         ['(vazio)', 'CNPJ (14 dígitos)', '3 dígitos', 'só letras', 'letras e dígitos'])
        self.assertEqual([dd.situacao_tipo(v) for v in ('', 'XPTO', '22G1', '22T0')],
                         ['(vazio)', 'não reconhecido', 'reconhecido, com capacidade de referência',
                          'reconhecido, sem capacidade de referência'])
        self.assertEqual([dd.base_do_sinal(s) for s in ('16a', '16e', '17a', '18b+')], ['16', '16e', '17', '18'])


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
