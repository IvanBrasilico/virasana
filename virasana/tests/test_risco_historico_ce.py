"""Testes do risco histórico por CE: fichas positivas, sinais do CE, cálculo e
diagnóstico F0 (virasana/risco_historico/, analytic_scripts/diagnostico_risco_ce.py).

Cenário (agora = 14/10/2026):
  fichas 1 e 2 — importador A, embarcador NINGBO TOYS, rota CNNGB > BRSSZ (Perdimento; Crédito);
                 a 1 cita o CNPJ C e o próprio CNPJ de A nas observações
  ficha 3 — FMA com perdimento (fora); 4 — só apreensão (fora); 6 — TG de 50 mil (fora);
  ficha 7 — exportação (tipo 2, fora); 5 — sem resultado, TGs somando R$ 1,5 mi (entra pelo TG, peso 2);
  ficha 8 — Multa RFB + TG de 200 mil, mas com CE só de exportação (descartada na extração);
  ficha 9 — Multa de outro órgão, sem CE.
  População (30 dias, sem exportação): P1 (A, NINGBO, com baldeação), P2 (C), P3..P10 (rota comum).
"""
import io
import math
import unittest
from argparse import Namespace
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from virasana.analytic_scripts import dados_risco_perdimentos as drp
from virasana.analytic_scripts import diagnostico_risco_ce as diag
from virasana.risco_historico import calculo, fichas_positivas, sinais
from virasana.tests.test_diagnostico_risco_perdimentos import DDL as DDL_FICHAS, insere

AGORA = datetime(2026, 10, 14, 12, 0)
CNPJ_A, CNPJ_C = '11222333000181', '11444777000161'   # válidos
CE1, CE2, CE3, CE4 = '152400000000001', '152500000000002', '152600000000003', '152600000000004'
REDEFINIDAS = ('ovr_tgovr', 'conhecimentosresumo', 'ncmitemresumo')
DDL = [ddl for ddl in DDL_FICHAS if not any(f'CREATE TABLE {t} (' in ddl for t in REDEFINIDAS)] + [
    'CREATE TABLE ovr_tgovr (id INTEGER PRIMARY KEY, ovr_id INTEGER, descricao VARCHAR(500), '
    'observacoes VARCHAR(500), tipomercadoria_id INTEGER, valor NUMERIC)',
    'CREATE TABLE conhecimentosresumo (ID INTEGER PRIMARY KEY, numeroCEmercante VARCHAR(15), '
    'manifestoCE VARCHAR(15), embarcador VARCHAR(200), consignatario VARCHAR(200), cnpjNotify VARCHAR(45), '
    'portoOrigemCarga VARCHAR(5), portoDestFinal VARCHAR(5), tipoTrafego VARCHAR(10), dataEmissao VARCHAR(20), '
    'create_date TIMESTAMP, last_modified DATETIME)',
    'CREATE TABLE ncmitemresumo (ID INTEGER PRIMARY KEY, numeroCEMercante CHAR(15), identificacaoNCM VARCHAR(10), '
    'create_date TIMESTAMP)',
    'CREATE TABLE laudo_empresas (id INTEGER PRIMARY KEY, cnpj VARCHAR(15), nome VARCHAR(255))',
]


def _ce(numero, consignatario, embarcador, origem, criado, manifesto=None, trafego='5', emitido=None,
        modificado=None):
    return {'numeroCEmercante': numero, 'consignatario': consignatario, 'embarcador': embarcador,
            'portoOrigemCarga': origem, 'portoDestFinal': 'BRSSZ', 'manifestoCE': manifesto, 'tipoTrafego': trafego,
            'create_date': criado, 'dataEmissao': (emitido or criado).strftime('%Y-%m-%d'),
            'last_modified': modificado}


def _fichas(con):
    insere(con, 'ovr_ovrs', [
        {'id': 1, 'datahora': datetime(2024, 3, 1), 'tipooperacao': 1, 'numeroCEmercante': CE1,
         'cnpj_fiscalizado': CNPJ_A, 'observacoes': f'Interposta {CNPJ_C}; fiscalizada {CNPJ_A}'},
        {'id': 2, 'datahora': datetime(2025, 5, 1), 'tipooperacao': 3, 'numeroCEmercante': CE2},
        {'id': 3, 'datahora': datetime(2025, 6, 1), 'tipooperacao': 0, 'numeroCEmercante': CE1},
        {'id': 4, 'datahora': datetime(2025, 7, 1), 'tipooperacao': 1, 'numeroCEmercante': CE1},
        {'id': 5, 'datahora': datetime(2026, 1, 10), 'tipooperacao': 1, 'numeroCEmercante': CE3},
        {'id': 6, 'datahora': datetime(2026, 2, 1), 'tipooperacao': 1, 'numeroCEmercante': CE3},
        {'id': 7, 'datahora': datetime(2026, 3, 1), 'tipooperacao': 2, 'numeroCEmercante': CE4},
        {'id': 8, 'datahora': datetime(2026, 4, 1), 'tipooperacao': None, 'numeroCEmercante': CE4},
        {'id': 9, 'datahora': datetime(2026, 5, 1), 'tipooperacao': 1},
    ])
    insere(con, 'ovr_resultados', [
        {'id': 1, 'ovr_id': 1, 'tipo_resultado': 2, 'valor': 1000}, {'id': 2, 'ovr_id': 2, 'tipo_resultado': 3},
        {'id': 3, 'ovr_id': 3, 'tipo_resultado': 2}, {'id': 4, 'ovr_id': 4, 'tipo_resultado': 1},
        {'id': 5, 'ovr_id': 7, 'tipo_resultado': 2}, {'id': 6, 'ovr_id': 8, 'tipo_resultado': 6},
        {'id': 7, 'ovr_id': 9, 'tipo_resultado': 7},
    ])
    insere(con, 'ovr_tgovr', [
        {'id': 1, 'ovr_id': 5, 'valor': 1000000}, {'id': 2, 'ovr_id': 5, 'valor': 500000},
        {'id': 3, 'ovr_id': 6, 'valor': 50000}, {'id': 4, 'ovr_id': 8, 'valor': 200000},
        {'id': 5, 'ovr_id': 3, 'valor': 300000},
    ])


def _mercante(con):
    recente = AGORA - timedelta(days=5)
    ces = [
        {**_ce(CE1, CNPJ_A, 'Ningbo Toys Co., Ltd.', 'CNNGB', datetime(2024, 2, 1), 'M1'), 'ID': 1},
        {**_ce(CE2, CNPJ_A, 'NINGBO TOYS CO LTD', 'CNNGB', datetime(2025, 4, 1)), 'ID': 2},
        {**_ce(CE3, '44555666000100', 'ACME', 'CNSHA', datetime(2025, 12, 1)), 'ID': 3},
        {**_ce(CE4, 'EXTERIOR', 'X', 'BRSSZ', datetime(2026, 3, 1), trafego='7'), 'ID': 4},
        {**_ce('P0', '77777777000177', 'VELHO', 'CNNGB', AGORA - timedelta(days=40)), 'ID': 5},
        {**_ce('P1', CNPJ_A, 'Ningbo Toys Co Ltd', 'CNNGB', recente, 'M2', emitido=recente - timedelta(days=2),
               modificado=recente + timedelta(days=3)), 'ID': 6},
        {**_ce('P2', CNPJ_C, 'OTHER SHIPPER', 'CNSHA', recente), 'ID': 7},
    ]
    ces += [{**_ce(f'P{i}', f'{30000000 + i:08d}000199', f'SHIPPER {i}', 'CNNGB', recente), 'ID': 5 + i}
            for i in range(3, 11)]
    ces += [{**_ce('PX', '88888888000188', 'EXPORT', 'BRSSZ', recente, trafego='7'), 'ID': 16},
            {**_ce('PZ', '99999999000199', 'ATRASADO', 'CNNGB', AGORA - timedelta(days=60)), 'ID': 17}]
    insere(con, 'conhecimentosresumo', ces)
    insere(con, 'manifestosresumo', [{'numero': 'M1', 'portoCarregamento': 'CNSHA'},
                                     {'numero': 'M2', 'portoCarregamento': 'CNSHA'}])
    insere(con, 'ncmitemresumo', [
        {'ID': 1, 'numeroCEMercante': CE1, 'identificacaoNCM': '95030010', 'create_date': datetime(2024, 2, 1)},
        {'ID': 2, 'numeroCEMercante': CE2, 'identificacaoNCM': '84713012', 'create_date': datetime(2025, 4, 1)},
        {'ID': 3, 'numeroCEMercante': 'P1', 'identificacaoNCM': '95030099', 'create_date': recente},
        {'ID': 4, 'numeroCEMercante': 'P2', 'identificacaoNCM': '84713012',
         'create_date': recente + timedelta(hours=2)},
    ])


def monta_banco(engine=None):
    engine = engine or create_engine('sqlite://')
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        _fichas(con)
        _mercante(con)
    return engine


class TestFichasPositivas(unittest.TestCase):

    def setUp(self):
        self.conexao = monta_banco().connect()

    def tearDown(self):
        self.conexao.close()

    def test_definicao_do_usuario(self):
        fichas = {f['id']: f for f in fichas_positivas.busca(self.conexao)}
        self.assertEqual(set(fichas), {1, 2, 5, 8, 9})   # sem FMA, apreensão, TG < 100 mil e exportação
        self.assertEqual({i: f['motivo'] for i, f in fichas.items()},
                         {1: 'resultado', 2: 'resultado', 5: 'tg', 8: 'ambos', 9: 'resultado'})
        self.assertEqual(fichas[5]['valor_tg'], 1500000.0)   # soma dos TGs da ficha
        # peso 2 só a partir de R$ 1 mi (a ficha 8 tem TG de 200 mil: entra, mas pesa 1)
        self.assertEqual({i: f['peso'] for i, f in fichas.items()}, {1: 1.0, 2: 1.0, 5: 2.0, 8: 1.0, 9: 1.0})

    def test_periodo_e_limiar(self):
        self.assertEqual({f['id'] for f in fichas_positivas.busca(self.conexao, inicio=datetime(2026, 1, 1))},
                         {5, 8, 9})
        self.assertEqual({f['id'] for f in fichas_positivas.busca(self.conexao, limiar=40000)}, {1, 2, 5, 6, 8, 9})


class TestSinais(unittest.TestCase):

    def test_sinais_do_ce(self):
        s = sinais.sinais_do_ce({'consignatario': CNPJ_A, 'embarcador': 'Ningbo Toys Co., Ltd.',
                                 'portoOrigemCarga': 'cnngb', 'portoDestFinal': 'BRSSZ'}, 'CNSHA', ['9503'])
        self.assertEqual(s, {'importador': {'11222333'}, 'embarcador': {'NINGBO TOYS'}, 'rota': {'CNNGB > BRSSZ'},
                             'porto_baldeacao': {'CNSHA'}, 'pais_ncm': {'CN 9503'}, 'ncm_declarado': {'9503'}})
        self.assertEqual(sinais.pontuaveis(s)[sinais.CITADO], {'11222333'})

    def test_cpf_e_carregamento_na_origem_nao_sao_sinal(self):
        s = sinais.sinais_do_ce({'consignatario': '52998224725', 'portoOrigemCarga': 'CNSHA',
                                 'portoDestFinal': 'BRSSZ'}, 'CNSHA', [])
        self.assertEqual(s['importador'], set())
        self.assertEqual(s['porto_baldeacao'], set())

    def test_mesmos_valores_do_historico(self):
        """O CE da ficha gera, nas dimensões do cálculo, os mesmos valores que a extração do histórico."""
        conexao = monta_banco().connect()
        [ficha] = drp.extrai(conexao, fichas_positivas.RESULTADOS_POSITIVOS,
                             fichas=[f for f in fichas_positivas.busca(conexao) if f['id'] == 1])['fichas']
        ce = dict(conexao.execute(text('SELECT * FROM conhecimentosresumo WHERE ID = 1')).one()._mapping)
        do_ce = sinais.sinais_do_ce(ce, 'CNSHA', ['9503'])
        for dimensao in sinais.DIMENSOES_CE:
            self.assertEqual(do_ce[dimensao], ficha['sinais'][dimensao], dimensao)
        conexao.close()

    def test_populacao_do_periodo(self):
        conexao = monta_banco().connect()
        por_ce, tempos = sinais.sinais_do_periodo(conexao, AGORA - timedelta(days=30), AGORA)
        self.assertEqual(set(por_ce), {f'P{i}' for i in range(1, 11)})   # sem exportação, sem os antigos
        self.assertEqual(por_ce['P1']['porto_baldeacao'], {'CNSHA'})
        self.assertEqual(por_ce['P2']['ncm_declarado'], {'8471'})          # NCM gravada 2 h depois do CE
        self.assertEqual(set(tempos), {'CEs do período', 'portos de carregamento (manifestos)', 'NCMs',
                                       'sinais (Python)'})
        contagens = sinais.conta(por_ce.values())
        self.assertEqual(contagens['rota']['CNNGB > BRSSZ'], 9)
        self.assertEqual(contagens[sinais.CITADO]['11444777'], 1)
        conexao.close()


class TestCalculo(unittest.TestCase):

    def test_cnpj_citado_sem_o_proprio_importador_e_sem_cpf(self):
        valores = calculo.valores_da_ficha({'importador': {'11222333', 'CPF 52998224725'},
                                            'cnpj_citado': {CNPJ_A, CNPJ_C}})
        self.assertEqual(valores['importador'], {'11222333'})
        self.assertEqual(valores[sinais.CITADO], {'11444777'})

    def test_peso(self):
        # k=2 de K=5; valor em 1 de 10 CEs: fatia suavizada (2 + 10*0,1)/(5 + 10) = 0,2 -> 2x a média
        p = calculo.peso('importador', 2, 2.0, 5.0, 1, 10)
        self.assertAlmostEqual(p.vezes, 2.0)
        self.assertAlmostEqual(p.peso, math.log(2), places=3)   # 3 casas, como gravado
        self.assertEqual(p.motivo, '')
        self.assertGreater(calculo.peso('importador', 1, 1.0, 5.0, 1, 100).peso, 0.0)    # identidade: 1 ficha basta
        rota_comum = calculo.peso('rota', 3, 3.0, 5.0, 9, 10, importadores=3)
        self.assertEqual((rota_comum.peso, rota_comum.motivo), (0.0, 'não passa da média'))
        self.assertEqual(calculo.peso('rota', 2, 2.0, 5.0, 1, 10 ** 6, importadores=2).motivo, 'menos de 3 fichas')
        uma_empresa = calculo.peso('ncm_declarado', 3, 3.0, 5.0, 1, 10 ** 6, importadores=1)
        self.assertEqual((uma_empresa.peso, uma_empresa.motivo), (0.0, 'fichas de menos de 2 importadores'))
        self.assertEqual(calculo.peso('rota', 3, 3.0, 5.0, 1, 10 ** 6, importadores=2).peso, 3.0)   # teto do contexto
        self.assertEqual(calculo.peso('importador', 5, 5.0, 5.0, 0, 10 ** 7).peso, 5.0)
        self.assertEqual(calculo.peso(sinais.CITADO, 5, 5.0, 5.0, 0, 10 ** 7).peso, 8.0)
        self.assertEqual(calculo.peso('importador', 2, 2.0, 0.0, 1, 10).peso, 0.0)

    def test_grupos_contam_uma_vez(self):
        p = calculo.Peso(3, 2, 3.0, 5.0, 1, 100, 10.0, 2.0)
        q = calculo.Peso(3, 2, 3.0, 5.0, 1, 100, 20.0, 2.5)
        pesos = {d: {} for d in sinais.DIMENSOES}
        pesos['ncm_declarado']['6108'], pesos['pais_ncm']['CN 6108'] = p, q
        pontos, parcelas = calculo.pontua({'ncm_declarado': {'6108'}, 'pais_ncm': {'CN 6108'}}, pesos)
        self.assertEqual((pontos, [(d, v) for d, v, _ in parcelas]), (2.5, [('pais_ncm', 'CN 6108')]))

    def test_importadores_distintos_e_embarcador_longo(self):
        longo = 'X' * 300
        hist = calculo.historico([
            {'id': 1, 'peso': 1.0, 'sinais': {'importador': {'11111111'}, 'rota': {'A > B'}, 'embarcador': {longo}}},
            {'id': 2, 'peso': 1.0, 'sinais': {'importador': {'11111111'}, 'rota': {'A > B'}}},
            {'id': 3, 'peso': 2.0, 'sinais': {'rota': {'A > B'}}}])                       # sem importador conhecido
        self.assertEqual(hist.total, 4.0)
        self.assertEqual(hist.importadores['rota']['A > B'], {'11111111', 'ficha 3'})
        self.assertIn('X' * 255, hist.k['embarcador'])

    def test_pontos_sao_a_soma_das_parcelas(self):
        conexao = monta_banco().connect()
        extracao = drp.extrai(conexao, fichas_positivas.RESULTADOS_POSITIVOS, fichas=fichas_positivas.busca(conexao))
        self.assertEqual(extracao['descartadas_exportacao'], 1)   # ficha 8
        hist = calculo.historico(extracao['fichas'])
        self.assertEqual(hist.total, 5.0)                          # 1 + 1 + 2 (TG) + 1
        self.assertEqual(sorted(hist.fichas['importador']['11222333']), [1, 2])
        self.assertEqual(hist.fichas[sinais.CITADO], {'11444777': [1]})
        por_ce, _ = sinais.sinais_do_periodo(conexao, AGORA - timedelta(days=30), AGORA)
        pesos = calculo.tabela_de_pesos(hist, sinais.conta(por_ce.values()), len(por_ce))
        pontos, parcelas = calculo.pontua(sinais.pontuaveis(por_ce['P1']), pesos)
        self.assertEqual({d for d, _, _ in parcelas}, {'importador', 'embarcador'})   # rota comum e NCM em 1 ficha: 0
        self.assertAlmostEqual(pontos, 2 * math.log(2), places=2)
        self.assertAlmostEqual(pontos, sum(p.peso for _, _, p in parcelas), places=4)
        pontos_p2, parcelas_p2 = calculo.pontua(sinais.pontuaveis(por_ce['P2']), pesos)
        self.assertEqual([d for d, _, _ in parcelas_p2], [sinais.CITADO])
        self.assertGreater(pontos_p2, 0)
        self.assertEqual(calculo.pontua(sinais.pontuaveis(por_ce['P3']), pesos), (0, []))
        conexao.close()


class TestDiagnostico(unittest.TestCase):

    def roda(self, **opcoes):
        args = Namespace(**{'dias_populacao': 30, 'dias_incremento': 30, 'limiar_tg': fichas_positivas.TG_ALTO_VALOR,
                            'sem_populacao': False, **opcoes})
        saida = io.StringIO()
        with monta_banco().connect() as conexao:
            diag.roda(conexao, saida, args, AGORA)
        return saida.getvalue()

    def test_secoes_e_numeros(self):
        log = self.roda()
        for secao in ('1. ESTRUTURA', '2. TIPO DE OPERAÇÃO', '3. VALOR DOS TGs', '4. FICHAS POSITIVAS',
                      '5. POPULAÇÃO', '6. INCREMENTO'):
            self.assertIn(secao, log)
        self.assertIn('Total: 5 fichas (só resultado 3, só TG alto 1, ambos 1); soma dos pesos 6', log)
        self.assertIn('Descartadas por terem só CEs de exportação: 1', log)
        self.assertIn('N (CEs na população): 10', log)
        self.assertIn('CNNGB > BRSSZ', log)
        self.assertIn('Prévia dos pontos: 2 de 10 CEs com algum sinal', log)
        self.assertIn('11 CEs em 1 dias', log)
        self.assertIn('Marca inicial (primeiro ID desde 2026-09-14): 6; IDs depois dela criados ANTES da data '
                      '(fora de ordem): 1', log)
        self.assertIn('Retificações (last_modified depois da criação): 1 de 11 CEs', log)

    def test_identidade_nunca_sai_no_log(self):
        log = self.roda()
        for sensivel in ('11222333', '11444777', 'NINGBO', '44555666', '30000003'):
            self.assertNotIn(sensivel, log)

    def test_sem_populacao(self):
        log = self.roda(sem_populacao=True)
        self.assertNotIn('5. POPULAÇÃO', log)
        self.assertIn('6. INCREMENTO', log)

    def test_faixas(self):
        self.assertEqual(diag.faixa(150000, diag.FAIXAS_TG), '100 a 500 mil')
        self.assertEqual(diag.faixa(0, diag.FAIXAS_PESO), '0 (não conta)')
        self.assertEqual(diag.faixa(0.5, diag.FAIXAS_PESO), 'até 1')
        self.assertEqual(diag.percentil([1.0, 2.0, 3.0], 0.5), 2.0)
        self.assertEqual(diag.percentil([], 0.5), 0.0)


if __name__ == '__main__':
    unittest.main()
