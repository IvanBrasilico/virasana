"""Testes do diagnóstico do risco histórico (analytic_scripts/diagnostico_risco_perdimentos.py)."""
import io
import unittest
from datetime import datetime

from sqlalchemy import create_engine, text

from virasana.analytic_scripts import diagnostico_risco_perdimentos as drp
from virasana.analytic_scripts.dados_transporte_apreensoes import digito_conteiner

CNPJ = '11222333000181'           # válido
CNPJ_OUTRA_FILIAL = '11222333000262'  # mesma raiz, válido
CPF = '52998224725'               # válido
CE1, CE2, CE4 = '152600000000001', '152600000000002', '152600000000004'
CONTEINER = 'MSCU123456' + str(digito_conteiner('MSCU123456'))  # dígito ISO 6346 válido

DDL = [
    'CREATE TABLE ovr_ovrs (id INTEGER PRIMARY KEY, numero VARCHAR(10), datahora TIMESTAMP, '
    'numeroCEmercante VARCHAR(15), observacoes VARCHAR(1000), cnpj_fiscalizado VARCHAR(15), tipooperacao INTEGER)',
    'CREATE TABLE ovr_resultados (id INTEGER PRIMARY KEY, ovr_id INTEGER, tipo_resultado INTEGER, valor NUMERIC)',
    'CREATE TABLE ovr_verificacoesfisicas (id INTEGER PRIMARY KEY, ovr_id INTEGER, numeroCEmercante VARCHAR(15), '
    'numerolote VARCHAR(20), descricao VARCHAR(5000), descricao_interna VARCHAR(5000))',
    'CREATE TABLE ovr_tiposapreensao (id INTEGER PRIMARY KEY, descricao VARCHAR(50))',
    'CREATE TABLE ovr_apreensoes_rvf (id INTEGER PRIMARY KEY, rvf_id INTEGER, tipo_id INTEGER)',
    'CREATE TABLE ovr_infracoes (id INTEGER PRIMARY KEY, nome VARCHAR(50))',
    'CREATE TABLE ovr_infracoesencontradas (rvf_id INTEGER, infracao_id INTEGER)',
    'CREATE TABLE ovr_marcas (id INTEGER PRIMARY KEY, nome VARCHAR(50))',
    'CREATE TABLE ovr_marcasencontradas (rvf_id INTEGER, marca_id INTEGER)',
    'CREATE TABLE ovr_tiposmercadoria (id INTEGER PRIMARY KEY, nome VARCHAR(50))',
    'CREATE TABLE ovr_tgovr (id INTEGER PRIMARY KEY, ovr_id INTEGER, descricao VARCHAR(500), '
    'observacoes VARCHAR(500), tipomercadoria_id INTEGER)',
    'CREATE TABLE ovr_itenstg (id INTEGER PRIMARY KEY, tg_id INTEGER, ncm VARCHAR(8), marca_id INTEGER)',
    'CREATE TABLE ovr_tiposevento (id INTEGER PRIMARY KEY, nome VARCHAR(50))',
    'CREATE TABLE ovr_eventos (id INTEGER PRIMARY KEY, ovr_id INTEGER, tipoevento_id INTEGER, motivo VARCHAR(200))',
    'CREATE TABLE ovr_processos (id INTEGER PRIMARY KEY, ovr_id INTEGER)',
    'CREATE TABLE ovr_flags (id INTEGER PRIMARY KEY, nome VARCHAR(100))',
    'CREATE TABLE ovr_flags_ovr (rvf_id INTEGER, flag_id INTEGER)',
    'CREATE TABLE conhecimentosresumo (numeroCEmercante VARCHAR(15), manifestoCE VARCHAR(15), embarcador VARCHAR(200), '
    'consignatario VARCHAR(200), cnpjNotify VARCHAR(45), portoOrigemCarga VARCHAR(5), portoDestFinal VARCHAR(5), '
    'tipoTrafego VARCHAR(10))',
    'CREATE TABLE manifestosresumo (numero VARCHAR(15), portoCarregamento VARCHAR(5))',
    'CREATE TABLE itensresumo (numeroCEmercante VARCHAR(15), codigoConteiner VARCHAR(11))',
    'CREATE TABLE ncmitemresumo (numeroCEMercante VARCHAR(15), identificacaoNCM VARCHAR(10))',
]


def insere(con, tabela, linhas):
    colunas = list(dict.fromkeys(c for linha in linhas for c in linha))
    con.execute(text(f'INSERT INTO {tabela} ({", ".join(colunas)}) VALUES ({", ".join(":" + c for c in colunas)})'),
                [{c: linha.get(c) for c in colunas} for linha in linhas])


def monta_banco(engine):
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        insere(con, 'ovr_ovrs', [
            {'id': 1, 'datahora': datetime(2025, 3, 1), 'numeroCEmercante': CE1, 'cnpj_fiscalizado': CNPJ,
             'observacoes': f'Motorista CPF 529.982.247-25; empresa {CNPJ_OUTRA_FILIAL}; contêiner {CONTEINER}'},
            {'id': 2, 'datahora': datetime(2026, 1, 5), 'numeroCEmercante': CE2, 'cnpj_fiscalizado': CNPJ,
             'observacoes': 'sem citações; número 12345678901 não é CPF válido'},
            {'id': 3, 'datahora': datetime(2026, 2, 1), 'numeroCEmercante': CE1},   # só crédito: fora do padrão
            {'id': 4, 'datahora': datetime(2026, 3, 1), 'numeroCEmercante': None},  # sem CE
        ])
        insere(con, 'ovr_resultados', [
            {'id': 1, 'ovr_id': 1, 'tipo_resultado': 2, 'valor': 1000},
            {'id': 2, 'ovr_id': 1, 'tipo_resultado': 1, 'valor': 0},
            {'id': 3, 'ovr_id': 2, 'tipo_resultado': 1, 'valor': 50},
            {'id': 4, 'ovr_id': 3, 'tipo_resultado': 3, 'valor': 7},
            {'id': 5, 'ovr_id': 4, 'tipo_resultado': 2, 'valor': 300},
        ])
        insere(con, 'ovr_verificacoesfisicas', [
            {'id': 10, 'ovr_id': 1, 'numeroCEmercante': CE1, 'numerolote': CONTEINER,
             'descricao': 'NCM 8471.30.12 declarado, encontrado outro'},
            {'id': 40, 'ovr_id': 4, 'numeroCEmercante': CE4, 'numerolote': ''},
        ])
        insere(con, 'ovr_tiposapreensao', [{'id': 1, 'descricao': 'Cocaína'}])
        insere(con, 'ovr_apreensoes_rvf', [{'id': 1, 'rvf_id': 10, 'tipo_id': 1}])
        insere(con, 'ovr_infracoes', [{'id': 1, 'nome': 'Falsa declaração'}])
        insere(con, 'ovr_infracoesencontradas', [{'rvf_id': 10, 'infracao_id': 1}])
        insere(con, 'ovr_marcas', [{'id': 1, 'nome': 'MARCA X'}])
        insere(con, 'ovr_marcasencontradas', [{'rvf_id': 10, 'marca_id': 1}])
        insere(con, 'ovr_tiposmercadoria', [{'id': 1, 'nome': 'Eletrônicos'}])
        insere(con, 'ovr_tgovr', [{'id': 1, 'ovr_id': 1, 'descricao': 'notebooks', 'observacoes': None,
                                   'tipomercadoria_id': 1}])
        insere(con, 'ovr_itenstg', [{'id': 1, 'tg_id': 1, 'ncm': '84713012', 'marca_id': 1}])
        insere(con, 'ovr_tiposevento', [{'id': 1, 'nome': 'Lavratura de auto'}])
        insere(con, 'ovr_eventos', [{'id': 1, 'ovr_id': 1, 'tipoevento_id': 1, 'motivo': 'auto lavrado'},
                                    {'id': 2, 'ovr_id': 2, 'tipoevento_id': 1, 'motivo': ''}])
        insere(con, 'ovr_processos', [{'id': 1, 'ovr_id': 1}])
        insere(con, 'ovr_flags', [{'id': 1, 'nome': 'Subfaturamento'}])
        insere(con, 'ovr_flags_ovr', [{'rvf_id': 2, 'flag_id': 1}])
        insere(con, 'conhecimentosresumo', [
            {'numeroCEmercante': CE1, 'manifestoCE': 'M1', 'embarcador': 'ACME CO', 'consignatario': CNPJ,
             'cnpjNotify': '', 'portoOrigemCarga': 'CNSHA', 'portoDestFinal': 'BRSSZ', 'tipoTrafego': '5'},
            {'numeroCEmercante': CE2, 'manifestoCE': 'M2', 'embarcador': 'acme co ', 'consignatario':
                CNPJ_OUTRA_FILIAL, 'cnpjNotify': CNPJ, 'portoOrigemCarga': 'CNNGB', 'portoDestFinal': 'BRSSZ',
             'tipoTrafego': '5'},
        ])
        insere(con, 'manifestosresumo', [{'numero': 'M1', 'portoCarregamento': 'CNSHA'},
                                         {'numero': 'M2', 'portoCarregamento': 'SGSIN'}])
        insere(con, 'itensresumo', [{'numeroCEmercante': CE1, 'codigoConteiner': CONTEINER}])
        insere(con, 'ncmitemresumo', [{'numeroCEMercante': CE1, 'identificacaoNCM': '84713012'},
                                      {'numeroCEMercante': CE2, 'identificacaoNCM': '84714100'}])


class TestIdentificadores(unittest.TestCase):

    def test_cnpj(self):
        self.assertTrue(drp.cnpj_valido(CNPJ))
        self.assertFalse(drp.cnpj_valido('11222333000182'))
        self.assertFalse(drp.cnpj_valido('11111111111111'))
        self.assertEqual(drp.extrai_cnpjs('CNPJ 11.222.333/0001-81 e 11222333000182'), [CNPJ])

    def test_ncm_citado(self):
        self.assertEqual(drp.extrai_ncms('NCM: 8471.30.12 e ncm 9503'), ['84713012', '9503'])

    def test_mascara(self):
        self.assertEqual(drp.mascara(CPF), '529.***.***-25')
        self.assertEqual(drp.mascara(CNPJ), '11.222.***/****-81')


class TestDiagnostico(unittest.TestCase):

    def roda(self, **kwargs):
        engine = create_engine('sqlite://')
        monta_banco(engine)
        saida = io.StringIO()
        with engine.connect() as conexao:
            diag = drp.Diagnostico(conexao, saida, **kwargs)
            for secao in (diag.secao_estrutura, diag.secao_resultados, diag.secao_cobertura, diag.secao_mercante,
                          diag.secao_textos, diag.secao_distribuicoes):
                secao()
        return saida.getvalue()

    def test_secoes_sem_erro(self):
        log = self.roda()
        self.assertNotIn('!! ERRO', log)
        for secao in range(1, 7):
            self.assertIn(f'\n{secao}. ', log)

    def test_universo_padrao_apreensao_e_perdimento(self):
        log = self.roda()
        self.assertIn('Fichas no universo: 3', log)       # 1, 2 e 4 (a 3 só tem crédito)
        self.assertIn('2 Perdimento', log)
        self.assertIn('Fichas com CE: 3; CEs distintos: 3', log)  # CE4 só na RVF

    def test_universo_por_tipo_e_periodo(self):
        self.assertIn('Fichas no universo: 2', self.roda(tipos=(2,)))
        self.assertIn('Fichas no universo: 2', self.roda(inicio=datetime(2026, 1, 1)))

    def test_textos_mascarados(self):
        log = self.roda()
        self.assertNotIn(CPF, log)
        self.assertNotIn(CNPJ_OUTRA_FILIAL, log)
        self.assertIn('529.***.***-25', log)
        self.assertIn('11.222.***/****-62', log)
        self.assertIn(CONTEINER, log)
        self.assertIn('SEM dígito verificador de CPF válido (descartadas): 1', log)

    def test_concentracao(self):
        log = self.roda()
        linha = next(lin for lin in log.splitlines() if lin.startswith('importador (raiz CNPJ'))
        self.assertEqual([c.strip() for c in linha.split('|')][1:], ['1', '1', '2'])  # mesma raiz em 2 fichas
        linha = next(lin for lin in log.splitlines() if lin.startswith('embarcador (nome)'))
        self.assertEqual([c.strip() for c in linha.split('|')][1:], ['1', '1', '2'])  # nome normalizado
        self.assertIn('o consignatário', log)


if __name__ == '__main__':
    unittest.main()
