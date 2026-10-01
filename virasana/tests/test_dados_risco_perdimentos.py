"""Testes da extração do risco histórico das Fichas (analytic_scripts/dados_risco_perdimentos.py).

Cenário: o do teste do diagnóstico (fichas 1 e 2 do mesmo importador/embarcador,
3 só com crédito, 4 com CE só na RVF e fora do Mercante) + uma ficha de
exportação e o cadastro de empresas.
"""
import io
import unittest
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from virasana.analytic_scripts import dados_risco_perdimentos as drp
from virasana.tests.test_diagnostico_risco_perdimentos import (
    CNPJ, CNPJ_OUTRA_FILIAL, CONTEINER, CPF, insere, monta_banco as monta_banco_diagnostico,
)

CE5 = '152600000000005'


def monta_banco(engine):
    monta_banco_diagnostico(engine)
    with engine.begin() as con:
        con.execute(text('CREATE TABLE laudo_empresas (id INTEGER PRIMARY KEY, cnpj VARCHAR(15), nome VARCHAR(255))'))
        insere(con, 'laudo_empresas', [{'cnpj': CNPJ, 'nome': 'IMPORTADORA ALFA LTDA'}])
        insere(con, 'ovr_ovrs', [{'id': 5, 'datahora': datetime(2026, 4, 1), 'numeroCEmercante': CE5,
                                  'cnpj_fiscalizado': '99888777000166', 'observacoes': ''}])
        insere(con, 'ovr_resultados', [{'id': 6, 'ovr_id': 5, 'tipo_resultado': 2, 'valor': 10}])
        insere(con, 'conhecimentosresumo', [{'numeroCEmercante': CE5, 'manifestoCE': 'M1', 'embarcador': 'X',
                                             'consignatario': 'EXTERIOR', 'portoOrigemCarga': 'BRSSZ',
                                             'portoDestFinal': 'USNYC', 'tipoTrafego': '7'}])
        # motivo de evento com CPF (de auditor): não pode virar sinal
        insere(con, 'ovr_eventos', [{'id': 9, 'ovr_id': 2, 'tipoevento_id': 1,
                                     'motivo': 'Atribuído ao AFRFB CPF 111.444.777-35'}])


class TestNormalizacao(unittest.TestCase):

    def test_embarcador(self):
        self.assertEqual(drp.normaliza_embarcador('Ningbo Toys Co., Ltd.'), 'NINGBO TOYS')
        self.assertEqual(drp.normaliza_embarcador('ÁGUA  & CIA S.A.'), 'AGUA CIA')
        self.assertIsNone(drp.normaliza_embarcador('  co. ltd '))

    def test_documento(self):
        self.assertEqual(drp.documento('11.222.333/0001-81'), '11222333')
        self.assertEqual(drp.documento('11222333'), '11222333')
        self.assertEqual(drp.documento(CPF), 'CPF ' + CPF)
        self.assertIsNone(drp.documento('12345678901'))   # CPF inválido
        self.assertIsNone(drp.documento('SHENZHEN CO'))

    def test_posicao_ncm(self):
        self.assertEqual(drp.posicao_ncm('8471.30.12'), '8471')
        self.assertIsNone(drp.posicao_ncm('84'))


class TestExtracao(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite://')
        monta_banco(engine)
        self.conexao = engine.connect()

    def tearDown(self):
        self.conexao.close()

    def tabelas(self, **kwargs):
        self.extracao = drp.extrai(self.conexao, **kwargs)
        return {d: {lin['valor']: lin for lin in linhas} for d, linhas in drp.indicadores(self.extracao).items()}

    def test_universo_padrao_sem_exportacao(self):
        self.tabelas()
        self.assertEqual(sorted(f['id'] for f in self.extracao['fichas']), [1, 2, 4])
        r = drp.resumo(self.extracao)
        self.assertEqual((r['fichas'], r['perdimentos'], r['apreensoes'], r['descartadas_exportacao']), (3, 2, 2, 1))
        self.assertEqual(r['valor'], 1350.0)   # 1000 + 0 + 50 + 300

    def test_incluir_exportacao_e_tipos(self):
        self.tabelas(incluir_exportacao=True)
        self.assertIn(5, [f['id'] for f in self.extracao['fichas']])
        self.tabelas(tipos=(2,))
        self.assertEqual(sorted(f['id'] for f in self.extracao['fichas']), [1, 4])
        self.tabelas(inicio=datetime(2026, 1, 1))
        self.assertEqual(sorted(f['id'] for f in self.extracao['fichas']), [2, 4])

    def test_identidade_agrupa_as_fichas(self):
        t = self.tabelas()
        importador = t['importador']['11222333']           # fiscalizado + consignatário (outra filial)
        self.assertEqual(importador['qtd_fichas'], 2)
        self.assertEqual(importador['fichas'], [2, 1])
        self.assertEqual(importador['descricao'], 'IMPORTADORA ALFA LTDA')
        self.assertEqual((importador['perdimentos'], importador['apreensoes']), (1, 2))
        self.assertEqual(t['embarcador']['ACME']['qtd_fichas'], 2)   # "ACME CO" e "acme co "

    def test_citacoes_nos_textos_sem_cpf_de_auditor(self):
        t = self.tabelas()
        self.assertEqual(list(t['cpf_citado']), [CPF])
        self.assertEqual(list(t['cnpj_citado']), [CNPJ_OUTRA_FILIAL])
        self.assertEqual(t['conteiner'][CONTEINER]['qtd_fichas'], 1)   # lote da RVF e texto: a mesma ficha

    def test_mercante_ncm_rota_baldeacao(self):
        t = self.tabelas()
        self.assertEqual(t['ncm_declarado']['8471']['qtd_fichas'], 2)
        self.assertEqual(t['ncm_encontrado']['8471']['qtd_fichas'], 1)
        self.assertEqual(t['pais_ncm']['CN 8471']['qtd_fichas'], 2)
        self.assertIn('China', t['pais_ncm']['CN 8471']['descricao'])
        self.assertEqual(set(t['rota']), {'CNSHA > BRSSZ', 'CNNGB > BRSSZ'})
        self.assertEqual(list(t['porto_baldeacao']), ['SGSIN'])          # M2 carregado fora da origem
        self.assertEqual(t['pais_origem']['CN']['descricao'], 'China')
        self.assertNotIn('BR', t['pais_origem'])                        # a exportação ficou de fora

    def test_categorias_da_ficha(self):
        t = self.tabelas()
        self.assertEqual(list(t['marca']), ['MARCA X'])
        self.assertEqual(list(t['infracao']), ['Falsa declaração'])
        self.assertEqual(list(t['apreensao']), ['Cocaína'])
        self.assertEqual(list(t['flag']), ['Subfaturamento'])

    def test_ce_fora_do_mercante(self):
        self.tabelas()
        ficha4 = next(f for f in self.extracao['fichas'] if f['id'] == 4)
        self.assertEqual(ficha4['ces'], {'152600000000004'})
        self.assertEqual(ficha4['sinais']['rota'], set())
        self.assertEqual((self.extracao['ces'], self.extracao['ces_no_mercante']), (4, 3))

    def test_planilha(self):
        self.tabelas()
        abas = pd.read_excel(io.BytesIO(drp.planilha_xlsx(self.extracao, {'tipos': '1,2'})), sheet_name=None,
                             engine='openpyxl')
        self.assertEqual(list(abas)[0], 'Resumo')
        self.assertEqual(list(abas)[-1], 'Fichas')
        self.assertEqual(len(abas['Fichas']), 3)
        self.assertIn('importador', abas)
        self.assertEqual(abas['importador'].iloc[0]['fichas'], '2, 1')

    def test_vazio(self):
        self.tabelas(inicio=datetime(2030, 1, 1))
        self.assertEqual(self.extracao['fichas'], [])
        self.assertEqual(drp.resumo(self.extracao)['fichas'], 0)
        drp.planilha_xlsx(self.extracao)  # não quebra


if __name__ == '__main__':
    unittest.main()
