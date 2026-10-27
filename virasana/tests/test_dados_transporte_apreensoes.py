"""Testes do script analytic_scripts/dados_transporte_apreensoes.py (SQLite, cenário simulado)."""
import os
import sys
import tempfile
import unittest
from datetime import datetime
from unittest import mock

import pandas as pd
from sqlalchemy import create_engine, text

from virasana.analytic_scripts import dados_transporte_apreensoes as dta

DDL = [
    'CREATE TABLE ovr_ovrs (id INTEGER PRIMARY KEY, numero VARCHAR(10), datahora TIMESTAMP, '
    'numeroCEmercante VARCHAR(15), observacoes VARCHAR(1000))',
    'CREATE TABLE ovr_verificacoesfisicas (id INTEGER PRIMARY KEY, ovr_id INTEGER, datahora TIMESTAMP, '
    'numeroCEmercante VARCHAR(15), numerolote VARCHAR(20), descricao VARCHAR(5000), '
    'descricao_interna VARCHAR(5000))',
    'CREATE TABLE ovr_tiposapreensao (id INTEGER PRIMARY KEY, descricao VARCHAR(50))',
    'CREATE TABLE ovr_apreensoes_rvf (id INTEGER PRIMARY KEY, rvf_id INTEGER, tipo_id INTEGER, '
    'descricao VARCHAR(200), peso NUMERIC(7, 2))',
    'CREATE TABLE apirecintos_acessosveiculo (id INTEGER PRIMARY KEY, codigoRecinto VARCHAR(7), '
    'dataHoraOcorrencia DATETIME, operacao VARCHAR(1), direcao VARCHAR(1), tipoOperacao VARCHAR(1), '
    'placa VARCHAR(7), placaSemirreboque VARCHAR(7), numeroConteiner VARCHAR(11), '
    'numeroConhecimento VARCHAR(15), cnpjTransportador VARCHAR(15), cpfMotorista VARCHAR(11), '
    'nomeMotorista VARCHAR(50))',
]


def conteiner(prefixo_e_numero):
    """Completa um contêiner válido com o dígito verificador."""
    return prefixo_e_numero + str(dta.digito_conteiner(prefixo_e_numero))


CTR_RVF = conteiner('MSCU123456')     # informado na RVF (numerolote)
CTR_TEXTO = conteiner('TGHU305438')   # citado só nas observações da OVR
CE = '152605322948092'
CPF = '52998224725'


def acesso(id_, data, **campos):
    base = {'id': id_, 'codigoRecinto': '8931356', 'dataHoraOcorrencia': data, 'operacao': 'C',
            'direcao': 'E', 'tipoOperacao': 'I', 'placa': None, 'placaSemirreboque': None,
            'numeroConteiner': None, 'numeroConhecimento': None, 'cnpjTransportador': None,
            'cpfMotorista': None, 'nomeMotorista': None}
    return {**base, **campos}


def monta_banco(engine):
    with engine.begin() as con:
        for ddl in DDL:
            con.execute(text(ddl))
        insere = lambda tabela, linhas: con.execute(  # noqa: E731
            text(f'INSERT INTO {tabela} ({", ".join(linhas[0])}) VALUES ({", ".join(":" + c for c in linhas[0])})'),
            linhas)
        insere('ovr_tiposapreensao', [{'id': 1, 'descricao': 'Cocaína'}, {'id': 2, 'descricao': 'Maconha'}])
        insere('ovr_ovrs', [
            {'id': 1, 'numero': '1/2026', 'datahora': datetime(2026, 3, 10), 'numeroCEmercante': CE,
             'observacoes': f'Motorista CPF 529.982.247-25, cavalo ABC1D23, carreta XYZ-9876. '
                            f'Ver também contêiner {CTR_TEXTO[:4]} {CTR_TEXTO[4:10]}-{CTR_TEXTO[10]}.'},
            {'id': 2, 'numero': '2/2026', 'datahora': datetime(2026, 4, 1), 'numeroCEmercante': None,
             'observacoes': 'sem pistas'},
            {'id': 3, 'numero': '3/2026', 'datahora': datetime(2026, 4, 2), 'numeroCEmercante': None,
             'observacoes': f'sem apreensão {CTR_RVF}'},
        ])
        insere('ovr_verificacoesfisicas', [
            {'id': 10, 'ovr_id': 1, 'datahora': datetime(2026, 3, 12, 10), 'numeroCEmercante': CE,
             'numerolote': f'{CTR_RVF[:4].lower()} {CTR_RVF[4:]}', 'descricao': 'NCM 8471.30, RVF 1234',
             'descricao_interna': None},
            {'id': 20, 'ovr_id': 2, 'datahora': datetime(2026, 4, 1), 'numeroCEmercante': None,
             'numerolote': 'GRANEL', 'descricao': None, 'descricao_interna': None},
            {'id': 30, 'ovr_id': 3, 'datahora': datetime(2026, 4, 2), 'numeroCEmercante': None,
             'numerolote': CTR_RVF, 'descricao': None, 'descricao_interna': None},
        ])
        insere('ovr_apreensoes_rvf', [
            {'id': 1, 'rvf_id': 10, 'tipo_id': 1, 'descricao': 'tijolos na carga', 'peso': 25.5},
            {'id': 2, 'rvf_id': 10, 'tipo_id': 2, 'descricao': 'na porta', 'peso': 3},
            {'id': 3, 'rvf_id': 20, 'tipo_id': 1, 'descricao': None, 'peso': None},
        ])
        motorista_joao = dict(placa='ABC1D23', cpfMotorista=CPF, nomeMotorista='JOAO',
                              cnpjTransportador='11222333000181')
        insere('apirecintos_acessosveiculo', [
            # contêiner apreendido + placa + CPF do texto: três formas de ligação
            acesso(101, datetime(2026, 3, 1, 8), numeroConteiner=CTR_RVF, **motorista_joao),
            acesso(108, datetime(2026, 3, 1, 8), numeroConteiner=CTR_RVF, tipoOperacao='R',
                   **motorista_joao),                                               # retificação do 101
            acesso(109, datetime(2026, 2, 28, 20), numeroConteiner=CTR_RVF, operacao='G',
                   **motorista_joao),                                               # agendamento
            acesso(110, datetime(2026, 3, 10), numeroConteiner='OUTRO0000001',
                   **motorista_joao),                                               # outra viagem do JOAO
            acesso(102, datetime(2026, 3, 20), direcao='S', numeroConteiner=CTR_RVF, numeroConhecimento=CE,
                   placa='DEF4G56'),
            acesso(103, datetime(2026, 3, 5), numeroConteiner=CTR_TEXTO, placa='GHI7J89'),
            acesso(104, datetime(2026, 3, 15), placa='KLM1N23', placaSemirreboque='XYZ9876'),
            acesso(107, datetime(2026, 3, 11), numeroConteiner='OUTRO0000002', numeroConhecimento=CE,
                   placa='PQR1S23'),                                                # outro contêiner do CE
            acesso(105, datetime(2025, 1, 1), numeroConteiner=CTR_RVF),        # fora da janela
            acesso(106, datetime(2026, 3, 2), placa='NCM8471'),                  # "placa" falsa do texto
        ])


class TestIdentificadores(unittest.TestCase):

    def test_conteiner(self):
        self.assertTrue(dta.conteiner_valido('CSQU3054383'))  # exemplo da ISO 6346
        self.assertFalse(dta.conteiner_valido('CSQU3054384'))
        self.assertFalse(dta.conteiner_valido(None))
        self.assertEqual(dta.normaliza_conteiner(' mscu 123456-7 '), 'MSCU1234567')
        self.assertIsNone(dta.normaliza_conteiner('GRANEL'))
        self.assertEqual(dta.extrai_conteineres('ctr csqu 305438-3 e CSQU3054384'), ['CSQU3054383'])

    def test_placas(self):
        self.assertEqual(dta.extrai_placas('cavalo ABC1D23, carreta xyz-9876'), ['ABC1D23', 'XYZ9876'])
        self.assertEqual(dta.extrai_placas('NCM 8471.30; NCM 8471; RVF 1234; peso 1234,5'), [])

    def test_cpfs(self):
        self.assertEqual(dta.extrai_cpfs('cpf 529.982.247-25 e 52998224725'), [CPF])
        self.assertEqual(dta.extrai_cpfs('111.111.111-11, 12345678900, CE 152605322948092'), [])


class TestRelaciona(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine('sqlite://')
        monta_banco(self.engine)
        self.conexao = self.engine.connect()

    def tearDown(self):
        self.conexao.close()

    def linhas(self, **kwargs):
        apreensoes = dta.busca_apreensoes(self.conexao, datetime(2026, 1, 1), datetime(2026, 12, 31))
        return dta.relaciona(self.conexao, apreensoes, **kwargs)

    def test_so_rvfs_com_apreensao_e_apreensoes_agregadas(self):
        apreensoes = dta.busca_apreensoes(self.conexao, datetime(2026, 1, 1), datetime(2026, 12, 31))
        self.assertEqual(sorted(apreensoes['rvf_id']), [10, 20])
        rvf10 = apreensoes[apreensoes['rvf_id'] == 10].iloc[0]
        self.assertEqual(rvf10['apreensao_tipos'], 'Cocaína; Maconha')
        self.assertAlmostEqual(rvf10['apreensao_peso_total'], 28.5)

    def rvf10(self, **kwargs):
        return {linha['placa']: linha for linha in self.linhas(**kwargs) if linha['rvf_id'] == 10}

    def test_padrao_so_conteiner_apreendido_agrupado_por_motorista_veiculo(self):
        por_placa = self.rvf10()
        self.assertEqual(sorted(por_placa), ['ABC1D23', 'DEF4G56', 'GHI7J89'])
        joao = por_placa['ABC1D23']
        self.assertEqual(joao['forma_ligacao'], 'conteiner_rvf+placa_texto+cpf_texto')
        self.assertEqual(joao['nomeMotorista'], 'JOAO')
        self.assertEqual(joao['qtd_ocorrencias'], 2)  # acesso + agendamento; a retificação não conta
        self.assertEqual(joao['movimentos'], 'acesso entrada: 1, agendamento entrada: 1')
        self.assertEqual(joao['conteineres'], CTR_RVF)
        self.assertEqual(joao['dias_da_rvf_mais_proximo'], -11.1)
        self.assertEqual(por_placa['DEF4G56']['forma_ligacao'], 'conteiner_rvf+ce_mercante')
        self.assertEqual(por_placa['GHI7J89']['forma_ligacao'], 'conteiner_texto')

    def test_ce_sozinho_nao_traz_outros_conteineres_do_ce(self):
        self.assertNotIn('PQR1S23', self.rvf10())

    def test_incluir_outras_viagens(self):
        linhas = [linha for linha in self.linhas(incluir_outras_viagens=True) if linha['rvf_id'] == 10]
        por_forma = {(linha['placa'], linha['forma_ligacao']): linha for linha in linhas}
        self.assertEqual(len(linhas), 6)
        self.assertIn(('ABC1D23', 'placa_texto+cpf_texto'), por_forma)      # a outra viagem do JOAO
        self.assertEqual(por_forma[('KLM1N23', 'placa_texto')]['valor_ligacao'], 'XYZ9876')  # semirreboque
        self.assertEqual(por_forma[('PQR1S23', 'ce_mercante')]['conteineres'], 'OUTRO0000002')

    def test_rvf_sem_acesso_sai_como_nenhuma(self):
        linhas = [linha for linha in self.linhas() if linha['rvf_id'] == 20]
        self.assertEqual(len(linhas), 1)
        self.assertEqual(linhas[0]['forma_ligacao'], 'nenhuma')

    def test_janela_de_dias(self):
        # ±5 dias da RVF (12/03): nada do contêiner apreendido; com outras viagens, 110 e 104
        self.assertEqual({linha['forma_ligacao'] for linha in self.linhas(dias_antes=5, dias_depois=5)},
                         {'nenhuma'})
        placas = {linha.get('placa') for linha in self.linhas(dias_antes=5, dias_depois=5,
                                                              incluir_outras_viagens=True)}
        self.assertEqual(placas, {'ABC1D23', 'KLM1N23', 'PQR1S23', None})

    def test_campo_fora_da_lista_e_recusado(self):
        with self.assertRaises(ValueError):
            dta.busca_acessos(self.conexao, 'placa; DROP TABLE x', 'ABC1234',
                              datetime(2026, 1, 1), datetime(2026, 2, 1))

    def test_main_grava_csv(self):
        with tempfile.TemporaryDirectory() as pasta:
            banco = os.path.join(pasta, 'teste.db')
            monta_banco(create_engine('sqlite:///' + banco))
            saida = os.path.join(pasta, 'saida.csv')
            argv = ['dta', '--uri', 'sqlite:///' + banco, '--inicio', '2026-01-01', '--saida', saida]
            with mock.patch.object(sys, 'argv', argv):
                dta.main()
            df = pd.read_csv(saida, sep=';', encoding='utf-8-sig')
        self.assertEqual(list(df.columns), dta.COLUNAS_CSV)
        self.assertEqual(len(df), 4)  # 3 conjuntos da RVF 10 + "nenhuma" da RVF 20
        self.assertEqual(set(df['rvf_id']), {10, 20})


if __name__ == '__main__':
    unittest.main()
