"""Testes das tabelas de referência locais do NAVI-I (virasana/navii/referencias)."""
import gzip
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from virasana.navii import referencias
from virasana.navii.referencias.atualiza_referencias import (
    codigos_usados, filtra_usados, gera_navios, gera_ncm, gera_paises, gera_portos,
)


class TestReferencias(unittest.TestCase):
    """Usa os JSON versionados no repo (gerados por atualiza_referencias.py)."""

    def test_tabelas_carregadas(self):
        # portos/navios podem estar reduzidos pelo filtro de uso (usados.json)
        quantidades = referencias._tabela('fontes.json')['quantidades']
        self.assertGreater(quantidades['portos_na_fonte'], 10000)
        self.assertGreater(quantidades['navios_na_fonte'], 50000)
        self.assertGreater(quantidades['portos'], 0)
        self.assertGreater(quantidades['navios'], 0)
        self.assertGreater(quantidades['ncm_posicoes'], 1200)
        self.assertGreater(quantidades['paises'], 240)

    def test_pais(self):
        self.assertEqual(referencias.nome_pais('BR'), 'Brasil')
        self.assertEqual(referencias.nome_pais('cn'), 'China')
        self.assertEqual(referencias.nome_pais('CNSHA'), 'China')  # aceita o código do porto
        self.assertEqual(referencias.nome_pais('HK'), 'Hong Kong')  # fora do IBGE, veio da Wikidata
        self.assertIsNone(referencias.nome_pais('??'))
        self.assertIsNone(referencias.nome_pais(None))

    def test_porto(self):
        self.assertEqual(referencias.descreve_porto('BRSSZ'), 'Santos, Brasil')
        self.assertEqual(referencias.nome_porto('USPEF'), 'Port Everglades')
        self.assertEqual(referencias.descreve_porto('HKHKG'), 'Hong Kong')  # sem repetir porto e país

    def test_porto_ajustado_a_mao_tem_precedencia(self):
        # no UN/LOCODE, CNSHA é o aeroporto de Hongqiao; no Mercante é o porto de Xangai
        self.assertEqual(referencias.descreve_porto('CNSHA'), 'Shanghai, China')

    def test_porto_desconhecido_mostra_so_o_pais_e_nunca_um_nome_errado(self):
        self.assertEqual(referencias.descreve_porto('CNZZZ'), 'China')   # "não especificado"
        self.assertEqual(referencias.descreve_porto('BRITA'), 'Brasil')  # no LOCODE seria Itabuna/BA
        self.assertEqual(referencias.descreve_porto('BR400'), 'Brasil')  # código próprio do Mercante
        self.assertIsNone(referencias.descreve_porto('??123'))
        self.assertIsNone(referencias.descreve_porto(''))

    def test_posicao_ncm_em_qualquer_formato(self):
        for codigo in ('84713012', '8471.30.12', '8471'):
            with self.subTest(codigo=codigo):
                self.assertIn('processamento de dados', referencias.descricao_posicao_ncm(codigo))
        self.assertIn('rinquedos', referencias.descricao_posicao_ncm('95030010'))  # posição sem linha de 4 dígitos
        self.assertIsNone(referencias.descricao_posicao_ncm('0000'))
        self.assertIsNone(referencias.descricao_posicao_ncm(None))

    def test_navio(self):
        self.assertEqual(referencias.nome_navio('9519119'), 'BRASILIA HIGHWAY')
        self.assertIsNone(referencias.nome_navio('0060024895'))  # inscrição nacional: sem fonte
        self.assertIsNone(referencias.nome_navio(None))


class TestGeradores(unittest.TestCase):
    """Transformações do atualiza_referencias.py, com dados mínimos no formato das fontes."""

    def test_ncm_posicao_sem_linha_propria_e_codigo_vencido(self):
        bruto = {'Nomenclaturas': [
            {'Codigo': '84', 'Descricao': 'Máquinas.', 'Data_Fim': '31/12/9999'},
            {'Codigo': '84.71', 'Descricao': 'Máquinas de <i>processamento</i> de dados.', 'Data_Fim': '31/12/9999'},
            {'Codigo': '8471.30', 'Descricao': '- Portáteis', 'Data_Fim': '31/12/9999'},
            {'Codigo': '9503.00.10', 'Descricao': 'Triciclos', 'Data_Fim': '31/12/9999'},
            {'Codigo': '9503.00', 'Descricao': 'Brinquedos:', 'Data_Fim': '31/12/9999'},
            {'Codigo': '01.01', 'Descricao': 'Revogada', 'Data_Fim': '30/09/2026'},
        ]}
        self.assertEqual(gera_ncm(bruto), {
            'capitulos': {'84': 'Máquinas'},
            'posicoes': {'8471': 'Máquinas de processamento de dados', '9503': 'Brinquedos'},
        })

    def test_portos_so_com_funcao_de_porto(self):
        linhas = [
            {'Country': 'CN', 'Location': 'SHA', 'Name': 'Shanghai Hongqiao International Apt', 'Function': '---4----'},
            {'Country': 'CN', 'Location': 'SHG', 'Name': 'Shanghai Pt', 'Function': '1-------'},
            {'Country': 'BR', 'Location': 'SSZ', 'Name': 'Santos', 'Function': '123--6--'},
            {'Country': 'BR', 'Location': '', 'Name': 'Brazil', 'Function': ''},
        ]
        self.assertEqual(gera_portos(linhas), {'CNSHG': 'Shanghai Pt', 'BRSSZ': 'Santos'})

    def test_paises_ibge_tem_precedencia_e_wikidata_completa(self):
        ibge = [{'id': {'ISO-3166-1-ALPHA-2': 'BR'}, 'nome': {'abreviado': 'Brasil'}}]
        wikidata = {'results': {'bindings': [
            {'codigo': {'value': 'BR'}, 'nome': {'value': 'República Federativa do Brasil', 'xml:lang': 'pt'}},
            {'codigo': {'value': 'HK'}, 'nome': {'value': 'hong Kong', 'xml:lang': 'pt'}},
        ]}}
        self.assertEqual(gera_paises(ibge, wikidata), {'BR': 'Brasil', 'HK': 'Hong Kong'})

    def test_navios_descarta_rotulos_sem_nome_e_limpa_sufixo(self):
        wikidata = {'results': {'bindings': [
            {'imo': {'value': '9951915'}, 'nome': {'value': 'Jasmine K (ship, 2023)'}},
            {'imo': {'value': '5367685'}, 'nome': {'value': 'IMO 5367685'}},
            {'imo': {'value': '123'}, 'nome': {'value': 'IMO curto'}},
            {'imo': {'value': '9519119'}, 'nome': {'value': 'Brasilia Highway'}},
            {'imo': {'value': '9519119'}, 'nome': {'value': 'Brasilia Highway II'}},
        ]}}
        self.assertEqual(gera_navios(wikidata), {'9951915': 'JASMINE K', '9519119': 'BRASILIA HIGHWAY'})


class TestLeituraTolerante(unittest.TestCase):
    """Arquivos baixados em gzip ou em outra codificação não podem derrubar a geração."""

    def grava_e_le(self, dados: bytes) -> str:
        from virasana.navii.referencias.atualiza_referencias import le_texto
        with tempfile.TemporaryDirectory() as pasta:
            caminho = os.path.join(pasta, 'bruto')
            with open(caminho, 'wb') as arquivo:
                arquivo.write(dados)
            return le_texto(caminho)

    def test_gzip(self):
        # o IBGE respondeu em gzip e o urllib gravou comprimido (erro 0x8b na posição 1)
        self.assertEqual(self.grava_e_le(gzip.compress('[{"nome": "Brasília"}]'.encode('utf-8'))),
                         '[{"nome": "Brasília"}]')

    def test_codificacoes(self):
        self.assertEqual(self.grava_e_le('﻿São Paulo'.encode('utf-8')), 'São Paulo')  # BOM
        self.assertEqual(self.grava_e_le('São Paulo'.encode('cp1252')), 'São Paulo')

    def test_falha_numa_fonte_mantem_a_tabela_anterior_e_gera_as_demais(self):
        from virasana.navii.referencias import atualiza_referencias as ar
        with tempfile.TemporaryDirectory() as destino, tempfile.TemporaryDirectory() as brutos:
            def escreve(pasta, nome, dados):
                with open(os.path.join(pasta, nome), 'wb') as arquivo:
                    arquivo.write(dados)
            escreve(destino, 'navios.json', b'{"9519119": "ANTERIOR"}')  # versão anterior
            escreve(brutos, 'ncm.json', json.dumps({'Nomenclaturas': [
                {'Codigo': '84.71', 'Descricao': 'Máquinas', 'Data_Fim': '31/12/9999'}]}).encode('utf-8'))
            escreve(brutos, 'locode.csv', gzip.compress(
                'Country,Location,Name,Function\nBR,SSZ,Santos,1-------\n'.encode('utf-8')))
            escreve(brutos, 'paises.json', json.dumps(
                [{'id': {'ISO-3166-1-ALPHA-2': 'BR'}, 'nome': {'abreviado': 'Brasil'}}],
                ensure_ascii=False).encode('cp1252'))
            escreve(brutos, 'paises_wikidata.json', b'{"results": {"bindings": []}}')
            escreve(brutos, 'navios.json', b'\x8b\x00 corrompido')  # ilegível
            argv = ['atualiza', '--brutos', brutos, '--sem-filtro-de-uso']
            with mock.patch.object(ar, 'AQUI', destino), mock.patch.object(sys, 'argv', argv):
                ar.main()

            def le(nome):
                with open(os.path.join(destino, nome), encoding='utf-8') as arquivo:
                    return json.load(arquivo)
            self.assertEqual(le('navios.json'), {'9519119': 'ANTERIOR'})
            self.assertEqual(le('portos.json'), {'BRSSZ': 'Santos'})
            self.assertEqual(le('paises.json'), {'BR': 'Brasil'})
            self.assertEqual(le('ncm.json')['posicoes'], {'8471': 'Máquinas'})
            self.assertEqual(le('fontes.json')['nao_atualizados_nesta_execucao'], ['navios.json'])


class TestFiltroDeUso(unittest.TestCase):
    """Navios/portos efetivamente usados na base (consulta real em SQLite, cenário simulado)."""

    def test_filtra_usados(self):
        tabela = {'BRSSZ': 'Santos', 'NOOSL': 'Oslo'}
        self.assertEqual(filtra_usados(tabela, ['BRSSZ', 'CNSHA']), {'BRSSZ': 'Santos'})
        self.assertEqual(filtra_usados(tabela, None), tabela)  # sem usados.json não filtra
        self.assertEqual(filtra_usados(tabela, []), {})

    def test_codigos_usados_so_navios_de_escalas_com_conteiner(self):
        from virasana.integracao.mercante.mercantealchemy import Escala, EscalaManifesto, Manifesto
        from virasana.tests.test_navii_busca_sqlite import cria_ce, cria_sessao, monta_cenario
        session = cria_sessao()
        monta_cenario(session)  # E1 (NAVIO A, BRSSZ, M1) e E2 (NAVIO B, BRSSZ, M2): manifestos com contêineres
        session.add_all([
            # graneleiro: manifesto só com carga sem contêiner
            Escala(numeroDaEscala='E8', dataPrevistaAtracacao='2026-01-09', embarcacao='GRANELEIRO',
                   portoDaEscala='BRPNG'),
            EscalaManifesto(ID=8, manifesto='M8', escala='E8'),
            Manifesto(numero='M8', portoCarregamento='USMSY', portoDescarregamento='BRPNG'),
            # escala antiga, fora do período
            Escala(numeroDaEscala='E9', dataPrevistaAtracacao='2020-01-01', embarcacao='NAVIO VELHO',
                   portoDaEscala='BRRIO'),
            EscalaManifesto(ID=9, manifesto='M1', escala='E9'),
        ])
        cria_ce(session, 'CE8', 'M8', {'': [('1001.99', 'Trigo')]}, porto_origem='USMSY', porto_destino='BRPNG')
        session.commit()

        usados = codigos_usados(session.connection(), '2026-01-01')

        self.assertEqual(usados['navios'], ['NAVIO A', 'NAVIO B'])
        self.assertEqual(usados['desde'], '2026-01-01')
        # portos das escalas com contêiner + origem/destino dos CEs do período (create_date = agora)
        self.assertTrue({'BRSSZ', 'CNSHA', 'CNNGB', 'BRRIG'} <= set(usados['portos']))
        self.assertNotIn('BRRIO', usados['portos'])
        self.assertNotIn('', usados['portos'])
        session.close()


if __name__ == '__main__':
    unittest.main()
