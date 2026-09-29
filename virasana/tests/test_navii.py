"""Unit tests para a camada de dados do NAVI-I (virasana.navii.busca_navii)."""
import unittest
from datetime import datetime, timedelta

from virasana.navii.busca_navii import (
    JANELA_EFETIVA_MAX_DIAS, JANELA_EFETIVA_MIN_DIAS,
    JANELA_PREVISTA_MAX_DIAS, JANELA_PREVISTA_MIN_DIAS,
    _janela_efetiva, _janela_previsao, _texto_para_float,
    busca_itens_navii, busca_nomes_empresas,
)
# Importados depois de busca_navii, que já ajusta o sys.path para o repo
# irmão bhadrasana2/ajna_commons (ver virasana/navii/busca_navii.py).
from virasana.integracao.mercante.mercantealchemy import Enumerado, Item, NCMItem
from bhadrasana.models.laudo import Empresa
from bhadrasana.models.laudo import NCM as LaudoNCM


class SessaoFake:
    """Sessão SQLAlchemy fake: despacha `.query(Modelo)` para uma lista de
    resultados pré-definida por modelo. Modelo não registrado derruba o teste
    (evita mascarar uma query inesperada)."""

    def __init__(self, respostas_por_modelo):
        self._respostas = respostas_por_modelo

    def query(self, modelo):
        respostas = self._respostas
        if modelo not in respostas:
            raise AssertionError(f'modelo inesperado: {modelo}')

        class QueryFake:
            def __init__(self, resultado):
                self._resultado = resultado

            def filter(self, *a, **k):
                return self

            def all(self):
                return self._resultado

        return QueryFake(respostas[modelo])


class SessaoQueNuncaDeveSerUsada:
    def query(self, *args, **kwargs):
        raise AssertionError('não deveria consultar a sessão')


class TestJanelasNavii(unittest.TestCase):
    """Garante que as janelas de atracação (sugestão de período) batem com a
    história de usuário do NAVI-I: previsão de 3 a 15 dias à frente, efetiva
    de 5 a 15 dias atrás."""

    def test_constantes_janela_prevista(self):
        self.assertEqual(JANELA_PREVISTA_MIN_DIAS, 3)
        self.assertEqual(JANELA_PREVISTA_MAX_DIAS, 15)

    def test_constantes_janela_efetiva(self):
        self.assertEqual(JANELA_EFETIVA_MIN_DIAS, 5)
        self.assertEqual(JANELA_EFETIVA_MAX_DIAS, 15)

    def test_janela_previsao_3_a_15_dias_a_frente(self):
        hoje = datetime.now()
        inicio_str, fim_str = _janela_previsao()
        inicio = datetime.strptime(inicio_str, '%Y-%m-%d')
        fim = datetime.strptime(fim_str, '%Y-%m-%d')
        self.assertEqual(inicio.date(), (hoje + timedelta(days=3)).date())
        self.assertEqual(fim.date(), (hoje + timedelta(days=15)).date())

    def test_janela_efetiva_5_a_15_dias_atras(self):
        hoje = datetime.now()
        inicio, fim = _janela_efetiva()
        self.assertAlmostEqual(inicio, hoje - timedelta(days=15), delta=timedelta(seconds=5))
        self.assertAlmostEqual(fim, hoje - timedelta(days=5), delta=timedelta(seconds=5))


class TestTextoParaFloat(unittest.TestCase):
    """Conversão defensiva dos campos texto do Mercante (Item.pesoBruto/tara)."""

    def test_valor_valido(self):
        self.assertEqual(_texto_para_float('1234.567'), 1234.567)

    def test_valor_com_virgula(self):
        self.assertEqual(_texto_para_float('1234,5'), 1234.5)

    def test_valor_none(self):
        self.assertIsNone(_texto_para_float(None))

    def test_valor_vazio(self):
        self.assertIsNone(_texto_para_float(''))

    def test_valor_nao_numerico_nao_derruba(self):
        self.assertIsNone(_texto_para_float('não informado'))


class TestBuscaItensNavii(unittest.TestCase):
    """busca_itens_navii não deve bater no banco quando não há CEs a enriquecer."""

    def test_lista_vazia_nao_consulta_sessao(self):
        self.assertEqual(busca_itens_navii(SessaoQueNuncaDeveSerUsada(), []), {})

    def test_ignora_numeros_vazios_ou_none(self):
        self.assertEqual(busca_itens_navii(SessaoQueNuncaDeveSerUsada(), [None, '']), {})

    def test_agrupa_conteineres_e_ncms_por_ce(self):
        itens = [
            Item(numeroCEmercante='CE1', codigoConteiner='CONT1', isoCode='22G1',
                pesoBruto='1000.5', tara='2200', lacre='LAC1'),
            Item(numeroCEmercante='CE1', codigoConteiner='CONT2', isoCode='42G1',
                pesoBruto='2500', tara='3800', lacre='LAC2'),
        ]
        ncm_itens = [
            NCMItem(numeroCEMercante='CE1', codigoConteiner='CONT1',
                    identificacaoNCM='8471.30', descritivo='Notebook'),
            NCMItem(numeroCEMercante='CE1', codigoConteiner='CONT1',
                    identificacaoNCM='8471.60', descritivo='Teclado'),
            NCMItem(numeroCEMercante='CE1', codigoConteiner='CONT2',
                    identificacaoNCM='8471.30', descritivo='Notebook'),
        ]
        sessao = SessaoFake({Item: itens, NCMItem: ncm_itens, LaudoNCM: []})

        resultado = busca_itens_navii(sessao, ['CE1'])

        self.assertEqual(resultado['CE1']['pesoBrutoTotal'], 3500.5)
        # 8471.30 se repete nos dois contêineres -> 2 NCMs distintos no total
        self.assertEqual(resultado['CE1']['qtdNcmsDistintos'], 2)
        self.assertEqual(len(resultado['CE1']['conteineres']), 2)
        cont1 = next(c for c in resultado['CE1']['conteineres'] if c['codigoConteiner'] == 'CONT1')
        self.assertEqual(cont1['lacre'], 'LAC1')
        self.assertEqual(cont1['pesoBruto'], 1000.5)
        self.assertEqual(len(cont1['ncms']), 2)

    def test_descricao_ncm_prioriza_laudo_mas_cai_para_manifesto(self):
        itens = [Item(numeroCEmercante='CE1', codigoConteiner='CONT1')]
        ncm_itens = [
            NCMItem(numeroCEMercante='CE1', codigoConteiner='CONT1',
                    identificacaoNCM='8471.30', descritivo='descrição do manifesto'),
            NCMItem(numeroCEMercante='CE1', codigoConteiner='CONT1',
                    identificacaoNCM='9999.99', descritivo='só existe no manifesto'),
        ]
        laudo_ncms = [LaudoNCM(title='8471.30', contents='descrição oficial do laudo')]
        sessao = SessaoFake({Item: itens, NCMItem: ncm_itens, LaudoNCM: laudo_ncms})

        resultado = busca_itens_navii(sessao, ['CE1'])
        ncms = {n['identificacaoNCM']: n['descritivo'] for n in resultado['CE1']['conteineres'][0]['ncms']}

        self.assertEqual(ncms['8471.30'], 'descrição oficial do laudo')
        self.assertEqual(ncms['9999.99'], 'só existe no manifesto')

    def test_tipo_carga_decodificado_via_enumerado(self):
        itens = [Item(numeroCEmercante='CE1', codigoConteiner='CONT1', tipoItemCarga='5')]
        enumerados = [Enumerado(id='5', tipoItemCarga='Carga Solta')]
        sessao = SessaoFake({Item: itens, NCMItem: [], LaudoNCM: [], Enumerado: enumerados})

        resultado = busca_itens_navii(sessao, ['CE1'])

        self.assertEqual(resultado['CE1']['conteineres'][0]['tipoCarga'], 'Carga Solta')

    def test_tipo_carga_ausente_no_enumerado_fica_none(self):
        itens = [Item(numeroCEmercante='CE1', codigoConteiner='CONT1', tipoItemCarga='999')]
        sessao = SessaoFake({Item: itens, NCMItem: [], LaudoNCM: [], Enumerado: []})

        resultado = busca_itens_navii(sessao, ['CE1'])

        self.assertIsNone(resultado['CE1']['conteineres'][0]['tipoCarga'])


class TestBuscaNomesEmpresas(unittest.TestCase):
    """busca_nomes_empresas não deve bater no banco quando não há CNPJs a resolver."""

    def test_lista_vazia_nao_consulta_sessao(self):
        self.assertEqual(busca_nomes_empresas(SessaoQueNuncaDeveSerUsada(), []), {})

    def test_ignora_valores_vazios_ou_none(self):
        self.assertEqual(busca_nomes_empresas(SessaoQueNuncaDeveSerUsada(), [None, '']), {})

    def test_resolve_cnpj_para_nome(self):
        empresas = [Empresa(cnpj='40260836000140', nome='NEX COMEX ASSESSORIA LTDA')]
        sessao = SessaoFake({Empresa: empresas})

        resultado = busca_nomes_empresas(sessao, ['40260836000140'])

        self.assertEqual(resultado, {'40260836000140': 'NEX COMEX ASSESSORIA LTDA'})


if __name__ == '__main__':
    unittest.main()
