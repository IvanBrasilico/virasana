"""Testes da busca por texto do NAVI-I (navii/texto_navii.py): cliente HTTP do Meilisearch (sessão simulada),
documento do CE, data de referência, configuração do índice e leitura do resultado da busca."""
import json
import unittest
from datetime import date, datetime
from types import SimpleNamespace

from virasana.navii import texto_navii as tn

CNPJ_A, CNPJ_B = '11222333000181', '99888777000166'


class Resposta:
    def __init__(self, status=200, corpo=None):
        self.status_code, self._corpo = status, corpo
        self.content = b'' if corpo is None else json.dumps(corpo).encode()
        self.text = self.content.decode()

    def json(self):
        if self._corpo is None:
            raise ValueError('sem corpo')
        return self._corpo


class SessaoFalsa:
    """Registra os pedidos e devolve as respostas da fila (ou 200 vazio)."""

    def __init__(self, *respostas):
        self.respostas, self.pedidos = list(respostas), []

    def request(self, metodo, url, headers=None, timeout=None, **kwargs):
        self.pedidos.append((metodo, url, headers, timeout, kwargs))
        return self.respostas.pop(0) if self.respostas else Resposta(200, {})


def ce(**campos):
    padrao = dict(ID=10, numeroCEmercante=' 152605000000001 ', manifestoCE='1526500000001', numeroCEMaster='',
                  numConhecimento='MEDU123', embarcador='SHANGHAI  ACME CO', consignatario=CNPJ_A, notify=None,
                  cnpjNotify=None, descricao='PECAS\nDE  BICICLETA', codigoEmpresaNavegacao='02378779000109',
                  portoOrigemCarga='CNSHA', portoDestFinal='BRSSZ', tipoTrafego='5')
    padrao.update(campos)
    return SimpleNamespace(**padrao)


class TestCliente(unittest.TestCase):

    def test_pedidos_com_a_chave_e_os_caminhos(self):
        sessao = SessaoFalsa(Resposta(202, {'taskUid': 7}), Resposta(202, {'taskUid': 8}))
        cliente = tn.ClienteMeili('http://127.0.0.1:7700/', 'segredo', timeout=5, sessao=sessao)
        self.assertEqual(cliente.adiciona('navii_ces', [{'ce': '1'}], 'ce'), {'taskUid': 7})
        cliente.apaga_por_filtro('navii_ces', 'id < 10')
        (metodo, url, cabecalhos, timeout, kwargs), (metodo2, url2, _, _, kwargs2) = sessao.pedidos
        self.assertEqual((metodo, url, timeout), ('POST', 'http://127.0.0.1:7700/indexes/navii_ces/documents', 5))
        self.assertEqual(cabecalhos, {'Authorization': 'Bearer segredo'})
        self.assertEqual(kwargs, {'params': {'primaryKey': 'ce'}, 'json': [{'ce': '1'}]})
        self.assertEqual((metodo2, url2, kwargs2), ('POST', 'http://127.0.0.1:7700/indexes/navii_ces/documents/delete',
                                                    {'json': {'filter': 'id < 10'}}))

    def test_sem_chave_sem_cabecalho(self):
        sessao = SessaoFalsa(Resposta(200, {'status': 'available'}))
        self.assertEqual(tn.ClienteMeili(sessao=sessao).saude(), {'status': 'available'})
        self.assertEqual(sessao.pedidos[0][1:3], ('http://127.0.0.1:7700/health', {}))

    def test_404_e_none_so_onde_faz_sentido(self):
        cliente = tn.ClienteMeili(sessao=SessaoFalsa(Resposta(404, {'message': 'Index `x` not found.'}),
                                                     Resposta(404, {'message': 'Document not found.'}),
                                                     Resposta(404, {'message': 'Index `x` not found.'})))
        self.assertIsNone(cliente.indice('x'))
        self.assertIsNone(cliente.documento('x', 'marcas'))
        with self.assertRaisesRegex(tn.ErroMeili, 'HTTP 404 Index `x` not found.'):
            cliente.busca('x', {'q': 'a'})

    def test_erro_sem_json(self):
        resposta = Resposta(500)
        resposta.text = 'falhou feio'
        with self.assertRaisesRegex(tn.ErroMeili, 'HTTP 500 falhou feio'):
            tn.ClienteMeili(sessao=SessaoFalsa(resposta)).saude()

    def test_espera_a_tarefa(self):
        pausas = []
        sessao = SessaoFalsa(Resposta(200, {'status': 'enqueued'}), Resposta(200, {'status': 'processing'}),
                             Resposta(200, {'uid': 3, 'status': 'succeeded'}))
        cliente = tn.ClienteMeili(sessao=sessao, dorme=pausas.append)
        self.assertEqual(cliente.espera({'taskUid': 3}, pausa=0.1)['status'], 'succeeded')
        self.assertEqual(pausas, [0.1, 0.1])
        self.assertTrue(all(p[1].endswith('/tasks/3') for p in sessao.pedidos))

    def test_espera_tarefa_que_falhou_ou_demorou(self):
        falhou = SessaoFalsa(Resposta(200, {'status': 'failed', 'error': {'message': 'invalid_document_id'}}))
        with self.assertRaisesRegex(tn.ErroMeili, 'tarefa 4 failed: invalid_document_id'):
            tn.ClienteMeili(sessao=falhou, dorme=lambda s: None).espera({'taskUid': 4}, limite=-1)
        demorou = SessaoFalsa(Resposta(200, {'status': 'processing'}))
        with self.assertRaisesRegex(tn.ErroMeili, 'ainda processing'):
            tn.ClienteMeili(sessao=demorou, dorme=lambda s: None).espera({'taskUid': 5}, limite=-1)

    def test_cliente_do_ambiente(self):
        cliente = tn.cliente_do_ambiente('NAVII_MEILI_CHAVE_BUSCA', {'NAVII_MEILI_URL': 'http://127.0.0.1:7701',
                                                                     'NAVII_MEILI_CHAVE_BUSCA': 'b'})
        self.assertEqual((cliente.url, cliente.cabecalhos), ('http://127.0.0.1:7701', {'Authorization': 'Bearer b'}))
        self.assertEqual(tn.cliente_do_ambiente('X', {}).url, tn.URL_PADRAO)


class TestDocumento(unittest.TestCase):

    def test_documento_com_nome_do_cnpj_e_texto_limpo(self):
        documento = tn.documento_do_ce(ce(), {CNPJ_A: 'IMPORTADORA ALFA LTDA'}, datetime(2026, 10, 20))
        self.assertEqual(documento, {
            'ce': '152605000000001', 'id': 10, 'manifesto': '1526500000001', 'master': '', 'bl': 'MEDU123',
            'notify': '', 'cnpjNotify': '', 'descricao': 'PECAS DE BICICLETA', 'transportador': '02378779000109',
            'origem': 'CNSHA', 'destino': 'BRSSZ', 'referencia': 1792454400,
            'embarcador': 'SHANGHAI ACME CO', 'embarcadorCnpj': '',
            'consignatario': 'IMPORTADORA ALFA LTDA', 'consignatarioCnpj': CNPJ_A})
        self.assertEqual(tn.de_segundos(documento['referencia']), date(2026, 10, 20))

    def test_cnpj_sem_nome_e_sem_referencia(self):
        documento = tn.documento_do_ce(ce(embarcador=CNPJ_B, consignatario=None), {}, None)
        self.assertEqual((documento['embarcador'], documento['embarcadorCnpj']), ('', CNPJ_B))
        self.assertEqual((documento['consignatario'], documento['consignatarioCnpj']), ('', ''))
        self.assertIsNone(documento['referencia'])
        self.assertEqual(tn.cnpjs_do_ce(ce(embarcador=CNPJ_B)), [CNPJ_B, CNPJ_A])

    def test_numero_que_parece_cnpj_mas_nao_e(self):
        documento = tn.documento_do_ce(ce(consignatario='1122233300018'), {}, None)    # 13 dígitos: é texto
        self.assertEqual((documento['consignatario'], documento['consignatarioCnpj']), ('1122233300018', ''))

    def test_ce_sem_numero_e_exportacao(self):
        self.assertIsNone(tn.documento_do_ce(ce(numeroCEmercante='  '), {}, None))
        self.assertFalse(tn.e_importacao(ce(tipoTrafego='7')))
        self.assertTrue(tn.e_importacao(ce(tipoTrafego=None)))

    def test_referencia(self):
        self.assertEqual(tn.referencia([('BRRIG', datetime(2026, 1, 1)), ('BRSSZ', datetime(2026, 1, 9))], 'BRSSZ'),
                         datetime(2026, 1, 9))
        self.assertEqual(tn.referencia([('BRRIG', datetime(2026, 1, 9)), ('BRITJ', datetime(2026, 1, 2)),
                                        ('BRSSZ', None)], 'BRSSZ'), datetime(2026, 1, 2))
        self.assertIsNone(tn.referencia([], 'BRSSZ'))
        self.assertIsNone(tn.em_segundos(None))


class TestConfiguracao(unittest.TestCase):

    def test_igual_mesmo_em_outra_ordem(self):
        atual = {'searchableAttributes': list(tn.CAMPOS_BUSCA),
                 'filterableAttributes': sorted(tn.CONFIGURACAO['filterableAttributes']),
                 'sortableAttributes': sorted(tn.CONFIGURACAO['sortableAttributes']),
                 'rankingRules': list(tn.CONFIGURACAO['rankingRules']),
                 'typoTolerance': {'enabled': True, 'minWordSizeForTypos': {'oneTypo': 5, 'twoTypos': 9},
                                   'disableOnAttributes': sorted(tn.SEM_ERRO_DE_DIGITACAO)}}
        self.assertEqual(tn.configuracao_difere(atual), [])

    def test_diferencas(self):
        self.assertEqual(tn.configuracao_difere({}), list(tn.CONFIGURACAO))           # índice novo: tudo
        atual = {**tn.CONFIGURACAO, 'searchableAttributes': list(reversed(tn.CAMPOS_BUSCA)),
                 'typoTolerance': {'enabled': True, 'disableOnAttributes': ['ce']}}
        self.assertEqual(tn.configuracao_difere(atual), ['searchableAttributes', 'typoTolerance'])

    def test_cnpj_e_numeros_sem_erro_de_digitacao_e_nomes_com(self):
        sem_erro = set(tn.CONFIGURACAO['typoTolerance']['disableOnAttributes'])
        self.assertTrue({'embarcadorCnpj', 'consignatarioCnpj', 'cnpjNotify', 'ce', 'bl'} <= sem_erro)
        self.assertFalse({'embarcador', 'consignatario', 'notify', 'descricao'} & sem_erro)
        self.assertEqual(tn.CONFIGURACAO['rankingRules'][-1], 'referencia:desc')     # desempate: os mais recentes
        self.assertEqual(tn.CAMPOS_BUSCA[:4], ['embarcador', 'consignatario', 'notify', 'descricao'])


class TestBusca(unittest.TestCase):

    def test_corpo_com_periodo_fim_inclusive(self):
        corpo = tn.corpo_da_busca('acme', date(2026, 10, 1), date(2026, 10, 31), limite=20, deslocamento=40)
        self.assertEqual(corpo['filter'], 'referencia >= 1790812800 AND referencia < 1793491200')   # 01/10 e 01/11
        self.assertEqual((corpo['q'], corpo['limit'], corpo['offset']), ('acme', 20, 40))
        self.assertEqual((corpo['highlightPreTag'], corpo['highlightPostTag']), (tn.MARCA_INICIO, tn.MARCA_FIM))
        self.assertTrue(corpo['showMatchesPosition'])
        self.assertNotIn('filter', tn.corpo_da_busca('acme'))
        self.assertEqual(tn.corpo_da_busca('acme', inicio=date(2026, 10, 1))['filter'], 'referencia >= 1790812800')

    def test_resultado_com_trechos_na_ordem_dos_campos(self):
        resposta = {'estimatedTotalHits': 2, 'hits': [
            {'ce': '1', 'referencia': 1790812800,
             '_matchesPosition': {'descricao': [{}], 'embarcador': [{}]},
             '_formatted': {'ce': '1', 'embarcador': '\x02ACME\x03 CO', 'descricao': '…pecas da \x02acme\x03…',
                            'notify': ''}},
            {'ce': '2', 'referencia': None, '_matchesPosition': {'notify': [{}]}, '_formatted': {'notify': ''}}]}
        resultado = tn.le_resultado(resposta)
        self.assertEqual(resultado.total, 2)
        primeiro, segundo = resultado.achados
        self.assertEqual((primeiro.ce, primeiro.referencia), ('1', date(2026, 10, 1)))
        self.assertEqual(primeiro.trechos, [('embarcador', '\x02ACME\x03 CO'), ('descrição', '…pecas da \x02acme\x03…')])
        self.assertEqual((segundo.referencia, segundo.trechos), (None, []))             # trecho vazio não entra

    def test_busca_pelo_cliente(self):
        sessao = SessaoFalsa(Resposta(200, {'hits': [], 'estimatedTotalHits': 0}))
        resultado = tn.busca(tn.ClienteMeili(sessao=sessao), 'acme', limite=10)
        self.assertEqual(resultado, tn.Resultado(0, []))
        metodo, url, _, _, kwargs = sessao.pedidos[0]
        self.assertEqual((metodo, url, kwargs['json']['q']), ('POST', 'http://127.0.0.1:7700/indexes/navii_ces/search',
                                                              'acme'))

    def test_trecho_html_escapa_o_texto_do_ce(self):
        self.assertEqual(tn.trecho_html('<script>x</script> \x02ACME\x03 & CO'),
                         '&lt;script&gt;x&lt;/script&gt; <mark>ACME</mark> &amp; CO')


if __name__ == '__main__':
    unittest.main()
