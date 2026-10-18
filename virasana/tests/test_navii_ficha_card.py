"""Testes do "Abrir Ficha" pelo card do NAVI-I (navii/ficha_navii.py + /navii/api/ficha), com as tabelas REAIS do Fichas
(models do bhadrasana2) no SQLite e os dois Mongos simulados: o do virasana (imagens de escaneamento) e o do Fichas
(`MONGODB_RISCO`, para onde as imagens são copiadas). A regra de abertura em si é testada na suíte do Fichas
(bhadrasana2/tests/test_navii_ficha.py); aqui, a montagem pelo card, o endpoint e os casos de erro.
"""
import logging
import unittest
from datetime import datetime
from unittest import mock

import mongomock
from bson import ObjectId
from flask import Flask
from flask_login import LoginManager, UserMixin
from gridfs import GridFS
from mongomock.gridfs import enable_gridfs_integration
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from virasana.navii import busca_navii, coleta_uso, dta_navii, ficha_navii, fichas_ovr, tabelas_opcionais
from virasana.navii.busca_navii import tabela_recintos
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import TABELAS, cria_ce, cria_ciclo, monta_cenario

from bhadrasana.models import Setor, Usuario  # noqa: I100
from bhadrasana.models import navii_ficha as regra
from bhadrasana.models.ovr import EventoOVR, OVR, TipoEventoOVR, create_tiposevento
from bhadrasana.models.rvf import ImagemRVF, RVF, metadata as metadata_fichas

enable_gridfs_integration()
ANALISTA = '11111111111'
IMAGEM_1, IMAGEM_2 = '5f0000000000000000000071', '5f0000000000000000000072'
CE_A = '152600000000071'   # o filtro de CE guarda só dígitos (CE real)


def cria_sessao_com_fichas():
    """Tabelas do Fichas (completas, pelos models) + as do NAVI-I (as que já existem ficam)."""
    engine = create_engine('sqlite://')
    metadata_fichas.create_all(engine)
    for modelo in TABELAS:
        modelo.__table__.create(engine, checkfirst=True)
    for tabela in (tabela_recintos, dta_navii.DTAS, fichas_ovr.tabela_ovrs, fichas_ovr.tabela_rvfs):
        tabela.create(engine, checkfirst=True)
    session = sessionmaker(bind=engine)()
    monta_cenario(session)
    cria_ce(session, CE_A, 'M1', {'C70': [('84713000', 'Notebook')], 'C71': []}, consignatario='11222333000181')
    cria_ciclo(session, CE_A, 'C70', datetime(2026, 1, 11, 8, 0), '8931356', imagens=[IMAGEM_1, IMAGEM_2])
    create_tiposevento(session)
    for nome, fase in (('Bloqueio de CE incluído', 1), ('Verificação Física Agendada', 2)):
        tipo = TipoEventoOVR()
        tipo.nome, tipo.fase = nome, fase
        session.add(tipo)
    setor = Setor()
    setor.id, setor.nome = '2', 'EQRIMP'
    usuario = Usuario()
    usuario.cpf, usuario.nome, usuario.setor_id = ANALISTA, 'Analista', '2'
    session.add_all([setor, usuario])
    session.commit()
    return session


class Usuario_(UserMixin):
    def __init__(self, id_):
        self.id = id_


class FichaCardTestCase(unittest.TestCase):

    def setUp(self):
        tabelas_opcionais.limpa_cache()
        self.session = cria_sessao_com_fichas()
        self.mongo_virasana = mongomock.MongoClient()['virasana']
        fs = GridFS(self.mongo_virasana)
        fs.put(b'imagem 1', _id=ObjectId(IMAGEM_1), filename='scan1.jpg')
        fs.put(b'imagem 2', _id=ObjectId(IMAGEM_2), filename='scan2.jpg')
        self.mongo_fichas = mongomock.MongoClient()['risco']
        self.app = Flask(__name__)
        self.app.config.update(LOGIN_DISABLED=False, TESTING=True, db_session=self.session,
                               mongodb=self.mongo_virasana, mongodb_fichas=self.mongo_fichas, SECRET_KEY='x')
        gerente = LoginManager(self.app)
        gerente.request_loader(lambda req: Usuario_(req.headers['X-Usuario']) if req.headers.get('X-Usuario') else None)
        self.app.register_blueprint(bp_navii)
        self.client = self.app.test_client()

    def tearDown(self):
        self.session.close()

    def previa(self, ce=CE_A, conteiner='C70', usuario=ANALISTA):
        return self.client.get('/navii/api/ficha', query_string={'ce': ce, 'conteiner': conteiner},
                               headers={'X-Usuario': usuario})

    def abre(self, ce=CE_A, conteiner='C70', usuario=ANALISTA, **campos):
        return self.client.post('/navii/api/ficha', json={'ce': ce, 'conteiner': conteiner, **campos},
                                headers={'X-Usuario': usuario})


class TestApiFicha(FichaCardTestCase):

    def test_previa_monta_tudo_pelo_card(self):
        dados = self.previa().get_json()
        self.assertEqual((dados['acao'], dados['impedimento'], dados['ce'], dados['conteiner']),
                         ('nova', None, CE_A, 'C70'))
        self.assertEqual(dados['opcoes'], {'bloqueio': True, 'agendada': True})
        self.assertTrue(dados['resumo'].startswith(f'[NAVI-I] Contêiner C70, CE {CE_A}.'))
        self.assertLessEqual(len(dados['resumo']), dados['limiteResumo'])
        self.assertIn('NCM 84713000', dados['descricao'])
        self.assertIn('Desembarque: 11/01/2026', dados['descricao'])
        self.assertEqual(dados['imagens'], 2)

    def test_ficha_fica_registrada_na_pesquisa_de_onde_veio(self):
        col = coleta_uso.colecao(self.mongo_virasana)
        pesquisa = coleta_uso.registra_pesquisa(col, ANALISTA, '2', 'aba-0001-teste', None, 'tela', 'efetiva',
                                                ('2026-01-01', '2026-01-31'), {}, 1, {}, [])
        dados = self.abre(pesquisa=pesquisa).get_json()
        [acao] = col.find_one({})['acoes']
        self.assertEqual({k: acao[k] for k in ('t', 'ficha', 'resultado', 'ce', 'c')},
                         {'t': 'ficha', 'ficha': dados['ovr_id'], 'resultado': 'nova', 'ce': CE_A, 'c': 'C70'})
        self.assertEqual(self.abre(conteiner='C71', pesquisa='lixo').status_code, 200)   # sem pesquisa válida: abre

    def test_abre_e_clique_duplo(self):
        resposta = self.abre(resumo='  meu resumo editado ', bloqueio=True)
        self.assertEqual(resposta.status_code, 200, resposta.get_data(as_text=True))
        dados = resposta.get_json()
        self.assertEqual((dados['acao'], dados['imagens'], dados['url']),
                         ('nova', 2, f'/bhadrasana2/ovr?id={dados["ovr_id"]}'))
        self.assertEqual(dados['eventos'], ['Atribuição de responsável', 'Inspeção não invasiva',
                                            'Bloqueio de CE incluído'])
        ovr = self.session.get(OVR, dados['ovr_id'])
        self.assertEqual((ovr.numeroCEmercante, ovr.tipooperacao, ovr.observacoes, ovr.cnpj_fiscalizado),
                         (CE_A, 1, '[NAVI-I] meu resumo editado', '11222333000181'))
        self.assertEqual(ovr.recinto.cod_siscomex, '8931356')
        rvf = self.session.get(RVF, dados['rvf_id'])
        self.assertIn('Contêiner: C70', rvf.descricao)
        self.assertIn('Memória do risco', rvf.descricao_interna)
        copias = [f.read() for f in GridFS(self.mongo_fichas).find({})]
        self.assertEqual(sorted(copias), [b'imagem 1', b'imagem 2'])   # copiadas para o Mongo do Fichas
        self.assertEqual(len(list(GridFS(self.mongo_virasana).find({}))), 2)   # o do virasana fica como está
        de_novo = self.abre().get_json()
        self.assertEqual((de_novo['acao'], de_novo['ovr_id']), ('ja_existia', dados['ovr_id']))
        self.assertEqual(self.previa().get_json()['acao'], 'ja_existe')

    def test_outro_conteiner_do_ce_entra_na_mesma_ficha(self):
        primeira = self.abre().get_json()
        previa = self.previa(CE_A, 'C71').get_json()
        self.assertEqual((previa['acao'], previa['ficha']['id'], previa['ficha']['url']),
                         ('acrescentar', primeira['ovr_id'], f'/bhadrasana2/ovr?id={primeira["ovr_id"]}'))
        segunda = self.abre(CE_A, 'C71').get_json()
        self.assertEqual((segunda['acao'], segunda['ovr_id'], segunda['imagens']),
                         ('acrescentada', primeira['ovr_id'], 0))   # C71 sem imagem: Ficha sai sem imagem

    def test_usuario_sem_cadastro_no_fichas(self):
        self.assertIn('não está habilitado', self.previa(usuario='99999999999').get_json()['impedimento'])
        resposta = self.abre(usuario='99999999999')
        self.assertEqual(resposta.status_code, 400)
        self.assertIn('inválido', resposta.get_json()['erro'])
        self.assertEqual(self.session.query(OVR).count(), 0)

    def test_sem_mongodb_risco(self):
        self.app.config['mongodb_fichas'] = None
        with mock.patch.dict('os.environ', {}, clear=False):
            import os
            os.environ.pop('MONGODB_RISCO', None)
            self.assertIn('MONGODB_RISCO', self.previa().get_json()['impedimento'])
            self.assertEqual(self.abre().status_code, 503)
        self.assertEqual(self.session.query(OVR).count(), 0)

    def test_conteiner_inexistente_e_usuario_anonimo(self):
        self.assertEqual(self.previa(conteiner='NAOEXISTE').status_code, 404)
        self.assertEqual(self.abre(ce='', conteiner='C70').status_code, 404)
        self.assertEqual(self.client.get(f'/navii/api/ficha?ce={CE_A}&conteiner=C70').status_code, 401)

    def test_modulo_do_fichas_que_nao_carrega_nao_derruba_o_navii(self):
        with mock.patch.object(ficha_navii, 'regra_do_fichas', side_effect=ImportError('quebrou')), \
                self.assertLogs(level=logging.ERROR):
            self.assertEqual(self.abre().status_code, 503)
        self.assertEqual(self.client.get('/navii/api/ces', query_string={'data_inicio': '2026-01-05',
                                                                         'data_fim': '2026-01-15'},
                                         headers={'X-Usuario': ANALISTA}).status_code, 200)

    def test_log_de_uso(self):
        with self.assertLogs('virasana.navii.uso', logging.INFO) as log:
            self.abre()
        self.assertIn('acao=ficha', log.output[0])
        self.assertIn('ficha=', log.output[0])


class TestTextosDoCard(unittest.TestCase):

    def card(self, **campos):
        base = {'numeroConteiner': 'ABCU1234567', 'numeroCEmercante': '152600000000001',
                'risco': {'rotuloFaixa': 'Alto', 'pontos': 12.4, 'versao': 3, 'calculadoEm': None, 'parcelas': [
                    {'grupo': 'Pessoas', 'dimensao': 'Importador', 'valor': '11222333', 'descricao': 'ALFA',
                     'peso': 5, 'fichas': 3, 'pct': 0.01, 'vezes': 40.0,
                     'fichas_links': [{'id': 10}, {'id': 11}]}]},
                'corad': {'rotulo': 'Alto'}, 'estagio': {'rotulo': 'No operador'},
                'linhaDoTempo': {'desembarque': {'data': None, 'recinto': '8931356'},
                                 'entradaRecintoSeco': {'data': None, 'recinto': '8932000'}},
                'conteineres': [{'codigoConteiner': 'ABCU1234567', 'lacre': 'L1', 'pesoBruto': 1000.0,
                                 'tipoConteiner': {'codigo': '45G1', 'descricao': "40' high cube"},
                                 'ncms': [{'identificacaoNCM': '84713000', 'descricaoPosicao': 'Máquinas',
                                           'descritivo': 'Notebook'}]}]}
        return {**base, **campos}

    def test_resumo(self):
        resumo = ficha_navii.resumo_padrao(self.card())
        self.assertEqual(resumo, '[NAVI-I] Contêiner ABCU1234567, CE 152600000000001. Risco histórico ALTO (12 pts):'
                                 ' Importador 11222333. CORAD Alto. Situação: No operador.')
        sem_desembarque = ficha_navii.resumo_padrao(self.card(semDesembarque=True, risco=None,
                                                             previsaoAtracacao={'data': '2026-10-20'}))
        self.assertIn('Risco histórico: não calculado.', sem_desembarque)
        self.assertIn('Ainda sem desembarque (atracação prevista 20/10/2026).', sem_desembarque)
        longo = self.card(risco={'rotuloFaixa': 'Alto', 'pontos': 1, 'parcelas': [
            {'dimensao': 'X' * 300, 'valor': 'Y' * 300}] * 3})
        self.assertEqual(len(ficha_navii.resumo_padrao(longo)), ficha_navii.TAMANHO_RESUMO)

    def test_descricao_memoria_e_recinto(self):
        card = self.card()
        descricao = ficha_navii.descricao_carga(card)
        self.assertIn("Contêiner: ABCU1234567 (tipo 45G1 — 40' high cube); lacre L1; peso bruto 1000 kg", descricao)
        self.assertIn('NCM 84713000 — Máquinas (declarado: Notebook)', descricao)
        memoria = ficha_navii.memoria_risco(card)
        self.assertIn('- Pessoas / Importador: 11222333 ALFA — peso 5; 3 ficha(s)', memoria)
        self.assertIn('Fichas 10, 11', memoria)
        self.assertEqual(ficha_navii.recinto_atual(card), '8932000')          # entrou no seco
        self.assertEqual(ficha_navii.recinto_atual(self.card(linhaDoTempo={'desembarque': {'recinto': 'OP'}})), 'OP')
        self.assertIsNone(ficha_navii.recinto_atual(self.card(linhaDoTempo=None)))

    def test_imagens_ausentes_sao_ignoradas(self):
        mongo = mongomock.MongoClient()['v']
        GridFS(mongo).put(b'ok', _id=ObjectId(IMAGEM_1), filename='a.jpg')
        card = self.card(imagens_ids=[IMAGEM_1, IMAGEM_2, 'nao-e-objectid'])
        self.assertEqual(ficha_navii.imagens_do_card(mongo, card), [(b'ok', 'a.jpg')])
        self.assertEqual(ficha_navii.imagens_do_card(None, card), [])

    def test_le_no_maximo_max_imagens(self):
        mongo = mongomock.MongoClient()['v']
        ids = [str(GridFS(mongo).put(bytes([i]), filename=f'{i}.jpg')) for i in range(6)]
        self.assertEqual(len(ficha_navii.imagens_do_card(mongo, self.card(imagens_ids=ids))), ficha_navii.MAX_IMAGENS)

    def test_card_do_conteiner_certo(self):
        """Se a busca trouxer mais de um card do CE, vale o do contêiner pedido (normalizado)."""
        outro, certo = {'numeroConteiner': 'MSCU7654321'}, {'numeroConteiner': 'ABCU1234567'}
        with mock.patch.object(ficha_navii, 'busca_ces_navii', return_value=[]), \
                mock.patch.object(ficha_navii, 'monta_cards_navii', return_value=[outro, certo]):
            self.assertIs(ficha_navii.card_do_conteiner(None, '152600000000001', 'abcu 123456-7'), certo)
            self.assertIsNone(ficha_navii.card_do_conteiner(None, '152600000000001', 'TGHU0000000'))

    def test_mesmo_limite_nos_dois_repositorios(self):
        self.assertEqual(ficha_navii.TAMANHO_RESUMO, regra.TAMANHO_RESUMO)
        self.assertEqual(ficha_navii.MAX_IMAGENS, regra.MAX_IMAGENS)


if __name__ == '__main__':
    unittest.main()
