"""Testes da completude de escaneamentos (virasana/completude), em SQLite.

Cenário (período 01 a 07/03/2026):
  TRA, recinto seco 8932000 (EADI SANTOS):
    T1 entrada 02/03, saída do seco 04/03, escaneado com imagem
    T2 entrada 03/03, saída do seco 05/03, SEM escaneamento
    T3 entrada 03/03, ainda no seco, escaneado SEM imagem
    T4 entrada 20/02 (fora do período)
  Operador (concluídos sem entrada no seco):
    O1 BTP 8931356, saída 02/03, escaneado com imagem
    O2 BTP, saída 06/03, SEM escaneamento
    O3 BTP, saída 06/03, NÃO concluído (o loop ainda espera a entrada no seco) -> fora
    O4 8939999 (sem cadastro), saída 04/03, SEM escaneamento
"""
import os
import unittest
from datetime import date, datetime, timedelta

from flask import Flask
from flask_login import LoginManager, UserMixin

from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos,
)
from virasana.completude import busca_completude as bc
from virasana.completude.views_completude import bp_completude
from virasana.navii.busca_navii import tabela_recintos
from virasana.tests.test_navii_busca_sqlite import cria_sessao

INICIO, FIM = datetime(2026, 3, 1), datetime(2026, 3, 7, 23, 59, 59)
AGORA = datetime(2026, 3, 10)
SECO, BTP, SEM_CADASTRO = '8932000', '8931356', '8939999'


def ciclo(session, conteiner, imagens=None, **campos):
    """imagens: None = sem escaneamento; lista de imagem_id (None = escaneado sem imagem)."""
    c = CicloDeVidaImportacao(numeroConteiner=conteiner, numeroCEmercante='CE' + conteiner, **campos)
    for i, imagem in enumerate(imagens or []):
        c.escaneamentos.append(CicloDeVidaImportacaoEscaneamentos(
            escaneamento_id=hash(conteiner) % 10000 + i, data_escaneamento=datetime(2026, 3, 2, 12), imagem_id=imagem))
    session.add(c)


def monta_cenario(session):
    session.execute(tabela_recintos.insert(), [
        {'id': 1, 'nome': 'BTP', 'cod_siscomex': BTP, 'cod_carga': 'T01'},
        {'id': 2, 'nome': 'EADI SANTOS', 'cod_siscomex': SECO, 'cod_carga': None},
    ])
    tra = dict(codigoRecintoDesembarque=BTP, codigoRecintoArmazenagem=SECO, data_desembarque=datetime(2026, 3, 1),
               data_saida=datetime(2026, 3, 1, 20))
    ciclo(session, 'T1', ['5f00000000000000000000a1'], data_entrada_seco=datetime(2026, 3, 2),
          data_saida_seco=datetime(2026, 3, 4), concluido=True, **tra)
    ciclo(session, 'T2', None, data_entrada_seco=datetime(2026, 3, 3), data_saida_seco=datetime(2026, 3, 5),
          concluido=True, **tra)
    ciclo(session, 'T3', [None], data_entrada_seco=datetime(2026, 3, 3), **tra)
    ciclo(session, 'T4', None, data_entrada_seco=datetime(2026, 2, 20), **tra)
    operador = dict(codigoRecintoDesembarque=BTP, data_desembarque=datetime(2026, 3, 1))
    ciclo(session, 'O1', ['5f00000000000000000000b1'], data_saida=datetime(2026, 3, 2), concluido=True, **operador)
    ciclo(session, 'O2', None, data_saida=datetime(2026, 3, 6), concluido=True, **operador)
    ciclo(session, 'O3', None, data_saida=datetime(2026, 3, 6), concluido=False, **operador)
    ciclo(session, 'O4', None, codigoRecintoDesembarque=SEM_CADASTRO, data_desembarque=datetime(2026, 3, 3),
          data_saida=datetime(2026, 3, 4), concluido=True)
    session.commit()


class CompletudeTestCase(unittest.TestCase):
    def setUp(self):
        self.session = cria_sessao()
        monta_cenario(self.session)

    def tearDown(self):
        self.session.close()


class TestBusca(CompletudeTestCase):

    def test_tra_por_recinto_seco(self):
        [seco] = bc.agregado(self.session, 'tra', INICIO, FIM, AGORA)
        self.assertEqual(seco['codigo_recinto'], SECO)
        self.assertEqual(seco['nome_recinto'], 'EADI SANTOS')
        self.assertEqual((seco['total'], seco['sem_escaneamento'], seco['sem_imagem']), (3, 1, 2))
        self.assertEqual((seco['total_saidas'], seco['saidas_sem_escaneamento']), (2, 1))
        self.assertAlmostEqual(seco['pct_sem_escaneamento'], 1 / 3)

    def test_operador_so_concluidos_sem_entrada_no_seco(self):
        linhas = {r['codigo_recinto']: r for r in bc.agregado(self.session, 'operador', INICIO, FIM, AGORA)}
        self.assertEqual(set(linhas), {BTP, SEM_CADASTRO})   # T1/T2 (concluídos via seco) não entram
        self.assertEqual((linhas[BTP]['total'], linhas[BTP]['sem_escaneamento']), (2, 1))   # O3 fora
        self.assertEqual(linhas[SEM_CADASTRO]['nome_recinto'], '(recinto não cadastrado)')
        self.assertEqual(linhas[SEM_CADASTRO]['pct_sem_escaneamento'], 1.0)
        # ordem: maior % sem escaneamento primeiro
        self.assertEqual([r['codigo_recinto'] for r in bc.agregado(self.session, 'operador', INICIO, FIM, AGORA)],
                         [SEM_CADASTRO, BTP])

    def test_tra_ignora_entradas_recentes(self):
        recente = AGORA - timedelta(hours=1)
        ciclo(self.session, 'T5', None, codigoRecintoArmazenagem=SECO, data_entrada_seco=recente)
        self.session.commit()
        [seco] = bc.agregado(self.session, 'tra', INICIO, AGORA, AGORA)
        self.assertEqual(seco['total'], 3)

    def test_panorama_e_totais(self):
        p = bc.panorama(self.session, INICIO, FIM, AGORA)
        self.assertEqual(list(p), ['tra', 'operador'])
        self.assertEqual(p['tra']['totais']['total'], 3)
        self.assertEqual(p['operador']['totais']['total'], 3)
        self.assertEqual(p['operador']['totais']['sem_escaneamento'], 2)
        self.assertAlmostEqual(p['tra']['participacao'], 0.5)

    def test_detalhe(self):
        linhas = bc.detalhe(self.session, 'tra', SECO, INICIO, FIM, AGORA)
        self.assertEqual([lin['conteiner'] for lin in linhas], ['T2', 'T1', 'T3'])   # sem escaneamento primeiro
        t1 = next(lin for lin in linhas if lin['conteiner'] == 'T1')
        self.assertEqual(t1['imagens'], ['5f00000000000000000000a1'])
        self.assertEqual(len(bc.detalhe(self.session, 'operador', BTP, INICIO, FIM, AGORA)), 2)

    def test_fluxo_invalido(self):
        with self.assertRaises(ValueError):
            bc.agregado(self.session, 'xpto', INICIO, FIM)

    def test_vazio(self):
        p = bc.panorama(self.session, datetime(2030, 1, 1), datetime(2030, 1, 2), AGORA)
        self.assertEqual(p['tra']['linhas'], [])
        self.assertEqual(p['tra']['participacao'], 0)


class Usuario(UserMixin):
    id = 'teste'


class TestTela(CompletudeTestCase):

    def setUp(self):
        super().setUp()
        pasta_templates = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        app = Flask(__name__, template_folder=pasta_templates)
        app.config.update(LOGIN_DISABLED=True, TESTING=True, db_session=self.session)
        LoginManager(app).request_loader(lambda req: Usuario())
        app.register_blueprint(bp_completude)
        self.client = app.test_client()

    def html(self, caminho, **params):
        resposta = self.client.get(caminho, query_string=params)
        self.assertEqual(resposta.status_code, 200, resposta.get_data(as_text=True)[:300])
        return resposta.get_data(as_text=True)

    def test_tela_com_os_dois_fluxos(self):
        html = self.html('/completude_escaneamentos/', inicio='2026-03-01', fim='2026-03-07')
        for trecho in ('Recintos secos (TRA)', 'Desembaraço no operador', 'EADI SANTOS', 'BTP',
                       '(recinto não cadastrado)', 'TOTAL', '50%'):
            self.assertIn(trecho, html)
        self.assertIn('/completude_escaneamentos/recinto/8932000?fluxo=tra', html)
        self.assertIn('/completude_escaneamentos/recinto/8931356?fluxo=operador', html)
        self.assertEqual(self.client.get('/completude_escaneamentos').status_code, 200)   # sem a barra

    def test_periodo_padrao_e_invalido(self):
        self.assertIn(str(date.today()), self.html('/completude_escaneamentos/'))
        self.assertIn('Período máximo', self.html('/completude_escaneamentos/', inicio='2026-01-01',
                                                  fim='2026-12-31'))
        self.assertIn('Data inválida', self.html('/completude_escaneamentos/', inicio='x'))

    def test_detalhe(self):
        html = self.html(f'/completude_escaneamentos/recinto/{SECO}', fluxo='tra', inicio='2026-03-01',
                         fim='2026-03-07')
        for trecho in ('EADI SANTOS', 'T1', 'T2', 'sem escaneamento', 'sem imagem', 'Entrada no seco',
                       '/mini1/5f00000000000000000000a1', '/file?_id=5f00000000000000000000a1'):
            self.assertIn(trecho, html)
        html = self.html(f'/completude_escaneamentos/recinto/{BTP}', fluxo='operador', inicio='2026-03-01',
                         fim='2026-03-07')
        self.assertIn('O1', html)
        self.assertNotIn('Entrada no seco', html)
        self.assertEqual(self.client.get(f'/completude_escaneamentos/recinto/{BTP}?fluxo=xpto').status_code, 404)
        self.assertEqual(self.client.get(f'/completude_escaneamentos/recinto/{BTP}?inicio=x').status_code, 400)


if __name__ == '__main__':
    unittest.main()
