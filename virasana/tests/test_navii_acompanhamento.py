"""Testes ADVERSARIAIS da aba "Fichas em acompanhamento" (navii/acompanhamento_fichas.py + /navii/fichas e /navii/api/fichas),
com as tabelas REAIS do Fichas no SQLite (as do teste do "Abrir Ficha").

Setores: 1 DIREP (pai) > 2 EQRIMP; 9 OUTRO; 7 e 8 apontam um para o outro (ciclo no cadastro: não pode travar).
Usuários: ANALISTA (setor 2), CHEFE (setor 1), OUTRO (setor 9), CICLO (setor 7).
Fichas (agora = 17/10/2026):
  F1 repressão, setor 2, NAVI-I, RVF do contêiner 'c70' (minúsculas) — o contêiner já foi ENTREGUE: ALERTA; bloqueio de CE
  F2 repressão, setor 2, sem RVF: os contêineres vêm do CE (C80 no seco; C81 sem desembarque, previsão vencida); agendada
  F3 repressão, setor 2, COM verificação física informada: fora (entra com `verificadas`)
  F4 repressão, setor 2, verificação física EXCLUÍDA: continua na lista
  F5 repressão arquivada (fase 4) e F6 FMA (tipo 0), setor 2: fora
  F7 repressão, setor 9, com o ANALISTA como responsável: entra para ele (e em "só as minhas")
  F8 repressão, setor 9, sem o ANALISTA: fora para ele
  F9 repressão, setor 1 (do chefe): fora para o ANALISTA, dentro para o CHEFE
"""
import os
import unittest
from datetime import datetime, timedelta
from unittest import mock

from flask import Flask
from flask_login import LoginManager, UserMixin

from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos,
)
from virasana.navii import acompanhamento_fichas as af
from virasana.navii import tabelas_opcionais
from virasana.navii.views_navii import bp_navii
from virasana.tests.test_navii_busca_sqlite import cria_ce, cria_ciclo
from virasana.tests.test_navii_ficha_card import ANALISTA, CE_A, IMAGEM_2, cria_sessao_com_fichas

from bhadrasana.models import Setor, Usuario  # noqa: I100
from bhadrasana.models.ovr import EventoOVR, Flag, OVR, TipoEventoOVR
from bhadrasana.models.rvf import RVF, metadata as metadata_fichas

AGORA = datetime(2026, 10, 17, 12)
CE_B = '152600000000081'
CHEFE, OUTRO, CICLO, ESTRANHO = '22222222222', '33333333333', '44444444444', '99999999999'


def novo(classe, **campos):
    """Os models rastreáveis do Fichas descartam os argumentos do construtor: campo a campo."""
    objeto = classe()
    for nome, valor in campos.items():
        setattr(objeto, nome, valor)
    return objeto


class AcompanhamentoTestCase(unittest.TestCase):

    def setUp(self):
        tabelas_opcionais.limpa_cache()
        s = self.session = cria_sessao_com_fichas()
        s.query(Setor).filter(Setor.id == '2').update({'pai_id': '1'})
        s.add_all([novo(Setor, id='1', nome='DIREP'), novo(Setor, id='9', nome='OUTRO'),
                   novo(Setor, id='7', nome='SETE', pai_id='8'), novo(Setor, id='8', nome='OITO', pai_id='7')])
        for cpf, nome, setor in ((CHEFE, 'Chefe', '1'), (OUTRO, 'Outro', '9'), (CICLO, 'Ciclo', '7')):
            s.add(novo(Usuario, cpf=cpf, nome=nome, setor_id=setor))
        # C70 (do teste do "Abrir Ficha") foi entregue; C80 está no seco; C81 sem ciclo
        ciclo = s.query(CicloDeVidaImportacao).filter(CicloDeVidaImportacao.numeroConteiner == 'C70').one()
        ciclo.situacao, ciclo.fluxo, ciclo.concluido, ciclo.data_saida = 'concluido', 'operador', True, datetime(2026, 1, 14)
        cria_ce(s, CE_B, 'M1', {'C80': [], 'C81': []})
        seco = cria_ciclo(s, CE_B, 'C80', datetime(2026, 1, 12, 8), '8931356', recinto_armazenagem='8932000')
        seco.situacao, seco.data_saida, seco.data_entrada_seco = 'aberto', datetime(2026, 1, 13), datetime(2026, 1, 13, 5)
        tipos = {t.nome: t.id for t in s.query(TipoEventoOVR)}
        especial = {t.eventoespecial: t.id for t in s.query(TipoEventoOVR) if t.eventoespecial}
        flag = novo(Flag, nome='NAVI-I')
        s.add(flag)
        fichas = [
            (1, dict(setor_id='2', numeroCEmercante=CE_A, user_name=ANALISTA, responsavel_cpf=ANALISTA)),
            (2, dict(setor_id='2', numeroCEmercante=CE_B, user_name=CHEFE, responsavel_cpf=CHEFE)),
            (3, dict(setor_id='2', numeroCEmercante=CE_B, user_name=CHEFE)),
            (4, dict(setor_id='2', numeroCEmercante=CE_B, user_name=CHEFE)),
            (5, dict(setor_id='2', numeroCEmercante=CE_B, fase=4)),
            (6, dict(setor_id='2', numeroCEmercante=CE_B, tipooperacao=0)),
            (7, dict(setor_id='9', user_name=OUTRO, responsavel_cpf=ANALISTA)),
            (8, dict(setor_id='9', user_name=OUTRO)),
            (9, dict(setor_id='1', user_name=CHEFE)),
        ]
        for id_, campos in fichas:
            s.add(novo(OVR, id=id_, tipooperacao=campos.pop('tipooperacao', 1), fase=campos.pop('fase', 1),
                       datahora=datetime(2026, 10, id_), **campos))
        s.flush()
        ficha_navii = s.get(OVR, 1)   # referência guardada até o commit (o identity map é fraco)
        ficha_navii.flags.append(flag)
        s.add(novo(RVF, ovr_id=1, numerolote='c70', numeroCEmercante=CE_A))
        eventos = [
            (1, especial[1], f'De: Nenhum Para: {ANALISTA}', 1), (1, especial[7], 'RVF 1', 2),
            (1, tipos['Bloqueio de CE incluído'], f'por {ESTRANHO}', 3),
            (2, especial[1], f'De: Nenhum Para: {CHEFE}', 1), (2, tipos['Verificação Física Agendada'], '', 4),
            (3, especial[2], 'RVF 9', 5),
            (4, especial[2], 'lançado por engano', 5, True),
        ]
        for ovr_id, tipo, motivo, dia, *excluido in eventos:
            s.add(novo(EventoOVR, ovr_id=ovr_id, tipoevento_id=tipo, motivo=motivo,
                       create_date=datetime(2026, 10, 10 + dia), excluido=bool(excluido)))
        s.commit()

    def tearDown(self):
        self.session.close()

    def lista(self, cpf=ANALISTA, **opcoes):
        return af.fichas_em_acompanhamento(self.session, cpf, agora=AGORA, **opcoes)

    def ids(self, cpf=ANALISTA, **opcoes):
        return [f['id'] for f in self.lista(cpf, **opcoes)['fichas']]


class TestQuaisFichas(AcompanhamentoTestCase):

    def test_padrao_do_analista(self):
        self.assertEqual(self.ids(), [1, 7, 4, 2])                 # F1 (alerta) primeiro; depois as mais novas

    def test_opcoes(self):
        self.assertEqual(self.ids(minhas=True), [1, 7])
        self.assertEqual(self.ids(navii=True), [1])
        self.assertEqual(self.ids(verificadas=True), [1, 7, 4, 3, 2])

    def test_setores_subordinados_e_ciclo_no_cadastro(self):
        self.assertEqual(self.ids(CHEFE), [1, 9, 4, 2])             # setor 1 e o subordinado 2
        self.assertEqual(self.ids(OUTRO), [8, 7])
        self.assertEqual(self.ids(CICLO), [])                      # 7 <-> 8: termina
        self.assertEqual(af.descendentes({'7': '8', '8': '7'}, '7'), {'7', '8'})

    def test_usuario_sem_cadastro_no_fichas(self):
        dados = self.lista(ESTRANHO)
        self.assertEqual((dados['fichas'], dados['usuario']), ([], None))
        self.assertIn('cadastro no Fichas', dados['aviso'])


class TestCard(AcompanhamentoTestCase):

    def card(self, id_, **opcoes):
        return next(f for f in self.lista(**opcoes)['fichas'] if f['id'] == id_)

    def test_alerta_saiu_sem_verificacao(self):
        f1 = self.card(1)
        self.assertTrue(f1['alerta'] and f1['navii'])
        self.assertEqual(self.lista()['alertas'], 1)
        [c70] = f1['conteineres']
        self.assertEqual((c70['numero'], c70['estagio']['codigo'], c70['alerta']), ('C70', 'entregue', True))
        self.assertEqual(c70['onde'], {'codigo': '8931356', 'nome': 'BTP'})
        self.assertEqual(c70['escaneamento']['imagem'], IMAGEM_2)
        self.assertEqual((f1['status'], f1['bloqueio'][:10], f1['abriu']), ('Bloqueio de CE incluído', '2026-10-13',
                                                                          'Analista'))

    def test_conteineres_do_ce_sem_rvf(self):
        f2 = self.card(2)
        c80, c81 = f2['conteineres']
        self.assertEqual((c80['numero'], c80['estagio']['codigo'], c80['onde']['codigo']), ('C80', 'no_seco', '8932000'))
        self.assertEqual((c81['numero'], c81['semDesembarque'], c81['previsao']['situacao']), ('C81', True, 'vencida'))
        self.assertIsNone(c80['previsao'])                         # já desembarcou: a previsão não importa mais
        self.assertEqual((c80['desembarque'][:10], c80['saida']), ('2026-01-12', None))   # saiu do porto para o
        self.assertEqual(self.card(1)['conteineres'][0]['saida'][:10], '2026-01-14')    # seco: não é saída
        self.assertFalse(f2['alerta'])
        self.assertEqual((f2['agendada'][:10], f2['bloqueio']), ('2026-10-14', None))

    def test_verificacao_excluida_e_verificada_sem_alerta(self):
        self.assertIsNone(self.card(4)['verificacao'])
        f3 = self.card(3, verificadas=True)
        self.assertEqual(f3['verificacao'][:10], '2026-10-15')
        self.assertTrue(all(not c['alerta'] for c in f3['conteineres']))

    def test_cpf_no_motivo_vira_nome_ou_mascara(self):
        eventos = self.card(1)['eventos']
        self.assertEqual([e['tipo'] for e in eventos][0], 'Bloqueio de CE incluído')   # mais novo primeiro
        motivos = ' '.join(e['motivo'] for e in eventos)
        self.assertIn('Para: Analista', motivos)
        self.assertIn('999.***.***-99', motivos)
        self.assertNotIn(ESTRANHO, motivos)


class TestCasosLimite(AcompanhamentoTestCase):
    """Os casos que os mutantes mostraram descobertos: cada um derruba uma regra diferente."""

    def card(self, id_, **opcoes):
        return next(f for f in self.lista(**opcoes)['fichas'] if f['id'] == id_)

    def muda(self, ovr_id, **campos):
        self.session.query(OVR).filter(OVR.id == ovr_id).update(campos)
        self.session.commit()

    def test_entra_por_qualquer_papel(self):
        for campo, original in (('user_name', OUTRO), ('cpfauditorresponsavel', None)):
            with self.subTest(campo=campo):
                self.muda(8, **{campo: ANALISTA})                  # F8: setor 9, só este papel é do ANALISTA
                self.assertEqual(self.ids(minhas=True), [1, 8, 7])
                self.muda(8, **{campo: original})

    def test_fase_3_ja_e_encerrada(self):
        self.muda(2, fase=3)
        self.assertNotIn(2, self.ids())
        self.muda(2, fase=0)
        self.assertIn(2, self.ids())

    def test_so_a_flag_navii_conta(self):
        outra = novo(Flag, nome='OUTRA')
        f2 = self.session.get(OVR, 2)
        f2.flags.append(outra)
        self.session.commit()
        self.assertEqual(self.ids(navii=True), [1])
        self.assertFalse(self.card(2)['navii'])

    def test_subordinados_em_qualquer_nivel(self):
        self.assertEqual(af.descendentes({'2': '1', '3': '2', '4': '3', '9': None}, '1'), {'1', '2', '3', '4'})

    def test_reembarcado_alerta_so_sem_verificacao(self):
        c80 = self.session.query(CicloDeVidaImportacao).filter(CicloDeVidaImportacao.numeroConteiner == 'C80').one()
        c80.situacao, c80.fluxo = 'concluido', 'passagem'
        self.session.commit()
        # F2 e F4 (o mesmo CE; a verificação da F4 foi EXCLUÍDA, não cala o alerta) e F1: as com alerta primeiro
        self.assertEqual(self.ids(), [4, 2, 1, 7])
        self.assertEqual(self.lista()['alertas'], 3)
        primeiro = self.card(2)['conteineres'][0]
        self.assertEqual((primeiro['estagio']['codigo'], primeiro['alerta']), ('embarcado', True))
        f3 = self.card(3, verificadas=True)                        # o mesmo CE, mas com verificação informada
        self.assertFalse(f3['alerta'] or any(c['alerta'] for c in f3['conteineres']))

    def test_ultimo_escaneamento_sem_imagem_mostra_a_ultima_imagem(self):
        c70 = self.session.query(CicloDeVidaImportacao).filter(CicloDeVidaImportacao.numeroConteiner == 'C70').one()
        c70.escaneamentos.append(CicloDeVidaImportacaoEscaneamentos(
            escaneamento_id=3, data_escaneamento=datetime(2026, 1, 12, 9), imagem_id=None))
        self.session.commit()
        escaneamento = self.card(1)['conteineres'][0]['escaneamento']
        self.assertEqual((escaneamento['data'], escaneamento['imagem']), ('2026-01-12T09:00', IMAGEM_2))

    def test_eventos_no_card_agendada_e_citados(self):
        tipos = {t.nome: t.id for t in self.session.query(TipoEventoOVR)}
        responsavel = self.session.query(TipoEventoOVR).filter(TipoEventoOVR.eventoespecial == 1).first().id
        for tipo, motivo, dia in ((tipos['Verificação Física Agendada'], 'reagendada', 6),
                                  (responsavel, f'De: {CHEFE} Para: {CICLO}', 7), (responsavel, 'nada', 8)):
            self.session.add(novo(EventoOVR, ovr_id=2, tipoevento_id=tipo, motivo=motivo,
                                  create_date=datetime(2026, 10, 10 + dia), excluido=False))
        self.session.commit()
        f2 = self.card(2)
        self.assertEqual(len(f2['eventos']), af.EVENTOS_NO_CARD)  # 5 eventos, mostra os 4 mais novos
        self.assertEqual([e['motivo'] for e in f2['eventos']][:2], ['nada', 'De: Chefe Para: Ciclo'])
        self.assertEqual(f2['agendada'][:10], '2026-10-16')        # a última agendada

    def test_pessoas_dias_setor_e_limite(self):
        self.muda(2, responsavel_cpf=ESTRANHO)
        f2 = self.card(2)
        self.assertEqual((f2['responsavel'], f2['abriu'], f2['navii']), ('999.***.***-99', 'Chefe', False))
        self.assertEqual((f2['dias'], f2['setor']), (15, 'EQRIMP'))           # aberta em 02/10, agora 17/10 12h
        self.assertEqual(self.card(7)['setor'], 'OUTRO')           # fora dos setores do usuário: o nome também
        with mock.patch.object(af, 'LIMITE_CARDS', 2):
            dados = self.lista()
        self.assertEqual(([f['id'] for f in dados['fichas']], dados['total'], dados['alertas']), ([1, 7], 4, 1))
        self.assertIn('2 primeiras de 4', dados['aviso'])
        self.assertIsNone(self.lista()['aviso'])


class TestColunas(unittest.TestCase):

    def test_tabelas_declaradas_existem_nos_models_do_fichas(self):
        for tabela in (af.OVRS, af.EVENTOS, af.TIPOS_EVENTO, af.RVFS, af.FLAGS, af.FLAGS_OVR, af.USUARIOS, af.SETORES):
            modelo = metadata_fichas.tables[tabela.name]
            self.assertTrue({c.name for c in tabela.columns} <= {c.name for c in modelo.columns}, tabela.name)


class Usuario_(UserMixin):
    def __init__(self, id_):
        self.id = id_


class TestTelaEApi(AcompanhamentoTestCase):

    def setUp(self):
        super().setUp()
        app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates'))
        app.config.update(TESTING=True, db_session=self.session, SECRET_KEY='x')
        LoginManager(app).request_loader(
            lambda req: Usuario_(req.headers['X-Usuario']) if req.headers.get('X-Usuario') else None)

        @app.route('/mini1/<_id>')
        def mini(_id):
            return ''
        app.register_blueprint(bp_navii)
        self.client = app.test_client()

    def test_api(self):
        dados = self.client.get('/navii/api/fichas', headers={'X-Usuario': ANALISTA}).get_json()
        self.assertEqual((dados['total'], dados['alertas']), (4, 1))
        dados = self.client.get('/navii/api/fichas?minhas=1&navii=1', headers={'X-Usuario': ANALISTA}).get_json()
        self.assertEqual([f['id'] for f in dados['fichas']], [1])
        dados = self.client.get('/navii/api/fichas?verificadas=true', headers={'X-Usuario': ANALISTA}).get_json()
        self.assertEqual(dados['total'], 5)
        self.assertEqual(self.client.get('/navii/api/fichas').status_code, 401)

    def test_link_do_conteiner_abre_o_card_no_navii(self):
        """O link leva contêiner, CE e a janela (±1 dia do desembarque; sem desembarque, da previsão): sem período a
        tela usaria o padrão e o contêiner antigo não apareceria. A busca do NAVI-I com esses parâmetros o encontra."""
        fichas = {f['id']: f for f in self.client.get('/navii/api/fichas', headers={'X-Usuario': ANALISTA})
                  .get_json()['fichas']}
        c70, (c80, c81) = fichas[1]['conteineres'][0], fichas[2]['conteineres']
        self.assertEqual(c70['navii'], {'conteiner': 'C70', 'ce': CE_A, 'tipo_janela': 'efetiva',
                                        'data_inicio': '2026-01-10', 'data_fim': '2026-01-12'})
        self.assertEqual((c80['navii']['tipo_janela'], c80['navii']['data_inicio']), ('efetiva', '2026-01-11'))
        prevista = datetime.strptime(c81['previsao']['data'], '%Y-%m-%d')
        self.assertEqual((c81['navii']['tipo_janela'], c81['navii']['data_fim']),
                         ('prevista', f'{prevista + timedelta(days=1):%Y-%m-%d}'))
        for conteiner in (c70, c80, c81):
            cards = self.client.get('/navii/api/ces', query_string=conteiner['navii'],
                                    headers={'X-Usuario': ANALISTA}).get_json()['cards']
            self.assertEqual([c['numeroConteiner'] for c in cards], [conteiner['numero']])

    def test_pagina(self):
        html = self.client.get('/navii/fichas', headers={'X-Usuario': ANALISTA}).get_data(as_text=True)
        self.assertIn('var API_URL = "/navii/api/fichas"', html)
        self.assertIn('aria-current="page">Fichas em acompanhamento', html)


if __name__ == '__main__':
    unittest.main()
