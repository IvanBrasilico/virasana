"""Testes do MODO SOMBRA da heurística v2 do Ciclo de Vida (sombra_ciclo_de_vida.py).

Cenário (SQLite, desembarques de março/2026, agora = 10/04/2026):
  escopo: BTP e Ecoporto (e_operador, unidade 0817800); fora: Multirio (operador do Rio) e EADI (seco)
  CTRA: desembarque na BTP declarando o MASTER; inspeção e saída declarando o FILHOTE; entrada e saída
        no EADI. Ciclo atual: CE master, concluído pelo operador (perdeu o seco) -> TRA, CE da família
  CTRB: desembarque no ECOPORTO, sem ciclo atual; CE simples + um CE de exportação (descartado)
  CTRC: saída 15 dias após o desembarque (além das 72 h do algoritmo antigo); ciclo atual pendente
  CTRE: inspeção declarando CE de OUTRA família (confiança baixa); o ciclo atual a tinha ligado
  CTRV: contêiner VAZIO no manifesto do Mercante (escala no dia), peso zero na balança, sem CE nem ciclo
  CTRR: desembarque no Rio (fora do escopo)
O CTRB também consta vazio num manifesto de dezembro/2025 (longe do desembarque: não conta).
"""
import csv
import io
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta

import mongomock
from sqlalchemy import text

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida import sombra_ciclo_de_vida as sombra
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import CEsResolvidos
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaImportacao as Ciclo, CicloDeVidaImportacaoEscaneamentos as Esc,
)
from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, ConteinerVazio, Escala, EscalaManifesto, Item, Manifesto,
)
from virasana.tests.test_navii_busca_sqlite import cria_sessao

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100

M, H1, ANTIGO = '152600000000001', '152600000000002', '152400000000005'
S, EXPORTACAO, X, Y = '152600000000010', '152600000000040', '152600000000020', '152600000000030'
BTP, ECOPORTO, EADI, RIO = '8931359', '8931318', '8932000', '8911101'
INICIO, FIM, AGORA = datetime(2026, 3, 1), datetime(2026, 4, 1), datetime(2026, 4, 10)


def d(dia, hora=8):
    return datetime(2026, 3, dia, hora)


def _mercante(session):
    def ce(numero, manifesto, conteineres, master=None, trafego='5'):
        session.add(Conhecimento(numeroCEmercante=numero, numeroCEMaster=master, manifestoCE=manifesto,
                                 tipoTrafego=trafego, tipoBLConhecimento='12' if master else '10'))
        session.add_all([Item(numeroCEmercante=numero, codigoConteiner=c) for c in conteineres])

    ce(M, 'MAN1', ['CTRA'])
    ce(H1, 'MAN1', ['CTRA'], master=M)
    ce(ANTIGO, 'MAN0', ['CTRA'])
    ce(S, 'MAN2', ['CTRB'])
    ce(EXPORTACAO, 'MAN2', ['CTRB'], trafego='7')
    ce(X, 'MAN3', ['CTRE'])
    session.add_all([Manifesto(numero=m) for m in ('MAN0', 'MAN1', 'MAN2', 'MAN3', 'MANV', 'MAN9')])
    ConteinerVazio.__table__.create(session.get_bind(), checkfirst=True)
    session.add_all([ConteinerVazio(manifesto='MANV', idConteinerVazio='CTRV'),
                     ConteinerVazio(manifesto='MAN9', idConteinerVazio='CTRB')])
    escalas = [('E1', 'MAN1', '2026-03-01', 'BRSSZ'), ('E0', 'MAN1', '2026-02-01', 'CNSHA'),
               ('E2', 'MAN2', '2026-03-03', 'BRSSZ'), ('E3', 'MAN3', '2026-03-09', 'BRSSZ'),
               ('E4', 'MANV', '2026-03-12', 'BRSSZ'), ('E9', 'MAN9', '2025-12-01', 'BRSSZ')]
    for i, (escala, manifesto, data, porto) in enumerate(escalas, start=1):
        session.add(Escala(numeroDaEscala=escala, dataPrevistaAtracacao=data, portoDaEscala=porto))
        session.add(EscalaManifesto(ID=i, manifesto=manifesto, escala=escala))


def _recintos(session):
    # ovr_recintos real tem e_operador e cod_unidade (a tabela leve do NAVI-I não declara as colunas);
    # a BTP com a unidade "numérica" (sem o zero à esquerda) testa a normalização
    session.execute(text('ALTER TABLE ovr_recintos ADD COLUMN e_operador BOOLEAN'))
    session.execute(text('ALTER TABLE ovr_recintos ADD COLUMN cod_unidade VARCHAR(7)'))
    session.execute(text("INSERT INTO ovr_recintos (id, nome, cod_siscomex, e_operador, cod_unidade) VALUES "
                         "(1, 'BTP', '8931359', 1, '817800'), (2, 'ECOPORTO SANTOS', '8931318', 1, '0817800'), "
                         "(3, 'EADI SANTOS', '8932000', 0, '0817800'), (4, 'MULTIRIO', '8911101', 1, '0717600')"))


def monta(session):
    _mercante(session)
    _recintos(session)
    ids = iter(range(1, 1000))
    eventos = {EmbarqueDesembarque: [], AcessoVeiculo: [], InspecaoNaoInvasiva: []}

    def novo(modelo, conteiner, data, recinto, ce=None, **campos):
        linha = dict(id=next(ids), numeroConteiner=conteiner, dataHoraOcorrencia=data, codigoRecinto=recinto,
                     tipoOperacao='I', numeroConhecimento=ce, **campos)
        eventos[modelo].append(linha)
        return linha['id']

    def desembarque(conteiner, data, recinto, ce=None, **pesos):
        return novo(EmbarqueDesembarque, conteiner, data, recinto, ce, embarqueDesembarque='D', **pesos)

    def acesso(conteiner, data, recinto, direcao, ce=None, operacao='C'):
        return novo(AcessoVeiculo, conteiner, data, recinto, ce, direcao=direcao, operacao=operacao,
                    vazioConteiner=False)

    def inspecao(conteiner, data, recinto, ce=None):
        return novo(InspecaoNaoInvasiva, conteiner, data, recinto, ce, vazio=False)

    ids_ = {}
    ids_['da'] = desembarque('CTRA', d(2), BTP, M, pesoBrutoBalanca=25000, taraConteiner=3800,
                             pesoBrutoManifesto=21000)
    ids_['ia'] = inspecao('CTRA', d(2, 12), BTP, H1)
    acesso('CTRA', d(3), BTP, 'S', operacao='G')  # agendamento: ignorado
    ids_['sa'] = acesso('CTRA', d(4), BTP, 'S', H1)
    acesso('CTRA', d(4, 11), EADI, 'E')
    acesso('CTRA', d(8), EADI, 'S')
    ids_['db'] = desembarque('CTRB', d(3), ECOPORTO)
    acesso('CTRB', d(5), ECOPORTO, 'S')
    ids_['dc'] = desembarque('CTRC', d(5), BTP)
    acesso('CTRC', d(20), BTP, 'S')
    ids_['de'] = desembarque('CTRE', d(9), BTP, X, pesoBrutoBalanca=3800, taraConteiner=3800)
    ids_['ie'] = inspecao('CTRE', d(9, 14), BTP, Y)
    ids_['dv'] = desembarque('CTRV', d(12), BTP, pesoBrutoBalanca=0, taraConteiner=2200)
    desembarque('CTRR', d(6), RIO)
    for modelo, linhas in eventos.items():
        modelo.__table__.create(session.get_bind(), checkfirst=True)
        for linha in linhas:  # uma a uma: nem todas têm as mesmas colunas (pesos)
            session.execute(modelo.__table__.insert(), linha)

    def ciclo(ce, conteiner, desembarque_id, data, concluido=False, saida_id=None, escaneamentos=()):
        c = Ciclo(numeroCEmercante=ce, numeroConteiner=conteiner, desembarque_id=desembarque_id,
                  data_desembarque=data, codigoRecintoDesembarque=BTP, concluido=concluido, saida_id=saida_id)
        for esc_id in escaneamentos:
            c.escaneamentos.append(Esc(escaneamento_id=esc_id, data_escaneamento=data))
        session.add(c)

    ciclo(M, 'CTRA', ids_['da'], d(2), concluido=True, saida_id=ids_['sa'], escaneamentos=[ids_['ia']])
    ciclo(None, 'CTRC', ids_['dc'], d(5))
    ciclo(X, 'CTRE', ids_['de'], d(9), escaneamentos=[ids_['ie']])
    session.commit()
    return ids_


class TestFuncoes(unittest.TestCase):

    def lig(self, id_):
        return ldt.Ligacao(ldt.Evento(ldt.SAIDA, id_, d(1), BTP), ldt.ALTA)

    def test_compara_id(self):
        casos = [(None, None, 'nenhuma'), (1, self.lig(1), 'igual'), (None, self.lig(1), 'nova'),
                 (1, None, 'removida'), (1, self.lig(2), 'diferente')]
        for atual, novo, esperado in casos:
            with self.subTest(atual=atual, novo=novo):
                self.assertEqual(sombra.compara_id(atual, novo), esperado)

    def test_compara_ce(self):
        ces = CEsResolvidos(principal=H1, ces={H1: 'filhote', M: 'master'})
        casos = [(None, ces, 'novo'), (None, CEsResolvidos(), 'nenhum'), (M, CEsResolvidos(), 'perdido'),
                 (H1, ces, 'igual'), (M, ces, 'mesma família'), (S, ces, 'diferente')]
        for atual, resolvidos, esperado in casos:
            with self.subTest(atual=atual, esperado=esperado):
                self.assertEqual(sombra.compara_ce(atual, resolvidos, set(resolvidos.ces)), esperado)

    def test_situacao_atual(self):
        base = {'concluido': False, 'saida_id': None, 'entrada_seco_id': None, 'saida_seco_id': None}
        self.assertEqual(sombra.situacao_atual(None), 'sem ciclo atual')
        self.assertEqual(sombra.situacao_atual(base), 'pendente (sem saída)')
        self.assertEqual(sombra.situacao_atual({**base, 'saida_id': 1, 'entrada_seco_id': 2}), 'pendente (no seco)')
        self.assertEqual(sombra.situacao_atual({**base, 'concluido': True, 'saida_id': 1}),
                         'concluído (saiu do operador)')
        self.assertEqual(sombra.situacao_atual({**base, 'concluido': True, 'saida_seco_id': 3}),
                         'concluído (saiu do seco)')

    def test_faixas_e_normalizacoes(self):
        casos = [(None, 3800, 'sem peso'), (0, 3800, 'peso zero'), (4000, None, 'sem tara'),
                 (3800, 3800, 'peso <= tara'), (4000, 3800, '0 a 500 kg'), (5000, 3800, '500 a 2.000 kg'),
                 (25000, 3800, '> 2.000 kg')]
        for peso, tara, esperado in casos:
            with self.subTest(peso=peso, tara=tara):
                self.assertEqual(sombra._faixa_peso(peso, tara), esperado)
        self.assertEqual(sombra._faixa_idade(3), '< 7 dias (atraso do loop)')
        self.assertEqual(sombra._faixa_idade(10), '7 a 30 dias')
        self.assertEqual(sombra._faixa_idade(100), '>= 93 dias')
        self.assertEqual(sombra._unidade(817800), '0817800')
        self.assertEqual(sombra._unidade(' 0817800 '), '0817800')
        self.assertEqual(sombra._ano_do_ce(M), 2026)
        self.assertIsNone(sombra._ano_do_ce('123'))
        self.assertIsNone(sombra._texto('  '))
        self.assertEqual(sombra._texto(7), '7')


    def test_vazio_no_manifesto(self):
        self.assertEqual(sombra.vazio_no_manifesto([date(2026, 3, 12)], d(12)), sombra.SIM)
        self.assertEqual(sombra.vazio_no_manifesto([date(2025, 12, 1), date(2026, 2, 1)], d(3)), sombra.NAO)
        self.assertEqual(sombra.vazio_no_manifesto([], d(3)), sombra.NAO)

    def test_defasagem_mongo_pela_imagem_mais_proxima(self):
        db = mongomock.MongoClient()['teste']
        inspecao = d(10, 12)

        def imagem(conteiner, delta):
            db['fs.files'].insert_one({'metadata': {'numeroinformado': conteiner,
                                                    'dataescaneamento': inspecao + delta}})

        imagem('C1', timedelta(hours=-2, minutes=-58))
        imagem('C2', timedelta(minutes=20))
        imagem('C2', timedelta(hours=3))
        imagem('C3', timedelta(hours=9))  # fora da janela de 8 h
        self.assertEqual(sombra.defasagem_mongo(db, 'C1', inspecao), ('-3 h', True))
        self.assertEqual(sombra.defasagem_mongo(db, 'C2', inspecao), ('+0 h', False))  # resíduo de 20 min
        self.assertEqual(sombra.defasagem_mongo(db, 'C3', inspecao), ('nenhuma imagem em ±8 h', False))


class TestSombra(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        session = cria_sessao()
        cls.ids = monta(session)
        mongo = mongomock.MongoClient()['teste']
        # imagem da inspeção do CTRA: Mongo em UTC (+3 h), 2 minutos depois
        mongo['fs.files'].insert_one({'metadata': {'numeroinformado': 'CTRA', 'dataescaneamento':
                                                   d(2, 12) + timedelta(hours=3, minutes=2)}})
        # imagem da inspeção do CTRE: mesmo relógio da API (+0 h), 5 minutos depois
        mongo['fs.files'].insert_one({'metadata': {'numeroinformado': 'CTRE', 'dataescaneamento':
                                                   d(9, 14) + timedelta(minutes=5)}})
        saida = io.StringIO()
        cls.sombra = sombra.Sombra(session.connection(), saida, INICIO, FIM, conteineres=100, amostras=5,
                                   agora=AGORA)
        cls.falhas = sombra.roda(cls.sombra, mongo)
        cls.log = saida.getvalue()
        cls.linhas = {linha['conteiner']: linha for linha in cls.sombra.linhas}
        cls.session = session

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def valores(self, *inicio, colunas=None):
        """Células seguintes da primeira linha de tabela cujas primeiras células são `inicio` (e que tem
        `colunas` células, quando informado)."""
        for linha in self.log.splitlines():
            celulas = [c.strip() for c in linha.split('|')]
            if celulas[:len(inicio)] == list(inicio) and colunas in (None, len(celulas)):
                return celulas[len(inicio):]
        raise AssertionError(f'linha não encontrada: {inicio}')

    def test_sem_erros(self):
        self.assertEqual(self.falhas, [])
        self.assertNotIn('!! ERRO', self.log)

    def test_escopo_so_operadores_de_santos(self):
        self.assertEqual(self.valores(BTP), ['BTP'])
        self.assertEqual(self.valores(ECOPORTO), ['ECOPORTO SANTOS'])
        self.assertEqual(sorted(self.linhas), ['CTRA', 'CTRB', 'CTRC', 'CTRE', 'CTRV'])  # CTRR: Rio, fora

    def test_amostra_espalhada_e_reprodutivel(self):
        """A amostra segue o hash do id (não os mais recentes): o limite pega os de menor hash."""
        carregador = sombra.Carregador(self.sombra, INICIO, FIM)
        ids = {'CTRA': self.ids['da'], 'CTRB': self.ids['db'], 'CTRC': self.ids['dc'], 'CTRE': self.ids['de'],
               'CTRV': self.ids['dv']}
        esperado = sorted(ids, key=lambda c: (ids[c] * 2654435761) % 4294967291)[:2]
        self.assertEqual(carregador.conteineres({BTP, ECOPORTO}, 2), esperado)
        self.assertEqual(carregador.conteineres({BTP, ECOPORTO}, 2), esperado)

    def test_atraso_do_loop_antigo(self):
        self.assertIn('Último desembarque com ciclo atual: 2026-03-09 08:00:00 (atraso: 31.7 dias)', self.log)
        self.assertEqual(self.valores('2026-03-02'), ['1', '1'])
        self.assertEqual(self.valores('2026-03-12'), ['1', '0'])
        self.assertEqual(self.valores('7 a 30 dias', sombra.SIM), ['1'])  # CTRV: vazio, sem ciclo atual
        self.assertEqual(self.valores('30 a 93 dias', sombra.NAO), ['1'])  # CTRB: Ecoporto

    def test_tra_que_o_algoritmo_antigo_perdeu(self):
        linha = self.linhas['CTRA']
        self.assertEqual((linha['situacao_atual'], linha['situacao_nova'], linha['fluxo_novo']),
                         ('concluído (saiu do operador)', ldt.CONCLUIDO, ldt.TRA))
        self.assertEqual((linha['saida'], linha['entrada_seco'], linha['saida_seco']), ('igual', 'nova', 'nova'))
        self.assertEqual(linha['escaneamentos'], 'iguais')

    def test_ce_master_vira_filhote_da_mesma_familia(self):
        linha = self.linhas['CTRA']
        self.assertEqual((linha['ce_atual'], linha['ce_novo'], linha['ce_comparacao'], linha['ce_confianca']),
                         (M, H1, 'mesma família', ldt.ALTA))
        self.assertEqual(linha['ces_familia'], f'{M} {H1}')

    def test_ecoporto_sem_ciclo_atual(self):
        linha = self.linhas['CTRB']
        self.assertEqual((linha['ciclo_atual_id'], linha['situacao_atual']), (None, 'sem ciclo atual'))
        self.assertEqual((linha['situacao_nova'], linha['fluxo_novo'], linha['saida']),
                         (ldt.CONCLUIDO, ldt.OPERADOR, 'nova'))
        self.assertEqual((linha['ce_novo'], linha['ce_comparacao']), (S, 'novo'))  # exportação descartada
        self.assertEqual(self.valores(ECOPORTO, 'ECOPORTO SANTOS', colunas=5), ['1', '0', '1'])  # seção 1
        self.assertEqual(self.valores(BTP, 'BTP', colunas=5), ['4', '3', '1'])

    def test_saida_tardia(self):
        linha = self.linhas['CTRC']
        self.assertEqual((linha['situacao_atual'], linha['saida'], linha['situacao_nova']),
                         ('pendente (sem saída)', 'nova', ldt.CONCLUIDO))
        self.assertEqual(self.valores(ldt.MEDIA), ['1'])  # seção 6: uma saída tardia, confiança média

    def test_escaneamento_de_outra_familia_perdido(self):
        linha = self.linhas['CTRE']
        self.assertEqual((linha['escaneamentos'], linha['escaneamentos_perdidos']), ('perdeu', 1))
        self.assertEqual((linha['situacao_nova'], linha['ce_comparacao']), (ldt.ABERTO, 'igual'))
        self.assertIn('CE declarado de OUTRA família', self.log)

    def test_ces_de_anos_antigos_descartados(self):
        self.assertIn("'itens com o contêiner (todos os anos)': 6", self.log)
        self.assertIn("'CEs dos anos do período': 5", self.log)

    def test_escala_de_santos_preferida(self):
        carregador = sombra.Carregador(self.sombra, INICIO, FIM)
        candidatos = {c.ce: c for c in carregador.candidatos(['CTRA'])['CTRA']}
        self.assertEqual(sorted(candidatos), [M, H1])
        self.assertEqual(candidatos[M].datas_escala, (date(2026, 3, 1),))
        self.assertEqual(candidatos[H1].master, M)

    def test_vazio_do_mercante_peso_e_tara(self):
        self.assertEqual((self.linhas['CTRV']['vazio_mercante'], self.linhas['CTRB']['vazio_mercante']),
                         (sombra.SIM, sombra.NAO))
        self.assertEqual(self.valores(sombra.SIM, sombra.NAO, colunas=3), ['1'])  # vazio e sem CE
        self.assertEqual(self.valores(sombra.SIM, 'peso zero'), ['1'])
        self.assertEqual(self.valores(sombra.NAO, 'peso <= tara'), ['1'])
        self.assertEqual(self.valores(sombra.NAO, '> 2.000 kg'), ['1'])
        self.assertEqual((self.linhas['CTRA']['peso_balanca'], self.linhas['CTRA']['peso_manifesto']),
                         ('> 2.000 kg', '> 2.000 kg'))

    def test_mongo_defasagem_por_recinto(self):
        self.assertEqual(self.valores(BTP, 'BTP', '+3 h'), ['1', '1'])
        self.assertEqual(self.valores(BTP, 'BTP', '+0 h'), ['1', '1'])

    def test_amostras_das_divergencias(self):
        self.assertIn('--- entrada no seco removida/diferente: 0 amostra(s)', self.log)
        self.assertIn('--- escaneamento perdido: 1 amostra(s)', self.log)
        self.assertIn('--- pendente hoje, concluído na nova: 1 amostra(s)', self.log)

    def test_csv(self):
        with tempfile.TemporaryDirectory() as pasta:
            caminho = os.path.join(pasta, 'sombra.csv')
            self.sombra.grava_csv(caminho)
            with open(caminho, encoding='utf-8-sig') as arquivo:
                linhas = list(csv.DictReader(arquivo, delimiter=';'))
        self.assertEqual(len(linhas), 5)
        self.assertEqual(list(linhas[0]), sombra.COLUNAS_CSV)


class TestSemEscopo(unittest.TestCase):

    def test_sem_operadores_no_escopo_nao_consulta(self):
        session = cria_sessao()
        saida = io.StringIO()
        sombra_vazia = sombra.Sombra(session.connection(), saida, INICIO, FIM, agora=AGORA)
        self.assertEqual(sombra.Carregador(sombra_vazia, INICIO, FIM).conteineres(set(), 10), [])
        session.close()


if __name__ == '__main__':
    unittest.main()
