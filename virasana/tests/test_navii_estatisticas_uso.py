"""Testes ADVERSARIAIS do cálculo das estatísticas de uso (navii/estatisticas_uso.py) — F2a.

Universo de jan/2026 no cenário da busca (porto BRSSZ, janela efetiva): C1 (desembarque 11/01) e C4 (12/01) pelo
desembarque; C2 e C6 (sem desembarque; escala E1 prevista para 10/01) pela previsão — 4 itens. C3 só desembarca em
25/02 (fora).

Pesquisas (coleta real no mongomock; agora = 17/10/2026, então jan/2026 é período FECHADO):
  p0 20/12/2025 ANA  (setor 2) vê C2                   -> uso de dez; cobertura de jan (dentro da margem de 30 d)
  p1 12/01      ANA  (setor 2) NCM, total 2, vê C1 e C3 -> seletividade 2 / (2+1+1) = 50%
  p2 20/01      BETO (setor 9) só o porto (navegação), vê C1 de novo (repetido)
  p5 25/01      GRAND(setor 3, neto do 1) navegação, não vê nada
  p3 20/02      ANA  vê C6 (só clique)                  -> cobertura de jan (margem), uso de fev
  p4 05/03      ANA  vê C4                              -> fora da margem: não conta
Cobertura de jan: C1, C2, C6 = 3 de 4.
Setores: 1 DIREP > 2 EQRIMP > 3 SUB; 9 SACIT. SUP (setor 1) é Supervisor; ANA, Operadora.
"""
import unittest
from datetime import date, datetime
from unittest import mock

import mongomock

from virasana.navii import acompanhamento_fichas as af
from virasana.navii import busca_navii, coleta_uso, tabelas_opcionais
from virasana.navii import estatisticas_uso as eu
from virasana.integracao.mercante.mercantealchemy import Escala, EscalaManifesto  # noqa: I100 (depois do navii)
from virasana.tests.test_diagnostico_coleta_uso import monta as monta_coleta
from virasana.tests.test_navii_busca_sqlite import cria_ce, cria_sessao, monta_cenario

ANA, BETO, SUP, GRAND, SEM_CADASTRO = '11111111111', '22222222222', '33333333333', '44444444444', '55555555555'
AGORA = datetime(2026, 10, 17, 12)


def d(texto):
    return date.fromisoformat(texto)


class TestPeriodos(unittest.TestCase):

    def test_codigos_rotulos_e_vizinhos(self):
        for tipo, codigo, rotulo, anterior, inicio, fim in (
                ('mes', '2026-01', 'jan/2026', '2025-12', '2026-01-01', '2026-01-31'),
                ('mes', '2024-02', 'fev/2024', '2024-01', '2024-02-01', '2024-02-29'),
                ('trimestre', '2026-T1', '1º trimestre de 2026', '2025-T4', '2026-01-01', '2026-03-31'),
                ('semestre', '2026-S1', '1º semestre de 2026', '2025-S2', '2026-01-01', '2026-06-30'),
                ('ano', '2026', '2026', '2025', '2026-01-01', '2026-12-31')):
            with self.subTest(codigo=codigo):
                p = eu.le_periodo(tipo, codigo, d('2026-10-17'))
                self.assertEqual((p.codigo, p.rotulo, p.anterior().codigo, p.inicio, p.fim),
                                 (codigo, rotulo, anterior, d(inicio), d(fim)))
                self.assertEqual(p.anterior().seguinte(), p)
        self.assertEqual(eu.le_periodo('trimestre', None, d('2026-10-17')).codigo, '2026-T4')
        self.assertEqual(eu.le_periodo('semestre', None, d('2026-06-30')).codigo, '2026-S1')

    def test_codigos_invalidos(self):
        for tipo, codigo in (('dia', None), ('mes', '2026-13'), ('mes', '2026-00'), ('trimestre', '2026-T5'),
                             ('semestre', '2026-S3'), ('mes', '1999-12'), ('ano', '26'), ('mes', '2026-T1'),
                             ('mes', "2026-01' OR 1=1")):
            with self.subTest(codigo=codigo), self.assertRaises(ValueError):
                eu.le_periodo(tipo, codigo, d('2026-10-17'))

    def test_em_curso_compara_com_o_mesmo_trecho(self):
        outubro = eu.le_periodo('mes', '2026-10', d('2026-10-17'))
        self.assertEqual(eu.comparacao(outubro, d('2026-10-17')),
                         eu.Comparacao((d('2026-10-01'), d('2026-10-17')), (d('2026-09-01'), d('2026-09-17')), True))
        marco = eu.le_periodo('mes', '2026-03', d('2026-03-31'))                       # hoje = último dia
        self.assertEqual(eu.comparacao(marco, d('2026-03-31')),
                         eu.Comparacao((d('2026-03-01'), d('2026-03-31')), (d('2026-02-01'), d('2026-02-28')), True))
        t4 = eu.le_periodo('trimestre', '2026-T4', d('2026-10-17'))
        self.assertEqual(eu.comparacao(t4, d('2026-10-17')).anterior, (d('2026-07-01'), d('2026-07-17')))
        setembro = eu.le_periodo('mes', '2026-09', d('2026-10-17'))
        self.assertEqual(eu.comparacao(setembro, d('2026-10-17')),
                         eu.Comparacao((d('2026-09-01'), d('2026-09-30')), (d('2026-08-01'), d('2026-08-31')), False))

    def test_pedacos_por_mes(self):
        self.assertEqual(eu.pedacos(d('2026-09-15'), d('2026-11-03')),
                         [(d('2026-09-15'), d('2026-09-30')), (d('2026-10-01'), d('2026-10-31')),
                          (d('2026-11-01'), d('2026-11-03'))])
        self.assertEqual(eu.pedacos(d('2025-12-31'), d('2026-01-01')),
                         [(d('2025-12-31'), d('2025-12-31')), (d('2026-01-01'), d('2026-01-01'))])

    def test_pedaco_vindo_da_tela(self):
        self.assertEqual(eu.Pedaco.de_dict({'janela': 'efetiva', 'inicio': '2026-01-01', 'fim': '2026-01-31'}),
                         eu.Pedaco('efetiva', d('2026-01-01'), d('2026-01-31')))
        for ruim in ({'janela': 'x', 'inicio': '2026-01-01', 'fim': '2026-01-02'},
                     {'janela': 'efetiva', 'inicio': '2026-01-01', 'fim': '2026-12-31'},     # um ano de uma vez
                     {'janela': 'efetiva', 'inicio': '2026-02-01', 'fim': '2026-01-01'}, {'janela': 'efetiva'}, []):
            with self.subTest(ruim=ruim), self.assertRaises(ValueError):
                eu.Pedaco.de_dict(ruim)


def pesquisa(usuario, setor, filtros, periodo=('2026-01-05', '2026-01-15')):
    return {'_id': object(), 'usuario': usuario, 'setor': setor, 'janela': 'efetiva', 'filtros': filtros,
            'periodo': {'inicio': periodo[0], 'fim': periodo[1]} if periodo else None, 'total': 10}


class TestAgregacao(unittest.TestCase):
    """O cenário da conferência da coleta (test_diagnostico_coleta_uso): ANA em duas abas, pausa de 10:00, C1 visto de
    novo no refinamento, extrato do C3 só clicado, Ficha 77 aberta pelo card — e aqui um 2º clique (já existia)."""

    def setUp(self):
        self.col = coleta_uso.colecao(mongomock.MongoClient()['v'])
        monta_coleta(self.col)
        p2 = self.col.find_one({'origem': 'card'})['_id']
        for ficha, resultado in ((77, 'ja_existia'), (78, 'acrescentada')):
            coleta_uso.registra_acao(self.col, p2, '52998224725', {'t': 'ficha', 'ficha': ficha, 'resultado': resultado},
                                     agora=datetime(2026, 10, 17, 9, 6))

    def test_numeros(self):
        docs = eu.pesquisas(self.col, d('2026-10-17'), d('2026-10-17'))
        numeros = eu.agrega(docs).numeros()
        esperado = {'pesquisas': 5, 'usuarios': 2, 'sessoes': 4, 'horas': 0.1, 'paginas': 5, 'enviados': 10,
                    'vistos': 4, 'examinados': 3, 'repetidos': 1, 'acionados': 2, 'fichasNovas': 1,
                    'vistosPorHora': 40.0, 'examinadosPorVistos': 75.0, 'acionadosPorExaminados': 66.7,
                    'profundidade': 5.0}                             # a 78 recebeu o contêiner: acionada, não nova
        self.assertEqual(numeros, esperado)
        como = eu.agrega(docs)
        self.assertEqual((como.origens['tela'], como.transicoes['mais critérios'], como.campos['ncm']), (3, 1, 3))
        self.assertEqual(eu.por_dia(docs), [{'dia': '2026-10-17', 'pesquisas': 5, 'usuarios': 2, 'vistos': 4}])
        self.assertEqual(eu.agrega([]).numeros()['vistosPorHora'], None)

    def test_limites_do_dia(self):
        col = coleta_uso.colecao(mongomock.MongoClient()['w'])
        for quando in (datetime(2026, 10, 16, 23, 59, 59), datetime(2026, 10, 17), datetime(2026, 10, 17, 23, 59, 59),
                       datetime(2026, 10, 18)):
            coleta_uso.registra_pesquisa(col, ANA, '2', 'aba-0001-teste', None, 'tela', 'efetiva', (None, None), {}, 0,
                                         {}, [], agora=quando)
        self.assertEqual([doc['inicio'] for doc in eu.pesquisas(col, d('2026-10-17'), d('2026-10-17'))],
                         [datetime(2026, 10, 17), datetime(2026, 10, 17, 23, 59, 59)])


class TestSeletividade(unittest.TestCase):

    def test_so_pesquisas_na_base_com_criterio_e_universo_de_todos_os_dias(self):
        com_ncm = {'porto_escala': ['BRSSZ'], 'ncm': ['8517']}
        docs = [
            {**pesquisa(ANA, '2', com_ncm, ('2026-01-01', '2026-01-02')), 'total': 50},     # 50 / 250 = 20%
            {**pesquisa(ANA, '2', com_ncm, ('2026-01-01', '2026-01-01')), 'total': 10},     # 10 / 100 = 10%
            pesquisa(ANA, '2', {'porto_escala': ['BRSSZ']}),                                 # navegação
            pesquisa(ANA, '2', {'porto_escala': ['BRRIG'], 'ncm': ['1']}),                   # outra base
            pesquisa(ANA, '2', {'ncm': ['1']}),                                              # sem porto
            pesquisa(ANA, '2', com_ncm, periodo=None),                                       # sem período
            pesquisa(ANA, '2', com_ncm, ('2026-01-01', '2026-01-03')),                       # falta o dia 3
            pesquisa(ANA, '2', com_ncm, ('2026-01-01', '2026-06-30')),                       # longo demais
            pesquisa(ANA, '2', com_ncm, ('2026-01-05', '2026-01-04')),                       # invertido
        ]
        diario = {('efetiva', d('2026-01-01')): 100, ('efetiva', d('2026-01-02')): 150}
        self.assertEqual(eu.seletividade(docs, 'BRSSZ', diario),
                         {'mediana': 15.0, 'pesquisas': 2, 'navegacao': 1, 'foraDaBase': 3, 'semUniverso': 3})
        self.assertEqual(len(eu.dias_necessarios(docs, 'BRSSZ')), 3)     # 01, 02 e 03/01 (o longo não pede)

    def test_universo_zero_nao_divide(self):
        doc = pesquisa(ANA, '2', {'porto_escala': ['BRSSZ'], 'ncm': ['1']}, ('2026-01-01', '2026-01-01'))
        self.assertEqual(eu.seletividade([doc], 'BRSSZ', {('efetiva', d('2026-01-01')): 0})['semUniverso'], 1)


class CenarioTestCase(unittest.TestCase):

    def setUp(self):
        tabelas_opcionais.limpa_cache()
        coleta_uso.limpa_cache()
        self.session = cria_sessao()
        monta_cenario(self.session)
        self.item = {c: i for i, c in self.session.query(busca_navii.Item.ID, busca_navii.Item.codigoConteiner)}
        self.db = mongomock.MongoClient()['virasana']
        self.col = coleta_uso.colecao(self.db)
        self.col_universo = eu.colecao_universo(self.db)

    def tearDown(self):
        self.session.close()

    def conta(self, inicio, fim, porto='BRSSZ', janela='efetiva'):
        return busca_navii.conta_conteineres_navii(self.session, janela, {
            'data_inicio': inicio, 'data_fim': fim, 'porto_escala': [porto]})


def adiciona_adversarios(session):
    """CE de EXPORTAÇÃO no manifesto de jan (fica fora sempre) e escala de jan em OUTRO porto (M3 em BRRIG, 08/01: o
    C7 entraria num universo sem o porto)."""
    cria_ce(session, 'CE7', 'M1', {'C8': []}, tipo_trafego='7')
    session.add_all([Escala(numeroDaEscala='E3', dataPrevistaAtracacao='2026-01-08', portoDaEscala='BRRIG'),
                     EscalaManifesto(ID=3, manifesto='M3', escala='E3')])
    session.commit()


class TestUniverso(CenarioTestCase):

    def setUp(self):
        super().setUp()
        adiciona_adversarios(self.session)
        self.item = {c: i for i, c in self.session.query(busca_navii.Item.ID, busca_navii.Item.codigoConteiner)}

    def test_calcula_guarda_e_soma_os_meses(self):
        total, faltam = eu.universo(self.col_universo, 'BRSSZ', d('2026-01-01'), d('2026-02-28'), AGORA)
        self.assertIsNone(total)
        self.assertEqual(faltam, [eu.Pedaco('efetiva', d('2026-01-01'), d('2026-01-31')),
                                  eu.Pedaco('efetiva', d('2026-02-01'), d('2026-02-28'))])
        self.assertEqual(eu.calcula_pendencias(self.session, self.col_universo, 'BRSSZ', faltam, AGORA), [])
        total, faltam = eu.universo(self.col_universo, 'BRSSZ', d('2026-01-01'), d('2026-02-28'), AGORA)
        self.assertEqual((total, faltam), (self.conta('2026-01-01', '2026-01-31') + self.conta('2026-02-01',
                                                                                               '2026-02-28'), []))
        self.assertEqual(self.conta('2026-01-01', '2026-01-31'), 4)          # sem o C8 (exportação) e o C7 (BRRIG)
        self.assertEqual(busca_navii.conta_conteineres_navii(self.session, 'efetiva', {
            'data_inicio': '2026-01-01', 'data_fim': '2026-01-31'}), 5)     # sem o porto, o C7 entraria
        # outro porto é outra chave (e outro número: a previsão de BRRIG é vazia)
        self.assertIsNone(eu.universo(self.col_universo, 'BRRIG', d('2026-01-01'), d('2026-01-31'), AGORA)[0])

    def test_validade_e_definitivo(self):
        recente = eu.Pedaco('efetiva', d('2026-10-01'), d('2026-10-16'))
        eu.calcula_universo(self.session, self.col_universo, 'BRSSZ', recente, AGORA)
        self.assertIsNotNone(eu.universo_guardado(self.col_universo, 'BRSSZ', recente, AGORA.replace(hour=17)))
        self.assertIsNone(eu.universo_guardado(self.col_universo, 'BRSSZ', recente, AGORA.replace(hour=19)))  # >6 h
        janeiro = eu.Pedaco('efetiva', d('2026-01-01'), d('2026-01-31'))
        eu.calcula_universo(self.session, self.col_universo, 'BRSSZ', janeiro, datetime(2026, 2, 15))   # 15 dias
        self.assertIsNone(eu.universo_guardado(self.col_universo, 'BRSSZ', janeiro, AGORA))             # não basta
        eu.calcula_universo(self.session, self.col_universo, 'BRSSZ', janeiro, datetime(2026, 2, 16))
        self.assertEqual(eu.universo_guardado(self.col_universo, 'BRSSZ', janeiro, datetime(2030, 1, 1)), 4)

    def test_orcamento_de_tempo(self):
        pedacos = [eu.Pedaco('efetiva', d(f'2026-01-0{n}'), d(f'2026-01-0{n}')) for n in (1, 2, 3)]
        relogio = iter([0, 5, 10]).__next__
        restantes = eu.calcula_pendencias(self.session, self.col_universo, 'BRSSZ', pedacos, AGORA, orcamento=8,
                                          relogio=relogio)
        self.assertEqual(restantes, pedacos[2:])
        relogio = iter([0, 100]).__next__
        self.assertEqual(eu.calcula_pendencias(self.session, self.col_universo, 'BRSSZ', pedacos[2:], AGORA,
                                               orcamento=8, relogio=relogio), [])    # ao menos um, sempre

    def test_vistos_no_universo_pela_mesma_regra_da_tela(self):
        todos = set(self.item.values())
        consulta = busca_navii.monta_query_navii(self.session, 'efetiva', {
            'data_inicio': '2026-01-01', 'data_fim': '2026-01-31', 'porto_escala': ['BRSSZ']})
        pela_tela = {i for (i,) in consulta.with_entities(busca_navii.Item.ID)}
        self.assertEqual(eu.itens_no_universo(self.session, 'BRSSZ', d('2026-01-01'), d('2026-01-31'),
                                              todos | {987654}), pela_tela)
        self.assertEqual(pela_tela, {self.item[c] for c in ('C1', 'C2', 'C4', 'C6')})
        with mock.patch.object(eu, 'LOTE_ITENS', 2):                        # em lotes, o mesmo resultado
            self.assertEqual(eu.itens_no_universo(self.session, 'BRSSZ', d('2026-01-01'), d('2026-01-31'), todos),
                             pela_tela)


class TestAcesso(CenarioTestCase):

    def setUp(self):
        super().setUp()
        motor = self.session.get_bind()
        for tabela in (af.USUARIOS, af.SETORES, eu.PERFIS):
            tabela.create(motor)
        self.session.execute(af.SETORES.insert(), [
            {'id': '1', 'nome': 'DIREP', 'pai_id': None}, {'id': '2', 'nome': 'EQRIMP', 'pai_id': '1'},
            {'id': '3', 'nome': 'SUB', 'pai_id': '2'}, {'id': '9', 'nome': 'SACIT', 'pai_id': None}])
        self.session.execute(af.USUARIOS.insert(), [
            {'cpf': ANA, 'nome': 'Ana', 'setor_id': '2'}, {'cpf': BETO, 'nome': 'Beto', 'setor_id': '9'},
            {'cpf': SUP, 'nome': 'Sup', 'setor_id': '1'}, {'cpf': GRAND, 'nome': 'Abel', 'setor_id': '3'}])
        self.session.execute(eu.PERFIS.insert(), [{'cpf': SUP, 'perfil': 2}, {'cpf': ANA, 'perfil': 1},
                                                  {'cpf': BETO, 'perfil': 1}])

    def test_supervisor_ve_os_seus_setores_em_qualquer_nivel(self):
        sup = eu.acesso_do_usuario(self.session, SUP)
        self.assertEqual((sup.supervisor, sup.setores), (True, frozenset({'1', '2', '3'})))
        self.assertTrue(sup.ve(ANA, '2') and sup.ve(GRAND, '3') and sup.ve(SUP, None))
        self.assertFalse(sup.ve(BETO, '9') or sup.ve(BETO, None))

    def test_quem_nao_e_supervisor_so_se_ve(self):
        ana = eu.acesso_do_usuario(self.session, ANA)
        self.assertEqual((ana.supervisor, ana.setores), (False, frozenset({'2', '3'})))
        self.assertFalse(ana.ve(GRAND, '3'))                                 # subordinado, mas ela não supervisiona
        self.assertTrue(ana.ve(ANA, '9'))
        self.assertEqual(eu.acesso_do_usuario(self.session, SEM_CADASTRO), eu.Acesso(SEM_CADASTRO))

    def test_sem_as_tabelas_do_fichas(self):
        tabelas_opcionais.limpa_cache()
        session = cria_sessao()
        self.assertEqual(eu.acesso_do_usuario(session, SUP), eu.Acesso(SUP))


class TestEstatisticas(TestAcesso):

    def setUp(self):
        super().setUp()
        ncm = {'porto_escala': ['BRSSZ'], 'ncm': ['8471']}
        so_porto = {'porto_escala': ['BRSSZ']}
        for usuario, setor, quando, filtros, total, periodo, vistos in (
                (ANA, '2', datetime(2025, 12, 20, 10), so_porto, 1, ('2025-12-15', '2025-12-25'), [('C2', 1, [])]),
                (ANA, '2', datetime(2026, 1, 12, 10), ncm, 2, ('2026-01-05', '2026-01-15'),
                 [('C1', 5, []), ('C3', 3, [])]),
                (BETO, '9', datetime(2026, 1, 20, 9), so_porto, 4, ('2026-01-15', '2026-01-25'), [('C1', 2, [])]),
                (GRAND, '3', datetime(2026, 1, 25, 9), so_porto, 4, ('2026-01-15', '2026-01-25'), []),
                (ANA, '2', datetime(2026, 2, 20, 9), so_porto, 4, ('2026-01-01', '2026-01-31'),
                 [('C6', 0, ['imagem'])]),
                (ANA, '2', datetime(2026, 3, 5, 9), so_porto, 4, ('2026-01-01', '2026-01-31'), [('C4', 4, [])])):
            cards = [{'item_id': self.item[c], 'numeroConteiner': c} for c, _, _ in vistos]
            id_ = coleta_uso.registra_pesquisa(self.col, usuario, setor, f'aba-{usuario}', None, 'tela', 'efetiva',
                                               periodo, filtros, total, {}, cards, agora=quando)
            lote = [{'item': self.item[c], 's': s, 'x': x} for c, s, x in vistos]
            coleta_uso.registra_vistos(self.col, id_, usuario, lote, quando)

    def estatisticas(self, cpf=ANA, codigo='2026-01'):
        return eu.estatisticas(self.session, self.db, cpf, 'mes', codigo, agora=AGORA)

    def calcula_tudo(self, dados):
        pendentes = [eu.Pedaco.de_dict(p) for p in dados['pendentes']]
        self.assertEqual(eu.calcula_pendencias(self.session, self.col_universo, 'BRSSZ', pendentes, AGORA,
                                               orcamento=1000), [])

    def test_primeiro_pede_o_universo_depois_mostra_tudo(self):
        dados = self.estatisticas()
        self.assertIsNone(dados['cobertura']['atual'])
        self.assertEqual(len(dados['pendentes']), 2 + 11)                   # jan e dez (meses) + 11 dias de p1
        self.calcula_tudo(dados)
        dados = self.estatisticas()
        self.assertEqual(dados['pendentes'], [])
        self.assertEqual(dados['cobertura']['atual'], {'universo': 4, 'vistos': 3, 'percentual': 75.0})
        self.assertEqual(dados['cobertura']['anterior'], {'universo': 0, 'vistos': 0, 'percentual': None})
        self.assertEqual(dados['seletividade'], {'mediana': 50.0, 'pesquisas': 1, 'navegacao': 2, 'foraDaBase': 0,
                                                 'semUniverso': 0})

    def test_uso_e_comparacao(self):
        dados = self.estatisticas()
        atual, anterior = dados['uso']['atual'], dados['uso']['anterior']
        self.assertEqual((atual['pesquisas'], atual['usuarios'], atual['vistos'], atual['repetidos']), (3, 3, 2, 1))
        self.assertEqual((anterior['pesquisas'], anterior['vistos']), (1, 1))
        self.assertEqual(dados['periodo']['anterior']['codigo'], '2025-12')
        self.assertEqual((dados['periodo']['parcial'], dados['periodo']['seguinte']), (False, '2026-02'))
        self.assertEqual([linha['dia'] for linha in dados['dias']], ['2026-01-12', '2026-01-20', '2026-01-25'])
        self.assertIsNone(self.estatisticas(codigo='2026-10')['periodo']['seguinte'])          # o futuro não abre

    def test_acesso_por_usuario_e_setores_para_todos(self):
        nomes = [linha['nome'] for linha in self.estatisticas(SUP)['usuarios']]
        self.assertEqual(nomes, ['Abel', 'Ana'])            # sem o Beto (SACIT); pelo nome, não pelo CPF
        self.assertEqual([linha['nome'] for linha in self.estatisticas(ANA)['usuarios']], ['Ana'])
        self.assertEqual([linha['nome'] for linha in self.estatisticas(BETO)['usuarios']], ['Beto'])
        self.assertEqual([linha['nome'] for linha in self.estatisticas(BETO)['setores']], ['EQRIMP', 'SACIT', 'SUB'])
        self.assertEqual([linha['nome'] for linha in self.estatisticas(SEM_CADASTRO)['usuarios']], [])

    def test_quem_mudou_de_setor_fica_com_o_da_epoca(self):
        self.session.execute(af.USUARIOS.update().where(af.USUARIOS.c.cpf == ANA).values(setor_id='9'))
        self.assertIn('Ana', [linha['nome'] for linha in self.estatisticas(SUP)['usuarios']])

    def test_periodo_invalido(self):
        with self.assertRaises(ValueError):
            self.estatisticas(codigo='2026-13')


if __name__ == '__main__':
    unittest.main()
