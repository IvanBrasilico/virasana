"""Testes de integração do NAVI-I: consultas REAIS (SQLAlchemy) contra SQLite em
memória, com um cenário simulado que cobre contêineres com e sem ciclo de vida,
com e sem imagem, dentro e fora da janela, e os filtros da tela.

Cenário (período de teste 2026-01-05 a 2026-01-15):

    Escala E1 (prevista 2026-01-10, NAVIO A) -> Manifesto M1 (carregado em CNSHA, terminal T01)
    Escala E2 (prevista 2026-02-20, NAVIO B) -> Manifesto M2 (carregado em SGSIN, terminal T50)
    Manifesto M3: sem escala

    CE1 (M1) C1: ciclo, desembarque 01-11 no 8931356, imagem, concluído
             C2: SEM ciclo de vida
    CE2 (M1) C3: ciclo, desembarque 02-25 (fora da janela efetiva)
    CE3 (M2) C4: ciclo, desembarque 01-12 (escala fora, mas desembarque dentro),
                 escaneado SEM imagem, armazenagem 8932000
    CE4 (M2) C5: sem ciclo, escala fora -> nunca aparece no período
    CE5 (M3) C7: sem escala -> nunca aparece
    CE6 (M1) C6: ciclo SEM data de desembarque (ciclo incompleto) -> cai na prevista

    Efetiva no período: C1, C2, C4, C6      Prevista no período: C1, C2, C3, C6
"""
import unittest
from datetime import datetime
from unittest import mock

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from virasana.navii import busca_navii
from virasana.navii.busca_navii import (
    busca_ces_navii, busca_ciclos_navii, busca_familias_navii, busca_manifestos_navii,
    busca_nomes_recintos, conta_conteineres_navii, monta_cards_navii,
    monta_query_navii, normaliza_filtros, tabela_recintos,
)
# Importados depois de busca_navii, que já ajusta o sys.path para o repo
# irmão bhadrasana2/ajna_commons (ver virasana/navii/busca_navii.py).
from virasana.analytic_scripts.ciclo_de_vida.ciclo_de_vida import (
    CicloDeVidaCE, CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos,
)
from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, Enumerado, Escala, EscalaManifesto, Item, Manifesto, NCMItem,
)
from virasana.navii.views_navii import bp_navii
from bhadrasana.models.apirecintos import EmbarqueDesembarque
from bhadrasana.models.laudo import Empresa

PERIODO = {'data_inicio': '2026-01-05', 'data_fim': '2026-01-15'}

TABELAS = [
    Enumerado, Escala, EscalaManifesto, Manifesto, Conhecimento, Item, NCMItem,
    CicloDeVidaImportacao, CicloDeVidaImportacaoEscaneamentos, CicloDeVidaCE,
    Empresa, EmbarqueDesembarque,   # desembarque: tipo do contêiner na completude
]


def cria_sessao():
    engine = create_engine('sqlite://')
    for modelo in TABELAS:
        modelo.__table__.create(engine)
    tabela_recintos.create(engine)
    return sessionmaker(bind=engine)()


def cria_ce(session, numero, manifesto, conteineres, embarcador='EMBARCADOR QUALQUER',
            consignatario='00000000000000', porto_origem='CNSHA', porto_destino='BRSSZ',
            tipo_trafego='5', descricao=None):
    """conteineres: {codigo: [(ncm, descritivo), ...]}"""
    session.add(Conhecimento(
        numeroCEmercante=numero, manifestoCE=manifesto, embarcador=embarcador,
        consignatario=consignatario, portoOrigemCarga=porto_origem,
        portoDestFinal=porto_destino, descricao=descricao or f'mercadoria do {numero}',
        tipoTrafego=tipo_trafego, tipoBLConhecimento='12',
    ))
    for codigo, ncms in conteineres.items():
        session.add(Item(numeroCEmercante=numero, codigoConteiner=codigo,
                         pesoBruto='1000', tipoItemCarga='1'))
        for ncm, descritivo in ncms:
            session.add(NCMItem(numeroCEMercante=numero, codigoConteiner=codigo,
                                identificacaoNCM=ncm, descritivo=descritivo))


def cria_ciclo(session, ce, conteiner, data_desembarque=None, recinto_desembarque=None,
               recinto_armazenagem=None, concluido=False, imagens=()):
    ciclo = CicloDeVidaImportacao(
        numeroCEmercante=ce, numeroConteiner=conteiner, data_desembarque=data_desembarque,
        codigoRecintoDesembarque=recinto_desembarque,
        codigoRecintoArmazenagem=recinto_armazenagem, concluido=concluido,
    )
    for i, imagem_id in enumerate(imagens):
        ciclo.escaneamentos.append(CicloDeVidaImportacaoEscaneamentos(
            escaneamento_id=i + 1, data_escaneamento=datetime(2026, 1, 11, 10, 0),
            imagem_id=imagem_id,
        ))
    session.add(ciclo)
    return ciclo


def monta_cenario(session):
    session.add_all([
        Escala(numeroDaEscala='E1', dataPrevistaAtracacao='2026-01-10', embarcacao='NAVIO A',
               portoDaEscala='BRSSZ'),
        Escala(numeroDaEscala='E2', dataPrevistaAtracacao='2026-02-20', embarcacao='NAVIO B',
               portoDaEscala='BRSSZ'),
        # BIGINT PK no SQLite não é autoincremento -> ID explícito
        EscalaManifesto(ID=1, manifesto='M1', escala='E1'),
        EscalaManifesto(ID=2, manifesto='M2', escala='E2'),
        Manifesto(numero='M1', portoCarregamento='CNSHA', codigoTerminalDescarregamento='T01'),
        Manifesto(numero='M2', portoCarregamento='SGSIN', codigoTerminalDescarregamento='T50'),
        Manifesto(numero='M3', portoCarregamento='CNSHA'),
        Empresa(cnpj='11222333000181', nome='IMPORTADORA ALFA LTDA'),
    ])
    session.execute(tabela_recintos.insert(), [
        {'id': 1, 'nome': 'BTP', 'cod_siscomex': '8931356', 'cod_carga': 'T01'},
        {'id': 2, 'nome': 'EADI SANTOS', 'cod_siscomex': '8932000', 'cod_carga': 'T99'},
    ])
    cria_ce(session, 'CE1', 'M1', {
        'C1': [('84713000', 'Notebook'), ('84716000', 'Teclado')],
        'C2': [('95030000', 'Brinquedo de plastico')],
    }, embarcador='ACME EXPORT CO', consignatario='11222333000181',
        descricao='NOTEBOOKS, TECLADOS E BRINQUEDOS DE PLASTICO')
    cria_ce(session, 'CE2', 'M1', {'C3': [('84713000', 'Notebook')]})
    cria_ce(session, 'CE3', 'M2', {'C4': [('61091000', 'Camiseta')]},
            consignatario='LOJA X LTDA', porto_origem='CNNGB', descricao='T-SHIRTS DE ALGODAO')
    cria_ce(session, 'CE4', 'M2', {'C5': [('61091000', 'Camiseta')]})
    cria_ce(session, 'CE5', 'M3', {'C7': []})
    cria_ce(session, 'CE6', 'M1', {'C6': []}, porto_destino='BRRIG')

    cria_ciclo(session, 'CE1', 'C1', datetime(2026, 1, 11, 8, 0), '8931356', concluido=True,
               imagens=['5f0000000000000000000001', '5f0000000000000000000002'])
    cria_ciclo(session, 'CE2', 'C3', datetime(2026, 2, 25, 8, 0), '8931356')
    cria_ciclo(session, 'CE3', 'C4', datetime(2026, 1, 12, 8, 0), '8931356',
               recinto_armazenagem='8932000', imagens=[None])
    cria_ciclo(session, 'CE6', 'C6', None)
    session.commit()


class NaviiSqliteTestCase(unittest.TestCase):
    def setUp(self):
        self.session = cria_sessao()
        monta_cenario(self.session)

    def tearDown(self):
        self.session.close()

    def conteineres(self, tipo_janela='efetiva', exigir_escaneamento=False, **filtros):
        """Conjunto de contêineres retornados (página única grande)."""
        filtros = {**PERIODO, **filtros}
        resultado = busca_ces_navii(self.session, tipo_janela, filtros,
                                    exigir_escaneamento=exigir_escaneamento, limit=50)
        return {r.item.codigoConteiner for r in resultado}

    def total(self, tipo_janela='efetiva', **filtros):
        return conta_conteineres_navii(self.session, tipo_janela, {**PERIODO, **filtros})


class TestJanelas(NaviiSqliteTestCase):

    def test_efetiva_inclui_conteineres_sem_ciclo_pela_atracacao_prevista(self):
        self.assertEqual(self.conteineres(), {'C1', 'C2', 'C4', 'C6'})

    def test_efetiva_desembarque_fora_do_periodo_exclui_mesmo_com_escala_dentro(self):
        # C3: escala E1 no período, mas o ciclo registra desembarque em 02-25
        self.assertNotIn('C3', self.conteineres())

    def test_efetiva_desembarque_dentro_do_periodo_inclui_mesmo_com_escala_fora(self):
        self.assertIn('C4', self.conteineres())

    def test_efetiva_ciclo_sem_data_de_desembarque_cai_na_prevista(self):
        self.assertIn('C6', self.conteineres())

    def test_prevista_nao_depende_do_ciclo(self):
        self.assertEqual(self.conteineres('prevista'), {'C1', 'C2', 'C3', 'C6'})

    def test_manifesto_com_varias_escalas_no_periodo_nao_duplica_conteiner(self):
        # M1 passa a ter duas escalas dentro do período (na base real são ~4 por manifesto)
        self.session.add_all([
            Escala(numeroDaEscala='E3', dataPrevistaAtracacao='2026-01-12', embarcacao='NAVIO A'),
            EscalaManifesto(ID=3, manifesto='M1', escala='E3'),
        ])
        self.session.commit()
        for tipo_janela, esperado in (('prevista', {'C1', 'C2', 'C3', 'C6'}), ('efetiva', {'C1', 'C2', 'C4', 'C6'})):
            with self.subTest(tipo_janela=tipo_janela):
                resultado = busca_ces_navii(self.session, tipo_janela, PERIODO, limit=50)
                conteineres = [r.item.codigoConteiner for r in resultado]
                self.assertEqual(sorted(conteineres), sorted(esperado))
                self.assertEqual(self.total(tipo_janela), len(esperado))

    def test_periodo_sem_nada(self):
        self.assertEqual(self.conteineres(data_inicio='2030-01-01', data_fim='2030-01-31'), set())
        self.assertEqual(self.total(data_inicio='2030-01-01', data_fim='2030-01-31'), 0)

    def test_limite_do_ultimo_dia_inclui_o_dia_inteiro(self):
        # desembarque de C1 às 08:00 de 01-11: período terminando em 01-11 inclui
        self.assertIn('C1', self.conteineres(data_inicio='2026-01-11', data_fim='2026-01-11'))

    def test_janela_sugerida_sem_datas_nao_quebra(self):
        self.assertIsInstance(conta_conteineres_navii(self.session, 'efetiva', {}), int)
        self.assertIsInstance(conta_conteineres_navii(self.session, 'prevista', {}), int)

    def test_tipo_janela_invalido(self):
        with self.assertRaises(ValueError):
            monta_query_navii(self.session, 'qualquer', PERIODO)

    def test_data_invalida(self):
        with self.assertRaises(ValueError):
            busca_ces_navii(self.session, 'efetiva', {'data_inicio': '2026-13-01', 'data_fim': '2026-13-31'})


class TestTrafegoEPorto(NaviiSqliteTestCase):
    """Só exportação é excluída; a escala que vale é a do porto de atracação escolhido."""

    def test_exportacao_nunca_aparece_cabotagem_passagem_e_sem_trafego_aparecem(self):
        cria_ce(self.session, 'CEX', 'M1', {'CEXP': []}, tipo_trafego='7')
        cria_ce(self.session, 'CCB', 'M1', {'CCAB': []}, tipo_trafego='3')
        cria_ce(self.session, 'CPS', 'M1', {'CPAS': []}, tipo_trafego='9')
        cria_ce(self.session, 'CNL', 'M1', {'CNUL': []}, tipo_trafego=None)
        self.session.commit()
        for tipo_janela in ('prevista', 'efetiva'):
            with self.subTest(tipo_janela=tipo_janela):
                conteineres = self.conteineres(tipo_janela)
                self.assertNotIn('CEXP', conteineres)
                self.assertTrue({'CCAB', 'CPAS', 'CNUL'} <= conteineres)
                self.assertEqual(self.total(tipo_janela), len(conteineres))

    def _navio_b_atraca_em_paranagua_no_periodo(self):
        # M2 (C4, C5): em Santos só em 02-20 (fora); em Paranaguá em 01-08 (dentro do período)
        self.session.add_all([
            Escala(numeroDaEscala='E4', dataPrevistaAtracacao='2026-01-08', embarcacao='NAVIO B',
                   portoDaEscala='BRPNG'),
            EscalaManifesto(ID=4, manifesto='M2', escala='E4'),
        ])
        self.session.commit()

    def test_sem_porto_qualquer_escala_do_manifesto_conta(self):
        self._navio_b_atraca_em_paranagua_no_periodo()
        self.assertEqual(self.conteineres('prevista'), {'C1', 'C2', 'C3', 'C6', 'C4', 'C5'})

    def test_lista_de_portos_de_atracacao(self):
        self._navio_b_atraca_em_paranagua_no_periodo()
        self.assertEqual(self.conteineres('prevista', porto_escala=['BRSSZ', 'BRPNG']),
                         {'C1', 'C2', 'C3', 'C6', 'C4', 'C5'})
        self.assertEqual(self.conteineres('prevista', porto_escala='BR'), {'C1', 'C2', 'C3', 'C6', 'C4', 'C5'})
        self.assertEqual(busca_manifestos_navii(self.session, ['M2'], ['BRPNG', 'BRSSZ'])['M2']
                         ['dataPrevistaAtracacao'], '2026-02-20')  # a maior data entre os portos

    def test_com_porto_so_conta_a_escala_naquele_porto(self):
        self._navio_b_atraca_em_paranagua_no_periodo()
        self.assertEqual(self.conteineres('prevista', porto_escala='BRSSZ'), {'C1', 'C2', 'C3', 'C6'})
        self.assertEqual(self.conteineres('prevista', porto_escala='BRPNG'), {'C4', 'C5'})
        # efetiva: C1 e C4 entram pelo desembarque (ciclo, independe do porto); C5, sem ciclo, só pela escala
        self.assertEqual(self.conteineres('efetiva', porto_escala='BRSSZ'), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres('efetiva', porto_escala='BRPNG'), {'C1', 'C4', 'C5'})

    def test_card_mostra_a_escala_do_porto_escolhido(self):
        self._navio_b_atraca_em_paranagua_no_periodo()
        r = next(r for r in busca_ces_navii(self.session, 'efetiva', {**PERIODO, 'porto_escala': 'BRPNG'}, limit=50)
                 if r.item.codigoConteiner == 'C5')
        self.assertEqual(r.data_prevista_atracacao, '2026-01-08')
        dados = busca_manifestos_navii(self.session, ['M2'], 'BRSSZ')['M2']
        self.assertEqual(dados['dataPrevistaAtracacao'], '2026-02-20')


class TestFiltros(NaviiSqliteTestCase):

    def test_somente_com_imagem(self):
        self.assertEqual(self.conteineres(com_imagem=True), {'C1'})
        self.assertEqual(self.conteineres(exigir_escaneamento=True), {'C1'})

    def test_somente_concluidos(self):
        self.assertEqual(self.conteineres(concluido=True), {'C1'})

    def test_ncm_por_prefixo_no_conteiner(self):
        self.assertEqual(self.conteineres(ncm='8471'), {'C1'})
        self.assertEqual(self.conteineres('prevista', ncm='8471'), {'C1', 'C3'})
        # 9503 está só no C2, não "vaza" para o C1 do mesmo CE
        self.assertEqual(self.conteineres(ncm='9503'), {'C2'})

    def test_entrada_do_usuario_normalizada(self):
        # NCM digitado com pontos e porto em minúsculas
        self.assertEqual(self.conteineres(ncm='8471.30'), {'C1'})
        self.assertEqual(self.conteineres(porto_escala='brssz'), self.conteineres(porto_escala='BRSSZ'))
        self.assertEqual(self.conteineres(porto_origem=' cnngb '), {'C4'})

    def test_produto_na_descricao_do_ce_sem_diferenciar_maiusculas(self):
        # vale para todos os contêineres do CE (a descrição é do conhecimento)
        self.assertEqual(self.conteineres(produto='brinq'), {'C1', 'C2'})
        self.assertEqual(self.conteineres(produto='T-SHIRT'), {'C4'})
        # o descritivo do NCM ("Camiseta") não é mais pesquisado
        self.assertEqual(self.conteineres(produto='camiseta'), set())

    def test_ncm_e_produto_combinados(self):
        # NCM do contêiner E descrição do CE
        self.assertEqual(self.conteineres(ncm='9503', produto='notebook'), {'C2'})
        self.assertEqual(self.conteineres(ncm='6109', produto='notebook'), set())

    def test_listas_valem_qualquer_um_dos_valores(self):
        self.assertEqual(self.conteineres(ncm=['8471', '6109']), {'C1', 'C4'})
        self.assertEqual(self.conteineres(ncm='8471, 6109'), {'C1', 'C4'})  # texto com vírgula
        self.assertEqual(self.conteineres(porto_origem=['CNNGB', 'US']), {'C4'})
        self.assertEqual(self.conteineres(porto_destino=['BRRIG', 'brssz']), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres(porto_baldeacao=['SGSIN', 'CNSHA']), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres(recinto=['8932000', '9999999']), {'C4'})
        self.assertEqual(self.total(ncm=['8471', '6109']), 2)

    def test_lista_vazia_nao_filtra(self):
        self.assertEqual(self.conteineres(ncm=[], porto_origem=' , '), self.conteineres())

    def test_normaliza_listas(self):
        self.assertEqual(normaliza_filtros({'ncm': '8471.30, 8471.30;6109', 'porto_origem': ['cnsha', 'CN'],
                                            'produto': 'x'}),
                         {'ncm': ['847130', '6109'], 'porto_origem': ['CNSHA', 'CN'], 'produto': 'x'})

    def test_portos_origem_destino(self):
        self.assertEqual(self.conteineres(porto_origem='CNNGB'), {'C4'})
        self.assertEqual(self.conteineres(porto_destino='BRRIG'), {'C6'})

    def test_porto_baldeacao_pelo_manifesto(self):
        self.assertEqual(self.conteineres(porto_baldeacao='SGSIN'), {'C4'})

    def test_porto_parcial_filtra_por_pais(self):
        # 2 primeiras letras do código do porto = país
        self.assertEqual(self.conteineres(porto_origem='CN'), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres(porto_origem='cn'), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres(porto_origem='CNN'), {'C4'})
        self.assertEqual(self.conteineres(porto_origem='US'), set())
        self.assertEqual(self.conteineres(porto_destino='BR'), {'C1', 'C2', 'C4', 'C6'})
        self.assertEqual(self.conteineres(porto_baldeacao='SG'), {'C4'})
        self.assertEqual(self.total(porto_origem='CN'), 4)

    def test_porto_parcial_nao_aceita_curinga(self):
        self.assertEqual(self.conteineres(porto_origem='%'), {'C1', 'C2', 'C4', 'C6'})  # vira prefixo vazio
        self.assertEqual(self.conteineres(porto_origem='_NN'), set())

    def test_recinto_de_desembarque_do_ciclo(self):
        # só pelo ciclo de vida (cod_siscomex): C2/C6, sem desembarque, ficam de fora
        self.assertEqual(self.conteineres(recinto='8931356'), {'C1', 'C4'})

    def test_recinto_de_armazenagem(self):
        self.assertEqual(self.conteineres(recinto='8932000'), {'C4'})

    def test_recinto_nao_usa_o_terminal_do_manifesto(self):
        self.assertEqual(self.conteineres(recinto='T01'), set())

    def test_item_sem_conteiner_carga_solta(self):
        # carga solta/granel: Item com codigoConteiner vazio e NCM declarado sem contêiner
        cria_ce(self.session, 'CES', 'M1', {'': [('10019900', 'Trigo a granel')]})
        self.session.commit()
        self.assertIn('', self.conteineres())
        self.assertEqual(self.conteineres(ncm='1001'), {''})
        self.assertNotIn('', self.conteineres(com_imagem=True))

    def test_empresa_embarcador_ou_consignatario(self):
        self.assertEqual(self.conteineres(empresa='acme'), {'C1', 'C2'})
        self.assertEqual(self.conteineres(empresa='11222333'), {'C1', 'C2'})
        self.assertEqual(self.conteineres(empresa='loja'), {'C4'})

    def test_ncms_max(self):
        # CE1 tem 3 NCMs distintos (nos dois contêineres); CE3 tem 1; CE6 nenhum
        self.assertEqual(self.conteineres(ncms_max=1), {'C4', 'C6'})
        self.assertEqual(self.conteineres(ncms_max=3), {'C1', 'C2', 'C4', 'C6'})

    def test_filtros_combinados(self):
        self.assertEqual(self.conteineres(empresa='acme', com_imagem=True), {'C1'})
        self.assertEqual(self.conteineres(recinto='8931356', ncm='6109'), {'C4'})
        self.assertEqual(self.conteineres(empresa='acme', porto_baldeacao='SGSIN'), set())

    def test_contagem_bate_com_os_resultados_de_cada_filtro(self):
        cenarios = [
            {}, {'com_imagem': True}, {'ncm': '8471'}, {'recinto': '8931356'},
            {'empresa': 'acme'}, {'ncms_max': 1}, {'porto_baldeacao': 'SGSIN'},
        ]
        for tipo_janela in ('efetiva', 'prevista'):
            for filtros in cenarios:
                with self.subTest(tipo_janela=tipo_janela, filtros=filtros):
                    self.assertEqual(self.total(tipo_janela, **filtros),
                                     len(self.conteineres(tipo_janela, **filtros)))


class TestPaginacao(NaviiSqliteTestCase):

    def test_paginas_percorrem_tudo_sem_repetir_em_ordem_decrescente(self):
        vistos, cursor = [], None
        while True:
            pagina = busca_ces_navii(self.session, 'efetiva', PERIODO, cursor_id=cursor, limit=1)
            if not pagina:
                break
            vistos.extend(r.item.ID for r in pagina)
            cursor = pagina[-1].item.ID
        self.assertEqual(len(vistos), len(set(vistos)))
        self.assertEqual(vistos, sorted(vistos, reverse=True))
        self.assertEqual(len(vistos), self.total())


class TestEnriquecimento(NaviiSqliteTestCase):

    def resultado(self, conteiner, tipo_janela='efetiva'):
        return next(r for r in busca_ces_navii(self.session, tipo_janela, PERIODO, limit=50)
                    if r.item.codigoConteiner == conteiner)

    def card(self, conteiner):
        resultados = busca_ces_navii(self.session, 'efetiva', PERIODO, limit=50)
        return next(c for c in monta_cards_navii(self.session, resultados)
                    if c['numeroConteiner'] == conteiner)

    def test_resultado_sem_ciclo_traz_manifesto_e_escala(self):
        r = self.resultado('C2')
        self.assertIsNone(r.ciclo)
        self.assertEqual(r.manifesto.numero, 'M1')
        self.assertEqual(r.embarcacao, 'NAVIO A')
        self.assertEqual(r.data_prevista_atracacao, '2026-01-10')

    def test_card_sem_ciclo(self):
        card = self.card('C2')
        self.assertFalse(card['cicloDeVidaLocalizado'])
        self.assertIsNone(card['ciclo_de_vida_id'])
        self.assertEqual(card['imagens_ids'], [])
        self.assertFalse(card['concluido'])
        self.assertIsNone(card['linhaDoTempo']['desembarque']['data'])
        self.assertEqual(card['terminalDescarregamento'], {'codigo': 'T01', 'nome': 'BTP'})
        self.assertEqual(card['dataPrevistaAtracacao'], '2026-01-10')
        self.assertIsNotNone(card['item_id'])

    def test_card_traz_nomes_de_portos_navio_e_posicao_ncm(self):
        card = self.card('C4')  # CE3: origem CNNGB, destino BRSSZ, manifesto M2 carregado em SGSIN
        self.assertEqual(card['nomePortoOrigemCarga'], 'Ningbo, China')
        self.assertEqual(card['nomePortoDestFinal'], 'Santos, Brasil')
        self.assertIn('Singap', card['manifesto']['nomePortoCarregamento'])
        self.assertIsNone(card['nomeEmbarcacao'])  # 'NAVIO B' não é um IMO conhecido
        ncm = card['conteineres'][0]['ncms'][0]
        self.assertEqual(ncm['descritivo'], 'Camiseta')
        self.assertIn('camisetas', ncm['descricaoPosicao'].lower())

    def test_card_com_ciclo_traz_nome_do_recinto_e_imagens(self):
        card = self.card('C1')
        self.assertTrue(card['cicloDeVidaLocalizado'])
        self.assertTrue(card['concluido'])
        self.assertEqual(len(card['imagens_ids']), 2)
        desembarque = card['linhaDoTempo']['desembarque']
        self.assertEqual(desembarque['recinto'], '8931356')
        self.assertEqual(desembarque['nomeRecinto'], 'BTP')
        self.assertEqual(card['nomeConsignatario'], 'IMPORTADORA ALFA LTDA')

    def test_card_armazenagem_e_escaneamento_sem_imagem(self):
        card = self.card('C4')
        self.assertEqual(card['linhaDoTempo']['entradaRecintoSeco']['nomeRecinto'], 'EADI SANTOS')
        self.assertEqual(card['imagens_ids'], [])  # escaneado, mas sem imagem

    def test_ciclo_mais_recente_prevalece(self):
        cria_ciclo(self.session, 'CE1', 'C1', datetime(2026, 1, 13), '8932000')
        self.session.commit()
        item = self.session.query(Item).filter(Item.codigoConteiner == 'C1').one()
        ciclo = busca_ciclos_navii(self.session, [item])[('CE1', 'C1')]
        self.assertEqual(ciclo.codigoRecintoDesembarque, '8932000')

    def test_manifesto_com_varias_escalas_usa_maior_data_prevista(self):
        self.session.add_all([
            Escala(numeroDaEscala='E3', dataPrevistaAtracacao='2026-01-12', embarcacao='NAVIO C'),
            EscalaManifesto(ID=3, manifesto='M1', escala='E3'),
        ])
        self.session.commit()
        dados = busca_manifestos_navii(self.session, ['M1'])['M1']
        self.assertEqual(dados['dataPrevistaAtracacao'], '2026-01-12')
        self.assertEqual(dados['embarcacao'], 'NAVIO C')

    def test_manifesto_inexistente(self):
        dados = busca_manifestos_navii(self.session, ['M9'])['M9']
        self.assertEqual(dados, {'manifesto': None, 'embarcacao': None, 'dataPrevistaAtracacao': None})

    def test_nomes_recintos_por_cod_carga(self):
        self.assertEqual(busca_nomes_recintos(self.session, ['T01', 'XX'], campo='cod_carga'), {'T01': 'BTP'})

    def test_nomes_recintos_lista_vazia(self):
        self.assertEqual(busca_nomes_recintos(self.session, [None, '']), {})


class TestFamiliaTiposDeBLETipoDeConteiner(NaviiSqliteTestCase):
    """Master CM (MBL) e filhotes CF (HBL, com o contêiner) e CF2 (irmão, sem o contêiner), no manifesto
    M1 (escala no período). C8 é open top (42U1, potencial OOG) e está nos itens do master e do filhote; o
    ciclo de vida foi gravado com o CE do MASTER (como o loop antigo faz em 36% dos casos)."""

    def setUp(self):
        super().setUp()
        self.session.add_all([
            Conhecimento(numeroCEmercante='CM', manifestoCE='M1', tipoBLConhecimento='11', tipoTrafego='5',
                         descricao='CONSOLIDADO'),
            Conhecimento(numeroCEmercante='CF', numeroCEMaster='CM', manifestoCE='M1', tipoBLConhecimento='12',
                         tipoTrafego='5', descricao='MERCADORIA DO FILHOTE'),
            Conhecimento(numeroCEmercante='CF2', numeroCEMaster='CM', manifestoCE='M1', tipoBLConhecimento='12',
                         tipoTrafego='5'),
            Item(numeroCEmercante='CM', codigoConteiner='C8', isoCode='42U1'),
            Item(numeroCEmercante='CF', codigoConteiner='C8', isoCode='42U1'),
        ])
        self.session.query(Item).filter(Item.codigoConteiner == 'C1').update({'isoCode': '45G1'})
        cria_ciclo(self.session, 'CM', 'C8', datetime(2026, 1, 11, 9, 0), '8931356',
                   imagens=['5f0000000000000000000008'])
        self.session.commit()

    def cards(self, **filtros):
        resultados = busca_ces_navii(self.session, 'efetiva', {**PERIODO, **filtros}, limit=50)
        return {(c['numeroCEmercante'], c['numeroConteiner']): c for c in monta_cards_navii(self.session, resultados)}

    def test_familia_com_irmaos(self):
        conhecimentos = self.session.query(Conhecimento).filter(Conhecimento.numeroCEmercante.in_(['CM', 'CF'])).all()
        familias = busca_familias_navii(self.session, conhecimentos)
        self.assertEqual(familias['CM'], {'master': None, 'filhotes': ['CF', 'CF2'], 'familia': {'CM', 'CF', 'CF2'}})
        self.assertEqual(familias['CF'], {'master': 'CM', 'filhotes': [], 'familia': {'CF', 'CM', 'CF2'}})

    def test_card_do_filhote_acha_o_ciclo_gravado_com_o_master(self):
        card = self.cards()[('CF', 'C8')]
        self.assertTrue(card['cicloDeVidaLocalizado'])
        self.assertEqual((card['cicloPorCE'], card['numeroCEMaster']), ('CM', 'CM'))
        self.assertEqual(card['imagens_ids'], ['5f0000000000000000000008'])
        self.assertIsNone(self.cards()[('CM', 'C8')]['cicloPorCE'])   # o do próprio CE

    def test_ciclo_do_proprio_ce_tem_preferencia(self):
        cria_ciclo(self.session, 'CF', 'C8', datetime(2026, 1, 10, 9, 0), '8932000')
        self.session.commit()
        item = self.session.query(Item).filter(Item.numeroCEmercante == 'CF').one()
        ciclo = busca_ciclos_navii(self.session, [item], {'CF': {'CF', 'CM', 'CF2'}})[('CF', 'C8')]
        self.assertEqual(ciclo.numeroCEmercante, 'CF')
        # sem a família, só o CE exato
        self.assertEqual(busca_ciclos_navii(self.session, [item]), {('CF', 'C8'): ciclo})

    def test_tipo_de_bl_com_sigla_descricao_e_filhotes(self):
        cards = self.cards()
        master, filhote = cards[('CM', 'C8')], cards[('CF', 'C8')]
        self.assertEqual((master['tipoBL'], master['tipoBLCodigo'], master['tipoBLDescricao']),
                         ('MBL', '11', 'master (consolida filhotes)'))
        self.assertEqual(master['filhotes'], ['CF', 'CF2'])
        self.assertEqual((filhote['tipoBL'], filhote['filhotes']), ('HBL', []))

    def test_tipo_do_conteiner_com_descricao_e_oog(self):
        cards = self.cards()
        self.assertEqual(cards[('CF', 'C8')]['tipoConteiner']['curta'], "40', open top")
        self.assertTrue(cards[('CF', 'C8')]['tipoConteiner']['oog'])
        self.assertFalse(cards[('CE1', 'C1')]['tipoConteiner']['oog'])
        self.assertIsNone(cards[('CE1', 'C2')]['tipoConteiner'])   # sem isoCode no Mercante
        conteiner = next(c for c in cards[('CE1', 'C1')]['conteineres'] if c['codigoConteiner'] == 'C1')
        self.assertEqual(conteiner['tipoConteiner']['curta'], "40' high cube, uso geral (dry)")

    def test_filtro_por_tipo_de_bl(self):
        self.assertEqual(self.conteineres(tipo_bl=['11']), {'C8'})
        self.assertEqual(self.total(tipo_bl='11'), 1)
        self.assertEqual(self.total(tipo_bl=['11', '12']), 6)   # C1, C2, C4, C6 (HBL) + C8 do master e do filhote
        self.assertEqual(self.total(tipo_bl=['10']), 0)

    def test_filtro_sem_oog(self):
        self.assertIn('C8', self.conteineres())
        self.assertNotIn('C8', self.conteineres(sem_oog=True))
        self.assertEqual(self.total(sem_oog=True), self.total() - 2)   # C8 do master e do filhote
        self.assertIn('C2', self.conteineres(sem_oog=True))            # sem tipo informado: não é OOG

    def test_normaliza_tipo_bl(self):
        self.assertEqual(normaliza_filtros({'tipo_bl': ' 11, x12 '})['tipo_bl'], ['11', '12'])


class TestCicloV2(NaviiSqliteTestCase):
    """Ciclo de vida v2: master VM e filhote VF, os dois com o contêiner C9 nos itens, no manifesto M2 (escala FORA
    do período). O ciclo foi gravado com o filhote (CE principal) e a família em `ciclodevida_ciclo_ce`:
    desembarque 12/01 na BTP, saída, entrada IMPLÍCITA no EADI (deduzida da inspeção), escaneado fora no
    Ecoporto (passagem antes do TRA, sem imagem) e no EADI (com imagem) — aberto, em recinto seco."""

    def setUp(self):
        super().setUp()
        self.session.add_all([
            Conhecimento(numeroCEmercante='VM', manifestoCE='M2', tipoBLConhecimento='11', tipoTrafego='5'),
            Conhecimento(numeroCEmercante='VF', numeroCEMaster='VM', manifestoCE='M2', tipoBLConhecimento='12',
                         tipoTrafego='5'),
            Item(numeroCEmercante='VM', codigoConteiner='C9'),
            Item(numeroCEmercante='VF', codigoConteiner='C9'),
        ])
        self.ciclo = CicloDeVidaImportacao(
            numeroCEmercante='VF', numeroConteiner='C9', data_desembarque=datetime(2026, 1, 12, 8),
            codigoRecintoDesembarque='8931356', data_saida=datetime(2026, 1, 12, 20),
            data_entrada_seco=datetime(2026, 1, 13, 9), codigoRecintoArmazenagem='8932000',
            entrada_seco_implicita_de='inspecao', concluido=False, situacao='aberto', fluxo='tra',
            motivo='no recinto seco', confianca_ce='alta', versao_heuristica='v2')
        self.ciclo.escaneamentos.extend([
            CicloDeVidaImportacaoEscaneamentos(
                escaneamento_id=91, data_escaneamento=datetime(2026, 1, 13, 10), imagem_id='5f0000000000000000000009',
                confianca='alta', codigoRecinto='8932000', fora=False, motivo='no recinto da etapa (entrada implícita)'),
            CicloDeVidaImportacaoEscaneamentos(
                escaneamento_id=90, data_escaneamento=datetime(2026, 1, 12, 22), confianca='media',
                codigoRecinto='8931318', fora=True,
                motivo='na passagem pelo operador 8931318 (escaneamento fora)'),
        ])
        self.session.add(self.ciclo)
        self.session.flush()
        self.session.add_all([
            CicloDeVidaCE(ciclo_de_vida_id=self.ciclo.ID, numeroCEmercante='VF', papel='filhote', principal=True,
                          confianca='alta'),
            CicloDeVidaCE(ciclo_de_vida_id=self.ciclo.ID, numeroCEmercante='VM', papel='master', principal=False,
                          confianca='alta'),
        ])
        self.session.commit()

    def pares(self, tipo_janela='efetiva', **filtros):
        resultado = busca_ces_navii(self.session, tipo_janela, {**PERIODO, **filtros}, limit=50)
        return {(r.item.numeroCEmercante, r.item.codigoConteiner) for r in resultado}

    def cards(self, **filtros):
        resultados = busca_ces_navii(self.session, 'efetiva', {**PERIODO, **filtros}, limit=50)
        return {(c['numeroCEmercante'], c['numeroConteiner']): c for c in monta_cards_navii(self.session, resultados)}

    def test_janela_efetiva_acha_o_card_do_master_pela_familia(self):
        self.assertTrue({('VM', 'C9'), ('VF', 'C9')} <= self.pares())
        self.assertEqual(self.total(), 6)
        with mock.patch.object(busca_navii, 'LIGACAO_FAMILIA', False):
            self.assertIn(('VF', 'C9'), self.pares())
            self.assertNotIn(('VM', 'C9'), self.pares())   # o ciclo é do filhote; a escala está fora

    def test_desembarque_pela_familia_fora_do_periodo_nao_cai_na_atracacao_prevista(self):
        self.session.query(Conhecimento).filter(Conhecimento.numeroCEmercante.in_(['VM', 'VF'])).update(
            {'manifestoCE': 'M1'}, synchronize_session=False)   # escala no período
        self.ciclo.data_desembarque = datetime(2026, 2, 25)
        self.session.commit()
        self.assertFalse({('VM', 'C9'), ('VF', 'C9')} & self.pares())
        with mock.patch.object(busca_navii, 'LIGACAO_FAMILIA', False):
            self.assertEqual({('VM', 'C9'), ('VF', 'C9')} & self.pares(), {('VM', 'C9')})

    def test_filtros_do_ciclo_valem_para_a_familia(self):
        self.assertEqual(self.pares(com_imagem=True), {('CE1', 'C1'), ('VM', 'C9'), ('VF', 'C9')})
        self.assertEqual(self.pares(recinto='8932000'), {('CE3', 'C4'), ('VM', 'C9'), ('VF', 'C9')})
        self.assertEqual(self.pares(estagio='no_seco'), {('VM', 'C9'), ('VF', 'C9')})
        self.assertEqual(self.total(com_imagem=True), 3)
        with mock.patch.object(busca_navii, 'LIGACAO_FAMILIA', False):
            self.assertEqual(self.pares(com_imagem=True), {('CE1', 'C1'), ('VF', 'C9')})

    def test_bl_unico_nao_consulta_a_familia(self):
        """BL único não tem master nem filhotes: nos filtros, só o próprio CE (atalho de desempenho)."""
        self.session.query(Conhecimento).filter(Conhecimento.numeroCEmercante == 'VM').update(
            {'tipoBLConhecimento': '10'}, synchronize_session=False)
        self.session.commit()
        self.assertEqual(self.pares(com_imagem=True), {('CE1', 'C1'), ('VF', 'C9')})
        self.session.query(Conhecimento).filter(Conhecimento.numeroCEmercante == 'VM').update(
            {'tipoBLConhecimento': None}, synchronize_session=False)   # sem tipo informado: consulta
        self.session.commit()
        self.assertEqual(self.pares(com_imagem=True), {('CE1', 'C1'), ('VM', 'C9'), ('VF', 'C9')})

    def test_estagios(self):
        self.assertEqual(self.pares(estagio=['entregue']), {('CE1', 'C1')})   # ciclo antigo concluído
        self.assertEqual(self.pares(concluido=True), {('CE1', 'C1')})          # filtro antigo = entregue
        self.assertEqual(self.pares(estagio=['entregue', 'no_seco']), {('CE1', 'C1'), ('VM', 'C9'), ('VF', 'C9')})
        self.assertEqual(self.pares(estagio=['no_operador', 'em_transito', 'embarcado', 'incompleto']), set())
        self.ciclo.data_entrada_seco = None
        self.session.commit()
        self.assertEqual(self.pares(estagio='em_transito'), {('VM', 'C9'), ('VF', 'C9')})

    def test_normaliza_estagio_e_filtro_antigo(self):
        self.assertEqual(normaliza_filtros({'estagio': 'NO_SECO, xyz'})['estagio'], ['no_seco'])
        self.assertNotIn('estagio', normaliza_filtros({'estagio': 'xyz'}))
        self.assertEqual(normaliza_filtros({'concluido': True}), {'estagio': ['entregue']})
        self.assertEqual(normaliza_filtros({'concluido': True, 'estagio': ['no_seco']})['estagio'],
                         ['no_seco', 'entregue'])
        self.assertEqual(normaliza_filtros({'concluido': False}), {})

    def test_card_v2_com_estagio_entrada_implicita_e_escaneamentos(self):
        card = self.cards()[('VM', 'C9')]
        self.assertEqual(card['cicloPorCE'], 'VF')
        self.assertEqual((card['estagio']['codigo'], card['estagio']['rotulo'], card['concluido']),
                         ('no_seco', 'Em recinto seco', False))
        self.assertEqual((card['situacao'], card['fluxo'], card['motivo']), ('aberto', 'tra', 'no recinto seco'))
        self.assertEqual(card['linhaDoTempo']['entradaRecintoSeco']['implicita'], 'inspecao')
        self.assertIsNone(card['linhaDoTempo']['embarque']['data'])
        fora, eadi = card['escaneamentos']   # em ordem de data
        self.assertEqual((fora['recinto'], fora['confianca'], fora['fora'], fora['temImagem']),
                         ('8931318', 'media', True, False))
        self.assertEqual((eadi['nomeRecinto'], eadi['confianca'], eadi['fora'], eadi['temImagem']),
                         ('EADI SANTOS', 'alta', False, True))
        self.assertEqual(card['imagens_ids'], ['5f0000000000000000000009'])

    def test_card_do_ciclo_antigo(self):
        card = self.cards()[('CE1', 'C1')]
        self.assertEqual((card['estagio']['codigo'], card['concluido']), ('entregue', True))
        self.assertEqual((card['situacao'], card['motivo']), (None, None))
        self.assertEqual([e['confianca'] for e in card['escaneamentos']], [None, None])
        self.assertIsNone(self.cards()[('CE1', 'C2')]['estagio'])   # sem ciclo

    def test_card_com_embarque(self):
        self.ciclo.situacao, self.ciclo.fluxo, self.ciclo.data_embarque = 'concluido', 'passagem', datetime(2026, 1, 14)
        self.ciclo.data_entrada_seco = None
        self.session.commit()
        card = self.cards()[('VF', 'C9')]
        self.assertEqual((card['estagio']['codigo'], card['concluido']), ('embarcado', False))
        self.assertEqual(card['linhaDoTempo']['embarque']['data'], datetime(2026, 1, 14))


class TestTabelaRecintos(unittest.TestCase):
    """A tabela leve do NAVI-I tem que continuar batendo com o model real do bhadrasana2."""

    def test_colunas_existem_no_model_do_bhadrasana(self):
        from bhadrasana.models.ovr import Recinto  # só a tabela: não configura mappers
        colunas_reais = {c.name: c for c in Recinto.__table__.columns}
        self.assertEqual(tabela_recintos.name, Recinto.__table__.name)
        for coluna in tabela_recintos.columns:
            with self.subTest(coluna=coluna.name):
                self.assertIn(coluna.name, colunas_reais)


class TestApiCes(NaviiSqliteTestCase):
    """Endpoint JSON /navii/api/ces com login desativado."""

    def setUp(self):
        super().setUp()
        app = Flask(__name__)
        app.config.update(LOGIN_DISABLED=True, db_session=self.session, TESTING=True)
        app.register_blueprint(bp_navii)
        self.client = app.test_client()

    def get(self, **params):
        return self.client.get('/navii/api/ces', query_string={**PERIODO, **params})

    def test_primeira_pagina_traz_total(self):
        dados = self.get(limit=2).get_json()
        self.assertEqual(dados['total'], 4)
        self.assertEqual(len(dados['cards']), 2)
        self.assertEqual(dados['next_cursor'], dados['cards'][-1]['item_id'])

    def test_paginas_seguintes_nao_recontam(self):
        primeira = self.get(limit=2).get_json()
        segunda = self.get(limit=2, cursor_id=primeira['next_cursor']).get_json()
        self.assertIsNone(segunda['total'])
        ids = [c['item_id'] for c in primeira['cards'] + segunda['cards']]
        self.assertEqual(len(set(ids)), 4)

    def test_filtro_com_imagem(self):
        dados = self.get(com_imagem='1').get_json()
        self.assertEqual(dados['total'], 1)
        self.assertEqual(dados['cards'][0]['numeroConteiner'], 'C1')
        self.assertEqual(dados['cards'][0]['linhaDoTempo']['desembarque']['data'], '2026-01-11 08:00:00')
        self.assertEqual(dados['cards'][0]['escaneamentos'][0]['data'], '2026-01-11 10:00:00')   # dentro da lista

    def test_filtro_porto_escala(self):
        self.assertEqual(self.get(porto_escala='BRSSZ').get_json()['total'], 4)
        self.assertEqual(self.get(porto_escala='BRPNG').get_json()['total'], 2)  # C1 e C4, pelo desembarque

    def test_filtro_em_lista_repetido_ou_com_virgula(self):
        self.assertEqual(self.client.get('/navii/api/ces', query_string=[
            ('data_inicio', PERIODO['data_inicio']), ('data_fim', PERIODO['data_fim']),
            ('ncm', '8471'), ('ncm', '6109')]).get_json()['total'], 2)
        self.assertEqual(self.get(ncm='8471,6109').get_json()['total'], 2)

    def test_janela_invalida_400(self):
        self.assertEqual(self.get(tipo_janela='xyz').status_code, 400)

    def test_estagio_e_concluido_antigo_pela_api(self):
        dados = self.get(estagio='entregue').get_json()
        self.assertEqual((dados['total'], dados['cards'][0]['estagio']['rotulo']), (1, 'Entregue'))
        self.assertEqual(self.get(concluido='1').get_json()['total'], 1)
        self.assertEqual(self.client.get('/navii/api/ces', query_string=[
            ('data_inicio', PERIODO['data_inicio']), ('data_fim', PERIODO['data_fim']),
            ('estagio', 'entregue'), ('estagio', 'no_seco')]).get_json()['total'], 1)

    def test_filtros_tipo_bl_e_sem_oog_pela_api(self):
        self.session.add(Conhecimento(numeroCEmercante='CM', manifestoCE='M1', tipoBLConhecimento='11',
                                      tipoTrafego='5'))
        self.session.add(Item(numeroCEmercante='CM', codigoConteiner='C8', isoCode='22U1'))
        self.session.commit()
        dados = self.get(tipo_bl='11').get_json()
        self.assertEqual((dados['total'], dados['cards'][0]['tipoBL']), (1, 'MBL'))
        self.assertEqual(self.get(tipo_bl='11', sem_oog='1').get_json()['total'], 0)
        self.assertTrue(dados['cards'][0]['tipoConteiner']['oog'])


if __name__ == '__main__':
    unittest.main()
