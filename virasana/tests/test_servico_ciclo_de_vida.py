"""Testes do SERVIÇO do Ciclo de Vida v2 (servico_ciclo_de_vida.py, gravacao_ciclo.py, imagens_ciclo.py).

Cenário (SQLite + Mongo simulado; agora = 20/03/2026 00:00; marcas iniciais desde 25/02/2026):
  CTRA: desembarque na BTP (101, declara o MASTER M; 45G1; escala E1), inspeção (301, declara o filhote H1;
        imagem no Mongo +3 h), saída (201), entrada no EADI (202) -> TRA aberto. Já tem a linha 77 do loop
        antigo (CE master, concluído, escaneamento 999 que a v2 não liga) -> vira v2 na mesma linha
  CTRB: desembarque no ECOPORTO (102, open top 42U1) e saída (203) -> entregue pelo operador, potencial OOG
  CTRV: desembarque na BTP (103), vazio no manifesto MANV (escala no dia) -> vazio, aberto no operador
  CTRC: desembarque (105) e saída 4 h antes de agora (204) -> aberto, aguardando entrada em seco
  CTRX: desembarque no Rio (104, fora do escopo) -> sem ciclo
  CTRO: ciclo antigo PENDENTE (linha 78) de 2025: desembarque 90 e saída 190, fora da janela das marcas
  Acesso 205 é agendamento (operação G): não conta
"""
import re
import unittest
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import mongomock
from bson import ObjectId
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.pool import StaticPool

from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import CEsResolvidos
from virasana.analytic_scripts.ciclo_de_vida.imagens_ciclo import escolhe_imagem
from virasana.integracao.mercante.mercantealchemy import (
    Conhecimento, ConteinerVazio, Escala, EscalaManifesto, Item, Manifesto,
)
from virasana.navii.busca_navii import tabela_recintos

from bhadrasana.models.apirecintos import (  # noqa: I100
    AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva, PesagemVeiculo,
)

AGORA = datetime(2026, 3, 20)
DESDE = datetime(2026, 2, 25)
BTP, ECO, EADI, RIO = '8931359', '8931318', '8932000', '8911101'
M, H1 = '152600000000001', '152600000000002'
IMAGEM = ObjectId()


def d(dia, hora=8, minuto=0, mes=3, ano=2026):
    return datetime(ano, mes, dia, hora, minuto)


def _insere(conexao, modelo, linhas):
    for linha in linhas:  # uma a uma: nem todas têm as mesmas colunas
        conexao.execute(modelo.__table__.insert(), linha)


def _evento(id_, conteiner, data, recinto, ce=None, **campos):
    return dict(id=id_, numeroConteiner=conteiner, dataHoraOcorrencia=data, codigoRecinto=recinto,
                tipoOperacao='I', **({'numeroConhecimento': ce} if ce else {}), **campos)


def monta_banco():
    engine = create_engine('sqlite://', poolclass=StaticPool, connect_args={'check_same_thread': False})
    for modelo in (Conhecimento, Item, Escala, EscalaManifesto, Manifesto, ConteinerVazio, EmbarqueDesembarque,
                   AcessoVeiculo, InspecaoNaoInvasiva, PesagemVeiculo):
        modelo.__table__.create(engine)
    tabela_recintos.create(engine)
    gc.metadata.create_all(engine)
    with engine.begin() as c:
        c.execute(text('ALTER TABLE ovr_recintos ADD COLUMN e_operador BOOLEAN'))
        c.execute(text('ALTER TABLE ovr_recintos ADD COLUMN cod_unidade VARCHAR(7)'))
        c.execute(text("INSERT INTO ovr_recintos (id, nome, cod_siscomex, e_operador, cod_unidade) VALUES "
                       "(1, 'BTP', '8931359', 1, '0817800'), (2, 'ECOPORTO', '8931318', 1, '0817800'), "
                       "(3, 'EADI', '8932000', 0, '0817800'), (4, 'MULTIRIO', '8911101', 1, '0717600')"))
        _insere(c, Conhecimento, [
            dict(numeroCEmercante=M, manifestoCE='MAN1', tipoTrafego='5', tipoBLConhecimento='11',
                 portoDestFinal='BRSSZ'),
            dict(numeroCEmercante=H1, numeroCEMaster=M, manifestoCE='MAN1', tipoTrafego='5',
                 tipoBLConhecimento='12', portoDestFinal='BRSSZ')])
        _insere(c, Item, [dict(numeroCEmercante=M, codigoConteiner='CTRA'),
                          dict(numeroCEmercante=H1, codigoConteiner='CTRA')])
        _insere(c, Manifesto, [dict(numero='MAN1'), dict(numero='MANV')])
        _insere(c, Escala, [dict(numeroDaEscala='E1', dataPrevistaAtracacao='2026-03-01', portoDaEscala='BRSSZ'),
                            dict(numeroDaEscala='E4', dataPrevistaAtracacao='2026-03-10', portoDaEscala='BRSSZ')])
        _insere(c, EscalaManifesto, [dict(ID=1, manifesto='MAN1', escala='E1'),
                                     dict(ID=2, manifesto='MANV', escala='E4')])
        _insere(c, ConteinerVazio, [dict(manifesto='MANV', idConteinerVazio='CTRV')])
        desembarque = dict(embarqueDesembarque='D')
        _insere(c, EmbarqueDesembarque, [
            _evento(90, 'CTRO', d(1, mes=10, ano=2025), BTP, **desembarque),
            _evento(101, 'CTRA', d(2), BTP, M, pesoBrutoBalanca=25000, taraConteiner=3800, pesoBrutoManifesto=21000,
                    tipoConteiner='45G1', escala='E1', **desembarque),
            _evento(102, 'CTRB', d(5), ECO, tipoConteiner='42U1', **desembarque),
            _evento(103, 'CTRV', d(10), BTP, tipoConteiner='22G1', **desembarque),
            _evento(104, 'CTRX', d(6), RIO, **desembarque),
            _evento(105, 'CTRC', d(15), BTP, **desembarque)])
        acesso = dict(operacao='C', vazioConteiner=False)
        _insere(c, AcessoVeiculo, [
            _evento(190, 'CTRO', d(3, mes=10, ano=2025), BTP, direcao='S', **acesso),
            _evento(201, 'CTRA', d(4), BTP, H1, direcao='S', **acesso),
            _evento(202, 'CTRA', d(4, 11), EADI, direcao='E', **acesso),
            _evento(203, 'CTRB', d(6), ECO, direcao='S', **acesso),
            _evento(204, 'CTRC', d(19, 20), BTP, direcao='S', **acesso),
            _evento(205, 'CTRA', d(3), BTP, direcao='S', operacao='G', vazioConteiner=False)])
        _insere(c, InspecaoNaoInvasiva, [_evento(301, 'CTRA', d(2, 12), BTP, H1, vazio=False)])
        _insere(c, PesagemVeiculo, [_evento(401, 'CTRV', d(10, 9), BTP)])
        # linhas do loop antigo: CTRA (concluído, escaneamento 999) e CTRO (pendente, de 2025)
        for linha in (dict(ID=77, desembarque_id=101, numeroConteiner='CTRA', numeroCEmercante=M,
                           data_desembarque=d(2), codigoRecintoDesembarque=BTP, concluido=True, saida_id=201,
                           data_saida=d(4)),
                      dict(ID=78, desembarque_id=90, numeroConteiner='CTRO', data_desembarque=d(1, mes=10, ano=2025),
                           codigoRecintoDesembarque=BTP, concluido=False)):
            c.execute(gc.CICLOS.insert(), linha)
        c.execute(gc.ESCANEAMENTOS.insert(), [dict(ID=7, ciclo_de_vida_id=77, escaneamento_id=999,
                                                   data_escaneamento=d(2))])
    mongo = mongomock.MongoClient()['teste']
    mongo['fs.files'].insert_one({'_id': IMAGEM, 'metadata': {'numeroinformado': 'CTRA',
                                                              'dataescaneamento': d(2, 15, 2)}})
    return engine, mongo


class Relogio:
    def __init__(self, agora):
        self.agora = agora

    def __call__(self):
        return self.agora


class ServicoTestCase(unittest.TestCase):
    """Banco montado e uma primeira volta já feita (gravando)."""

    def setUp(self):
        self.engine, self.mongo = monta_banco()
        self.relogio = Relogio(AGORA)
        self.servico = sv.Servico(self.engine, self.mongo, relogio=self.relogio)
        self.estatisticas, self.cheio = self.servico.uma_volta(DESDE)

    def ciclo(self, conteiner) -> dict:
        with self.engine.connect() as c:
            linha = c.execute(select(gc.CICLOS).where(gc.CICLOS.c.numeroConteiner == conteiner)).mappings().one()
        return dict(linha)

    def tabela(self, tabela, *criterios):
        with self.engine.connect() as c:
            return [dict(linha) for linha in c.execute(select(tabela).where(*criterios)).mappings()]


class TestEtapaRisco(ServicoTestCase):
    """Etapa do risco histórico por CE no fim da volta: erro nela não derruba o ciclo de vida."""

    def test_erro_na_etapa_do_risco_nao_derruba_o_ciclo(self):
        from unittest import mock
        servico = sv.Servico(self.engine, self.mongo, relogio=self.relogio, risco=True)
        self.assertIsNotNone(servico.etapa_risco)
        servico.etapa_risco = mock.Mock(executa=mock.Mock(side_effect=RuntimeError('tabela não existe')))
        with self.assertLogs('ciclo_de_vida_v2', 'ERROR'):
            servico.uma_volta(DESDE)
        servico.etapa_risco.executa.assert_called_once()

    def test_desligada_por_padrao_e_na_simulacao(self):
        self.assertIsNone(self.servico.etapa_risco)
        self.assertIsNone(sv.Servico(self.engine, self.mongo, simula=True, risco=True).etapa_risco)
        self.assertTrue(sv._argumentos(['--sem-risco']).sem_risco)


class TestPrimeiraVolta(ServicoTestCase):

    def test_cria_os_novos_e_atualiza_a_linha_do_loop_antigo(self):
        self.assertEqual((self.estatisticas['ciclos inseridos'], self.estatisticas['ciclos atualizados']), (3, 1))
        self.assertEqual(self.estatisticas['contêineres com evento novo'], 5)   # CTRA, CTRB, CTRV, CTRX, CTRC
        self.assertFalse(self.cheio)
        self.assertEqual(self.ciclo('CTRA')['ID'], 77)                        # mesma linha
        self.assertEqual(self.tabela(gc.CICLOS, gc.CICLOS.c.numeroConteiner == 'CTRX'), [])   # Rio: fora
        self.assertIsNone(self.ciclo('CTRO')['versao_heuristica'])            # fora da janela: intocado

    def test_tra_com_o_filhote_como_ce_principal(self):
        ciclo = self.ciclo('CTRA')
        esperado = {'versao_heuristica': 'v2', 'situacao': ldt.ABERTO, 'fluxo': ldt.TRA, 'concluido': False,
                    'numeroCEmercante': H1, 'confianca_ce': ldt.ALTA, 'portoDestFinal': 'BRSSZ',
                    'saida_id': 201, 'entrada_seco_id': 202, 'codigoRecintoArmazenagem': EADI,
                    'data_entrada_seco': d(4, 11), 'saida_seco_id': None, 'tipoConteiner': '45G1', 'oog': False,
                    'escala': 'E1', 'vazio': False, 'processado_em': AGORA}
        self.assertEqual({k: ciclo[k] for k in esperado}, esperado)
        self.assertEqual((float(ciclo['peso_balanca']), float(ciclo['tara']), float(ciclo['peso_menos_tara'])),
                         (25000, 3800, 21200))

    def test_escaneamentos_com_confianca_e_imagem(self):
        [esc] = self.tabela(gc.ESCANEAMENTOS, gc.ESCANEAMENTOS.c.ciclo_de_vida_id == 77)
        self.assertEqual((esc['escaneamento_id'], esc['confianca'], esc['codigoRecinto'], esc['fora']),
                         (301, ldt.ALTA, BTP, False))
        self.assertEqual(esc['imagem_id'], str(IMAGEM))           # BTP: imagem 3 h depois da inspeção
        self.assertIn('CE declarado da família', esc['motivo'])
        self.assertEqual(self.estatisticas['escaneamentos removidos'], 1)   # o 999 do loop antigo

    def test_familia_do_ce_gravada(self):
        ces = {(c['numeroCEmercante'], c['papel'], c['principal'])
               for c in self.tabela(gc.CICLO_CE, gc.CICLO_CE.c.ciclo_de_vida_id == 77)}
        self.assertEqual(ces, {(H1, 'filhote', True), (M, 'master', False)})

    def test_ecoporto_entregue_e_potencial_oog(self):
        ciclo = self.ciclo('CTRB')
        self.assertEqual((ciclo['situacao'], ciclo['fluxo'], ciclo['concluido'], ciclo['saida_id']),
                         (ldt.CONCLUIDO, ldt.OPERADOR, True, 203))
        self.assertEqual((ciclo['tipoConteiner'], ciclo['oog']), ('42U1', True))

    def test_vazio_com_manifesto(self):
        ciclo = self.ciclo('CTRV')
        self.assertEqual((ciclo['vazio'], ciclo['numeroManifesto'], ciclo['numeroCEmercante'], ciclo['situacao']),
                         (True, 'MANV', None, ldt.ABERTO))

    def test_marcas_dagua(self):
        marcas = {m['tabela']: m['ultimo_id'] for m in self.tabela(gc.MARCAS)}
        self.assertEqual(marcas, {'apirecintos_embarquedesembarque': 105, 'apirecintos_acessosveiculo': 204,
                                  'apirecintos_inspecoesnaoinvasivas': 301, 'apirecintos_pesagensveiculo': 401})


class TestVoltasSeguintes(ServicoTestCase):

    def test_sem_novidade_nao_processa_nada(self):
        estatisticas, _ = self.servico.uma_volta(DESDE)
        self.assertEqual(estatisticas['contêineres'], 0)

    def test_evento_novo_conclui_o_tra(self):
        with self.engine.begin() as c:
            _insere(c, AcessoVeiculo, [_evento(206, 'CTRA', d(16), EADI, direcao='S', operacao='C',
                                               vazioConteiner=False)])
        estatisticas, _ = self.servico.uma_volta(DESDE)
        self.assertEqual(estatisticas['contêineres com evento novo'], 1)
        ciclo = self.ciclo('CTRA')
        self.assertEqual((ciclo['situacao'], ciclo['saida_seco_id'], ciclo['concluido']), (ldt.CONCLUIDO, 206, True))
        [esc] = self.tabela(gc.ESCANEAMENTOS, gc.ESCANEAMENTOS.c.ciclo_de_vida_id == 77)
        self.assertEqual(esc['imagem_id'], str(IMAGEM))           # mantida
        marcas = {m['tabela']: m['ultimo_id'] for m in self.tabela(gc.MARCAS)}
        self.assertEqual(marcas['apirecintos_acessosveiculo'], 206)

    def test_revisita_fecha_por_prazo(self):
        self.relogio.agora = AGORA + timedelta(days=3)
        estatisticas, _ = self.servico.uma_volta(DESDE)
        # abertos (CTRA, CTRV, CTRC) e o concluído recente SEM CE (CTRB: o CE pode chegar depois ao Mercante)
        self.assertEqual(estatisticas['contêineres revisitados'], 4)
        ciclo = self.ciclo('CTRC')
        self.assertEqual((ciclo['situacao'], ciclo['motivo']), (ldt.CONCLUIDO, 'entregue pelo operador'))
        self.assertEqual(self.ciclo('CTRB')['processado_em'], self.relogio.agora)
        estatisticas, _ = self.servico.uma_volta(DESDE)   # acabaram de ser processados: não revisita de novo
        self.assertEqual(estatisticas['contêineres revisitados'], 0)

    def test_sem_ce_revisita_com_intervalo_maior(self):
        self.relogio.agora = AGORA + timedelta(hours=12)   # abertos: sim (> 6 h); sem CE: ainda não (< 24 h)
        self.servico.uma_volta(DESDE)
        self.assertEqual(self.ciclo('CTRB')['processado_em'], AGORA)
        self.assertEqual(self.ciclo('CTRC')['processado_em'], AGORA + timedelta(hours=12))

    def test_remocao_a_bordo_apaga_o_ciclo_do_novo_desembarque(self):
        """CTRV (no operador BTP desde 10/03): embarque e novo desembarque na BTP 6 h depois — o loop antigo já
        tinha criado uma linha para o novo desembarque (ID 500), que é apagada; o ciclo do 1º desembarque continua."""
        with self.engine.begin() as c:
            c.execute(text("INSERT INTO apirecintos_embarquedesembarque (id, numeroConteiner, dataHoraOcorrencia, "
                           "codigoRecinto, tipoOperacao, embarqueDesembarque) VALUES "
                           "(106, 'CTRV', '2026-03-12 08:00:00', '8931359', 'I', 'E'), "
                           "(107, 'CTRV', '2026-03-12 14:00:00', '8931359', 'I', 'D')"))
            c.execute(gc.CICLOS.insert(), dict(ID=500, desembarque_id=107, numeroConteiner='CTRV',
                                               data_desembarque=d(12, 14), codigoRecintoDesembarque=BTP,
                                               concluido=False))
        self.relogio.agora = AGORA + timedelta(hours=1)
        estatisticas, _ = self.servico.uma_volta(DESDE)
        self.assertEqual(estatisticas['ciclos de remoção a bordo apagados'], 1)
        ciclo = self.ciclo('CTRV')   # um só
        self.assertEqual((ciclo['desembarque_id'], ciclo['situacao'], ciclo['embarque_id']), (103, 'aberto', None))

    def test_aberto_alem_da_janela_de_eventos_continua_sendo_refeito(self):
        """O CTRA (aberto no EADI, desembarque em 02/03) com uma janela de eventos de só 10 dias: a carga recua até
        o desembarque do ciclo aberto — senão ele nunca mais seria refeito."""
        self.relogio.agora = AGORA + timedelta(hours=7)
        servico = sv.Servico(self.engine, self.mongo, dias_eventos=10, relogio=self.relogio)
        with self.engine.connect() as c:
            self.assertEqual(servico._desde_do_lote(c, ['CTRA', 'CTRB'], None), d(2))
            self.assertEqual(servico._desde_do_lote(c, ['CTRB'], None), self.relogio.agora - timedelta(days=10))
        estatisticas = servico.processa(['CTRA'])
        self.assertEqual(estatisticas['ciclos atualizados'], 1)
        ciclo = self.ciclo('CTRA')
        self.assertEqual((ciclo['processado_em'], ciclo['situacao'], ciclo['ID']), (self.relogio.agora, 'aberto', 77))
        limitado = sv.Servico(self.engine, self.mongo, dias_eventos=10, dias_maximos_aberto=15, relogio=self.relogio)
        with self.engine.connect() as c:
            self.assertEqual(limitado._desde_do_lote(c, ['CTRA'], None), self.relogio.agora - timedelta(days=15))

    def test_refaz_periodo_inclusive_concluidos(self):
        """--refaz-desde (15/10/2026): depois de mudar o núcleo, os ciclos CONCLUÍDOS do período são refeitos uma vez
        (o serviço contínuo não os revisita). O CTRB (entregue pelo Ecoporto) é "estragado" e volta ao certo."""
        with self.engine.begin() as c:
            c.execute(gc.CICLOS.update().where(gc.CICLOS.c.numeroConteiner == 'CTRB').values(fluxo='baldeacao',
                                                                                           motivo='antigo'))
        with self.engine.connect() as c:
            self.assertEqual(self.servico.conteineres_do_periodo(c, d(1), d(10)), ['CTRB', 'CTRA'])
        estatisticas = self.servico.refaz(d(1), d(10))
        self.assertEqual((estatisticas['contêineres'], estatisticas['ciclos atualizados']), (2, 2))
        self.assertEqual((self.ciclo('CTRB')['fluxo'], self.ciclo('CTRB')['situacao']), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertEqual(sv._argumentos(['--refaz-desde', '2026-09-01']).refaz_desde, datetime(2026, 9, 1))

    def test_refaz_cria_ciclo_de_desembarque_sem_ciclo(self):
        """Sombra de 15/10/2026: desembarques no Ecoporto de antes do serviço ficaram sem ciclo (o loop antigo não os
        criava) e o refaz só pegava contêiner com ciclo. Agora pega também o desembarque no escopo sem ciclo; o de
        fora do escopo (CTRX, no Rio, em 06/03) continua de fora."""
        with self.engine.begin() as c:
            c.execute(gc.CICLOS.delete().where(gc.CICLOS.c.numeroConteiner == 'CTRB'))
        with self.engine.connect() as c:
            self.assertEqual(self.servico.conteineres_do_periodo(c, d(1), d(10)), ['CTRB', 'CTRA'])
        estatisticas = self.servico.refaz(d(1), d(10))
        self.assertEqual((estatisticas['contêineres'], estatisticas['ciclos inseridos']), (2, 1))
        self.assertEqual((self.ciclo('CTRB')['fluxo'], self.ciclo('CTRB')['situacao']), (ldt.OPERADOR, ldt.CONCLUIDO))

    def test_escopo_com_operadores_fora_de_santos(self):
        """Ampliação do escopo (20/10/2026): lista EXPLÍCITA; só entra quem também é e_operador no cadastro."""
        from unittest import mock
        from virasana.analytic_scripts.ciclo_de_vida import carga_ciclo
        self.assertEqual(carga_ciclo.OPERADORES_FORA_DE_SANTOS, {'7921303', '4931303', '3111401'})   # sem ICTSI
        with self.engine.connect() as c:
            consulta = sv.ConsultaSQL(c)
            self.assertEqual(carga_ciclo.CargaCiclo(consulta, d(1), d(10)).operadores()[0], {BTP, ECO})
            with mock.patch.object(carga_ciclo, 'OPERADORES_FORA_DE_SANTOS', frozenset({RIO, EADI})):
                self.assertEqual(carga_ciclo.CargaCiclo(consulta, d(1), d(10)).operadores()[0], {BTP, ECO, RIO})

    def test_refaz_so_os_operadores_novos(self):
        """--refaz-operadores: ao ampliar o escopo, cria os ciclos do Rio (CTRX) sem refazer os de Santos."""
        from unittest import mock
        from virasana.analytic_scripts.ciclo_de_vida import carga_ciclo
        with self.engine.connect() as c:
            self.assertEqual(self.servico.conteineres_do_periodo(c, d(1), d(10), [RIO]), [])   # fora do escopo
        with mock.patch.object(carga_ciclo, 'OPERADORES_FORA_DE_SANTOS', frozenset({RIO})):
            with self.engine.connect() as c:
                self.assertEqual(self.servico.conteineres_do_periodo(c, d(1), d(10), [RIO]), ['CTRX'])
            estatisticas = self.servico.refaz(d(1), d(10), operadores=[RIO])
        self.assertEqual((estatisticas['contêineres'], estatisticas['ciclos inseridos']), (1, 1))
        self.assertEqual(self.ciclo('CTRX')['codigoRecintoDesembarque'], RIO)
        self.assertEqual(sv._argumentos(['--refaz-desde', '2026-07-19', '--refaz-operadores', '7921303', '4931303'])
                         .refaz_operadores, ['7921303', '4931303'])

    def test_refaz_segue_quando_uma_parte_falha(self):
        from unittest import mock
        with mock.patch.object(self.servico, 'processa', side_effect=[RuntimeError('chave duplicada'), Counter()]) \
                as processa, self.assertLogs('ciclo_de_vida_v2', 'ERROR'):
            estatisticas = self.servico.refaz(d(1), d(10), parte=1)
        self.assertEqual((estatisticas['partes com erro'], processa.call_count), (1, 2))

    def test_reprocessa_pendente_antigo(self):
        estatisticas = self.servico.reprocessa(10)
        ciclo = self.ciclo('CTRO')
        self.assertEqual((ciclo['ID'], ciclo['versao_heuristica'], ciclo['situacao'], ciclo['saida_id']),
                         (78, 'v2', ldt.CONCLUIDO, 190))
        self.assertEqual(estatisticas['pendentes antigos restantes'], 0)
        self.assertEqual(self.servico.reprocessa(10), {})


class TestSimulacao(unittest.TestCase):

    def test_simula_nao_grava_nada(self):
        engine, mongo = monta_banco()
        estatisticas, _ = sv.Servico(engine, mongo, simula=True, relogio=Relogio(AGORA)).uma_volta(DESDE)
        self.assertEqual((estatisticas['ciclos inseridos'], estatisticas['ciclos atualizados']), (3, 1))
        with engine.connect() as c:
            self.assertEqual(c.execute(select(func.count()).select_from(gc.CICLOS)).scalar(), 2)
            self.assertEqual(c.execute(select(func.count()).select_from(gc.MARCAS)).scalar(), 0)
            self.assertIsNone(c.execute(select(gc.CICLOS.c.versao_heuristica).where(gc.CICLOS.c.ID == 77)).scalar())

    def test_lote_cheio_pede_nova_volta_logo(self):
        engine, mongo = monta_banco()
        servico = sv.Servico(engine, mongo, lote_eventos=2, relogio=Relogio(AGORA))
        _, cheio = servico.uma_volta(DESDE)
        self.assertTrue(cheio)


class TestFuncoes(unittest.TestCase):
    DESEMBARQUE = ldt.Evento(ldt.DESEMBARQUE, 1, d(1), BTP)

    def test_ligacao_de_confianca_baixa_nao_e_gravada(self):
        ciclo = ldt.CicloCalculado(self.DESEMBARQUE, saida=ldt.Ligacao(ldt.Evento(ldt.SAIDA, 2, d(2), BTP), ldt.BAIXA),
                                   situacao=ldt.ABERTO)
        ciclo.escaneamentos = [ldt.Ligacao(ldt.Evento(ldt.INSPECAO, 3, d(1, 9), BTP), ldt.BAIXA),
                               ldt.Ligacao(ldt.Evento(ldt.INSPECAO, 4, d(1, 10), ECO), ldt.MEDIA,
                                           ['na passagem pelo operador 8931318 (escaneamento fora)'])]
        linha = gc.linha_ciclo('C', ciclo, CEsResolvidos(), {}, {}, None, None, AGORA)
        self.assertEqual((linha['saida_id'], linha['data_saida'], linha['concluido']), (None, None, False))
        escaneamentos = gc.linhas_escaneamentos(ciclo)
        self.assertEqual(list(escaneamentos), [4])
        self.assertTrue(escaneamentos[4]['fora'])

    def test_entrada_implicita_e_peso_do_manifesto(self):
        entrada = ldt.Ligacao(ldt.Evento(ldt.ENTRADA, 55, d(3), EADI, implicita_de=ldt.PESAGEM), ldt.MEDIA)
        ciclo = ldt.CicloCalculado(self.DESEMBARQUE, entrada_seco=entrada, situacao=ldt.CONCLUIDO, fluxo=ldt.TRA)
        linha = gc.linha_ciclo('C', ciclo, CEsResolvidos(), {'peso': 0, 'tara': 3800, 'manifesto': 20000},
                               {}, None, '2251', AGORA)
        self.assertEqual((linha['entrada_seco_id'], linha['data_entrada_seco'], linha['entrada_seco_implicita_de'],
                          linha['codigoRecintoArmazenagem']), (None, d(3), ldt.PESAGEM, EADI))
        self.assertEqual((linha['peso_menos_tara'], linha['oog'], linha['concluido']), (16200, True, True))

    def test_imagem_pela_defasagem_do_recinto(self):
        inspecao = d(5, 10)
        imagens = [(inspecao + timedelta(minutes=5), 'zero'), (inspecao + timedelta(hours=3, minutes=1), 'tres')]
        self.assertEqual(escolhe_imagem(imagens, inspecao, '8931404'), 'zero')   # Embraport: +0 h
        self.assertEqual(escolhe_imagem(imagens, inspecao, BTP), 'tres')          # BTP: +3 h
        self.assertEqual(escolhe_imagem(imagens, inspecao, '9999999'), 'tres')   # sem medição: menor resíduo
        self.assertIsNone(escolhe_imagem([(inspecao + timedelta(hours=1), 'x')], inspecao, BTP))

    def test_tabelas_batem_com_a_ddl(self):
        ddl = (Path(gc.__file__).parent / 'ddl_ciclo_de_vida_v2.sql').read_text(encoding='utf-8')
        ddl = '\n'.join(linha for linha in ddl.splitlines() if not linha.lstrip().startswith('--'))
        blocos = {}
        for nome, corpo in re.findall(r'(?:ALTER|CREATE) TABLE (?:IF NOT EXISTS )?(\w+)(.*?);', ddl, re.S):
            blocos[nome] = blocos.get(nome, '') + corpo   # a tabela do ciclo tem dois ALTER (colunas e índices)
        for tabela in (gc.CICLOS, gc.ESCANEAMENTOS):
            colunas = set(re.findall(r'ADD COLUMN (\w+)', blocos[tabela.name]))
            self.assertTrue(colunas and colunas <= set(tabela.c.keys()), (tabela.name, colunas - set(tabela.c.keys())))
        for tabela in (gc.CICLO_CE, gc.MARCAS):
            colunas = set(re.findall(r'^\s+(\w+) [A-Z]', blocos[tabela.name], re.M)) - {'PRIMARY', 'UNIQUE', 'KEY',
                                                                                        'CONSTRAINT', 'REFERENCES'}
            self.assertEqual(colunas, set(tabela.c.keys()), tabela.name)

    def test_banco_de_homologacao(self):
        self.assertEqual(sv.uri_do_banco(sv._argumentos(['--homologacao']), {'SQL_URI_HOM': 'mysql://hom'}),
                         'mysql://hom')
        self.assertEqual(sv.uri_do_banco(sv._argumentos(['--uri', 'sqlite://'])), 'sqlite://')


if __name__ == '__main__':
    unittest.main()
