"""Health check (virasana/saude/, 23/10/2026): CICLO DE VIDA (serviço, invariantes, refaz, cobertura), PESAGENS e as
APLICAÇÕES (risco histórico, busca por texto, NAVI-I, Fichas de FMA), mais a linha de comando. SQLite em memória e
mongomock; "agora" fixo.

Invariantes: o banco de `test_audita_ciclos` (desembarques em 07/10/2026: 3 ciclos com violação).
"""
from virasana.navii.busca_navii import tabela_recintos  # noqa: F401 — 1º: ajusta o sys.path
import unittest  # noqa: I100
from datetime import datetime, timedelta
from unittest import mock

import mongomock
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.integracao.mercante.mercantealchemy import Conhecimento
from virasana.saude import aplicacoes, ciclo, saude_ajna
from virasana.saude.verificacao import ATENCAO, FALHA, NAO_MEDIDO, OK, Contexto
from virasana.tests import test_audita_ciclos, test_risco_historico_lotes

AGORA = datetime(2026, 10, 23, 7, 0)


def banco_do_ciclo():
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    gc.metadata.create_all(engine)
    return engine


def ciclos(engine, inicio, quantos, com_ce, com_imagem, processado_em=AGORA):
    """`quantos` ciclos v2 desembarcados a partir de `inicio`, todos escaneados; os primeiros com CE / com imagem."""
    with engine.begin() as c:
        primeiro = c.execute(text(f'SELECT COUNT(*) FROM {gc.CICLOS.name}')).scalar()
        for i in range(quantos):
            id_ = primeiro + i + 1
            c.execute(gc.CICLOS.insert(), dict(
                ID=id_, desembarque_id=id_, numeroConteiner=f'C{id_}', versao_heuristica=gc.VERSAO,
                numeroCEmercante=f'15260000000{id_:04d}' if i < com_ce else None, processado_em=processado_em,
                data_desembarque=inicio + timedelta(hours=i)))
            c.execute(gc.ESCANEAMENTOS.insert(), dict(ciclo_de_vida_id=id_, escaneamento_id=id_,
                                                      data_escaneamento=inicio,
                                                      imagem_id='5f' * 12 if i < com_imagem else None))


class TestServico(unittest.TestCase):

    def resultado(self, alertas):
        with mock.patch.object(ciclo, 'alertas_do_acompanhamento', return_value=alertas):
            return ciclo.verifica_servico(Contexto(None, agora=AGORA))

    def test_sem_alertas_atencao_e_servico_parado(self):
        self.assertEqual(self.resultado([]).estado, OK)
        atencao = self.resultado(['recinto 8931359: só 10 (40%) dos escaneamentos com imagem — conferir'])
        self.assertEqual((atencao.estado, len(atencao.detalhes)), (ATENCAO, 1))
        parado = self.resultado(['marca de apirecintos_acessosveiculo sem atualização há 45 min: serviço parado ou '
                                 'volta demorada (ver error.log/access.log)', 'outro alerta'])
        self.assertEqual((parado.estado, parado.resumo),
                         (FALHA, '2 alerta(s) do acompanhamento (acompanha_ciclo_de_vida.py, seção 7)'))

    def test_com_o_acompanhamento_de_verdade(self):
        """O cenário do serviço (uma volta gravando): logo depois, sem alertas; 2 h depois sem volta nova, as marcas
        d'água estão paradas — serviço parado = FALHA."""
        from virasana.analytic_scripts.ciclo_de_vida import acompanha_ciclo_de_vida as ac
        from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
        from virasana.tests.test_servico_ciclo_de_vida import AGORA as AGORA_SERVICO, DESDE, Relogio, monta_banco
        engine, mongo = monta_banco()
        sv.Servico(engine, mongo, relogio=Relogio(AGORA_SERVICO)).uma_volta(DESDE)
        with mock.patch.object(ac, 'API_PARADA_HORAS', 10 ** 6):   # o cenário tem eventos de dias atrás
            em_dia = ciclo.verifica_servico(Contexto(engine, agora=AGORA_SERVICO))
            parado = ciclo.verifica_servico(Contexto(engine, agora=AGORA_SERVICO + timedelta(hours=2)))
        self.assertEqual(em_dia.estado, OK)
        self.assertEqual(parado.estado, FALHA)
        self.assertTrue(any('serviço parado' in alerta for alerta in parado.detalhes))


class TestInvariantes(unittest.TestCase):

    def test_violacoes_dos_ultimos_dois_dias(self):
        engine = test_audita_ciclos.banco()
        resultado = ciclo.verifica_invariantes(Contexto(engine, agora=datetime(2026, 10, 8, 12)))
        self.assertEqual((resultado.estado, resultado.resumo),
                         (ATENCAO, '4 ciclos das últimas 48 h auditados; 3 com violação'))
        self.assertIn('C4 desembarque 4 em 2026-10-07 10:00:00 no 8931356: concluído (tra) sem data_saida_seco',
                      resultado.detalhes)
        self.assertIn('caso_ciclo.py', resultado.detalhes[-1])
        sem = ciclo.verifica_invariantes(Contexto(engine, agora=datetime(2026, 10, 20)))
        self.assertEqual((sem.estado, sem.resumo), (OK, '0 ciclos das últimas 48 h auditados; 0 com violação'))


class TestRefaz(unittest.TestCase):

    def test_ciclos_processados_antes_da_troca_do_nucleo(self):
        engine = banco_do_ciclo()
        nucleo = datetime(2026, 10, 22, 15)
        ciclos(engine, datetime(2026, 9, 25), 3, 3, 3, processado_em=datetime(2026, 10, 1))   # não refeitos
        ciclos(engine, datetime(2026, 10, 15), 2, 2, 2, processado_em=datetime(2026, 10, 22, 17))
        self.assertEqual(ciclo.verifica_refeitos(Contexto(engine, agora=AGORA)).estado, NAO_MEDIDO)
        resultado = ciclo.verifica_refeitos(Contexto(engine, agora=AGORA, nucleo_desde=nucleo))
        self.assertEqual(resultado.estado, ATENCAO)
        self.assertTrue(resultado.resumo.startswith('3 ciclos dos últimos 30 dias processados antes'))
        self.assertEqual(resultado.detalhes, ['desembarques de 25/09/2026 a 25/09/2026: refazer no servidor com '
                                              '--refaz-desde 2026-09-25 --refaz-ate 2026-09-26'])


class TestCobertura(unittest.TestCase):

    def test_queda_de_ce_contra_a_semana_anterior(self):
        engine = banco_do_ciclo()
        fim = AGORA - timedelta(days=2)
        ciclos(engine, fim - timedelta(days=14), 10, com_ce=10, com_imagem=8)   # semana anterior: 100% e 80%
        ciclos(engine, fim - timedelta(days=7), 10, com_ce=5, com_imagem=8)     # recente: 50% e 80%
        resultado = ciclo.verifica_cobertura(Contexto(engine, agora=AGORA))
        self.assertEqual(resultado.estado, ATENCAO)
        self.assertEqual(resultado.detalhes, ['com CE: 50% na semana mais recente, 100% na anterior — queda de 50 '
                                              'pontos'])
        self.assertIn('escaneados com imagem 80% (sem. ant. 80%)', resultado.resumo)


class TestPesagens(unittest.TestCase):

    def pesagens(self, engine, inicio, quantas, divergentes):
        with engine.begin() as c:
            primeiro = c.execute(text(f'SELECT COUNT(*) FROM {gc.PESAGENS.name}')).scalar()
            c.execute(gc.PESAGENS.insert(), [dict(
                ciclo_de_vida_id=1, pesagem_id=primeiro + i + 1, data=inicio + timedelta(hours=i), operacao='retirada',
                principal=True, diagnostico='divergencia' if i < divergentes else 'coerente') for i in range(quantas)])

    def test_salto_de_possivel_fraude(self):
        engine = banco_do_ciclo()
        self.pesagens(engine, AGORA - timedelta(days=20), 100, divergentes=2)    # 2% nos 21 dias anteriores
        self.pesagens(engine, AGORA - timedelta(days=5), 30, divergentes=6)      # 20% nos últimos 7
        resultado = ciclo.verifica_pesagens(Contexto(engine, agora=AGORA))
        self.assertEqual(resultado.estado, ATENCAO)
        self.assertEqual(resultado.detalhes, ['possível fraude: 20,0% nos últimos 7 dias, 2,0% nos 21 anteriores — '
                                              'balança ou transmissão com problema?'])
        self.assertEqual(resultado.resumo, '30 pesagens principais em 7 dias; possível fraude 20%, digitação 0%')

    def test_sem_a_tabela(self):
        engine = create_engine('sqlite://')
        self.assertEqual(ciclo.verifica_pesagens(Contexto(engine, agora=AGORA)).estado, NAO_MEDIDO)


class TestRisco(unittest.TestCase):

    def test_extracao_em_dia_e_parada(self):
        from virasana.risco_historico.extracao import MARCA_CE, Extracao
        engine = test_risco_historico_lotes.banco()
        agora = test_risco_historico_lotes.AGORA
        parada = aplicacoes.verifica_risco(Contexto(engine, agora=agora))   # sem marcas: nunca rodou
        self.assertEqual(parada.estado, ATENCAO)
        self.assertIn('extração parada: marca atualizada sem registro', parada.detalhes[0])
        with engine.begin() as conexao:
            maior = conexao.execute(text('SELECT MAX(ID) FROM conhecimentosresumo')).scalar()
            Extracao(conexao, relogio=lambda: agora - timedelta(hours=1)).grava_marca(MARCA_CE, maior)
        em_dia = aplicacoes.verifica_risco(Contexto(engine, agora=agora))
        self.assertEqual((em_dia.estado, em_dia.detalhes), (OK, []))
        self.assertTrue(em_dia.resumo.startswith('extração há 1,0 h; 0 IDs por ler; fila 0'))


class Meili:
    """Cliente simulado do Meilisearch (as três chamadas da verificação)."""

    def __init__(self, status='available', ultimo_id=None):
        self.status, self.ultimo_id = status, ultimo_id

    def saude(self):
        return {'status': self.status}

    def documento(self, indice, chave):
        return None if self.ultimo_id is None else {'chave': chave, 'ultimo_id': self.ultimo_id}

    def estatisticas(self, indice):
        return {'numberOfDocuments': 1234}


class TestBusca(unittest.TestCase):

    def contexto(self, meili):
        engine = create_engine('sqlite://')
        Conhecimento.__table__.create(engine)
        with engine.begin() as c:
            c.execute(Conhecimento.__table__.insert(), [{'ID': 30000, 'numeroCEmercante': 'X'}])
        return Contexto(engine, meili=meili, agora=AGORA)

    def test_em_dia_atrasado_fora_e_sem_chave(self):
        em_dia = aplicacoes.verifica_busca(self.contexto(Meili(ultimo_id=29990)))
        self.assertEqual((em_dia.estado, em_dia.resumo), (OK, '1.234 CEs no índice; 10 IDs do Mercante por indexar'))
        self.assertEqual(aplicacoes.verifica_busca(self.contexto(Meili(ultimo_id=1000))).estado, ATENCAO)
        self.assertEqual(aplicacoes.verifica_busca(self.contexto(Meili(ultimo_id=None))).estado, ATENCAO)
        self.assertEqual(aplicacoes.verifica_busca(self.contexto(Meili(status='unavailable'))).estado, FALHA)
        self.assertEqual(aplicacoes.verifica_busca(self.contexto(None)).estado, NAO_MEDIDO)


class TestNavii(unittest.TestCase):

    def contexto(self, segundos):
        mongo = mongomock.MongoClient().db
        if segundos:
            mongo['navii_uso_pesquisas'].insert_many([
                {'inicio': AGORA - timedelta(hours=2), 'tempos': {'contagem': s / 2, 'consulta': s / 4, 'cards': s / 4}}
                for s in segundos] + [{'inicio': AGORA - timedelta(days=3), 'tempos': {'consulta': 99.0}}])
        return Contexto(None, mongo=mongo, agora=AGORA)

    def test_tempo_de_resposta(self):
        ok = aplicacoes.verifica_navii(self.contexto([1.0] * 25))
        self.assertEqual((ok.estado, ok.resumo), (OK, '25 pesquisas em 24 h; 1ª página no servidor: mediana 1,0 s, '
                                                      'p95 1,0 s; 0 lenta(s) (>= 10 s)'))
        lento = aplicacoes.verifica_navii(self.contexto([1.0] * 25 + [15.0] * 3))
        self.assertEqual(lento.estado, ATENCAO)
        self.assertIn('3 lenta(s)', lento.resumo)
        self.assertEqual(aplicacoes.verifica_navii(self.contexto([])).resumo, 'nenhuma pesquisa nas últimas 24 h')
        self.assertEqual(aplicacoes.verifica_navii(Contexto(None, agora=AGORA)).estado, NAO_MEDIDO)


class TestFma(unittest.TestCase):

    def resultado(self, dias):
        engine = create_engine('sqlite://')
        with engine.begin() as c:
            c.execute(text('CREATE TABLE ovr_ovrs (id INTEGER PRIMARY KEY, tipooperacao INTEGER, '
                           'create_date DATETIME)'))
            c.execute(text('INSERT INTO ovr_ovrs (tipooperacao, create_date) VALUES (0, :d), (1, :hoje)'),
                      {'d': AGORA - timedelta(days=dias), 'hoje': AGORA})
        return aplicacoes.verifica_fma(Contexto(engine, agora=AGORA))

    def test_ficha_de_fma_recente_e_parada(self):
        self.assertEqual(self.resultado(1).estado, OK)
        parada = self.resultado(5)
        self.assertEqual((parada.estado, parada.resumo), (ATENCAO, 'Ficha de FMA mais nova criada há 5,0 dias'))
        self.assertEqual(aplicacoes.verifica_fma(Contexto(create_engine('sqlite://'), agora=AGORA)).estado,
                         NAO_MEDIDO)


class TestLinhaDeComando(unittest.TestCase):

    def test_argumentos(self):
        args = saude_ajna._argumentos(['--nucleo-desde', '2026-10-22', '--so', 'mercante', 'api'])
        self.assertEqual((args.nucleo_desde, args.so), (datetime(2026, 10, 22), ['mercante', 'api']))

    def test_roda_so_as_pedidas_e_isola_o_erro(self):
        resultados = saude_ajna.roda(Contexto(None, agora=AGORA), so=['mercante', 'imagens'])
        self.assertEqual([(r.componente, r.estado) for r in resultados],
                         [('Mercante (carga)', FALHA), ('Imagens (MongoDB)', NAO_MEDIDO)])   # sem banco: erro isolado
        self.assertTrue(resultados[0].resumo.startswith('erro na verificação: AttributeError'))
        self.assertEqual(len(saude_ajna.VERIFICACOES), 12)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
