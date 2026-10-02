"""Testes do ACOMPANHAMENTO do Ciclo de Vida v2 (acompanha_ciclo_de_vida.py).

Usa o cenário dos testes do serviço (test_servico_ciclo_de_vida): uma volta gravando em 20/03/2026 e o
relatório logo depois.
"""
import io
import unittest
from datetime import timedelta
from unittest import mock

from sqlalchemy import text

from virasana.analytic_scripts.ciclo_de_vida import acompanha_ciclo_de_vida as ac
from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
from virasana.tests.test_servico_ciclo_de_vida import AGORA, DESDE, Relogio, monta_banco


class AcompanhamentoTestCase(unittest.TestCase):

    def setUp(self):
        self.engine, mongo = monta_banco()
        sv.Servico(self.engine, mongo, relogio=Relogio(AGORA)).uma_volta(DESDE)

    def relatorio(self, agora=AGORA, secao=None):
        saida = io.StringIO()
        with self.engine.connect() as conexao:
            acompanhamento = ac.Acompanhamento(conexao, saida, AGORA - timedelta(days=28), AGORA, agora=agora)
            if secao:
                getattr(acompanhamento, secao)()
                alertas = acompanhamento.alertas
            else:
                alertas = ac.roda(acompanhamento)
        return saida.getvalue(), alertas


class TestRelatorio(AcompanhamentoTestCase):

    def test_todas_as_secoes_sem_erro_e_sem_alerta(self):
        with mock.patch.object(ac, 'API_PARADA_HORAS', 10 ** 6):   # o cenário tem eventos de dias atrás
            texto, alertas = self.relatorio()
        self.assertNotIn('ERRO', texto)
        for titulo in ('1. SERVIÇO', '2. COBERTURA', '3. SITUAÇÃO', '4. ESCANEAMENTOS', '5. CE', '6. PENDENTES',
                       '7. ALERTAS'):
            self.assertIn(titulo, texto)
        self.assertEqual(alertas, [])
        self.assertIn('- nenhum', texto)

    def test_marcas_e_gravacoes(self):
        texto, _ = self.relatorio(secao='secao_servico')
        self.assertRegex(texto, r'embarquedesembarque\s+\|\s+105\s+\|\s+105\s+\|\s+0 ')
        self.assertRegex(texto, r'acessosveiculo\s+\|\s+204\s+\|\s+205\s+\|\s+0 ')   # 205 é agendamento (G)
        self.assertRegex(texto, r'ciclos gravados na última hora\s+\|\s+4')   # CTRA, CTRB, CTRV, CTRC

    def test_cobertura_da_semana(self):
        texto, _ = self.relatorio(secao='secao_cobertura')
        # semana de 02/03: CTRA (TRA aberto, CE, escaneamento com imagem) e CTRB (Ecoporto, entregue, OOG)
        self.assertRegex(texto, r'2026-03-02 \| 2 +\| 2 \(100%\) \| 0 +\| 1 \(50%\) +\| 1 \(50%\) +\| 0 +\| 1 +'
                                r'\| 0 +\| 1 \(50%\) +\| 1 \(100%\) +\| 1')
        # semana de 09/03: CTRV (vazio) e CTRC (domingo, 15/03; sem CE) — os dois abertos
        self.assertRegex(texto, r'2026-03-09 \| 2 +\| 2 \(100%\) \| 0 +\| 2 \(100%\) \| 0 \(0%\) +\| 1 ')

    def test_situacao_fluxo_e_escaneamentos(self):
        texto, _ = self.relatorio(secao='secao_situacao')
        self.assertRegex(texto, r'concluido\s+\|\s+operador\s+\|\s+1')       # CTRB, Ecoporto
        self.assertRegex(texto, r'aberto\s+\|\s+tra\s+\|\s+1')               # CTRA no EADI
        texto, _ = self.relatorio(secao='secao_escaneamentos')
        self.assertRegex(texto, r'8931359\s+\|\s+BTP\s+\|\s+1\s+\|\s+1 \(100%\)\s+\|\s+0 ')

    def test_pendentes_antigos(self):
        texto, _ = self.relatorio(secao='secao_pendentes_antigos')
        self.assertRegex(texto, r'2025-10\s+\|\s+1')                        # CTRO
        self.assertIn('Total: 1', texto)


class TestAlertas(AcompanhamentoTestCase):

    def test_servico_parado(self):
        _, alertas = self.relatorio(agora=AGORA + timedelta(hours=1), secao='secao_servico')
        self.assertEqual(len([a for a in alertas if 'sem atualização há 60 min' in a]), 4)

    def test_fila_de_eventos(self):
        with self.engine.begin() as c:
            c.execute(text('INSERT INTO apirecintos_inspecoesnaoinvasivas (id, numeroConteiner, dataHoraOcorrencia, '
                           "codigoRecinto) VALUES (302, 'CTRA', '2026-03-19 23:00:00', '8931359')"))
        with mock.patch.object(ac, 'FILA_ALERTA', 1):
            _, alertas = self.relatorio(secao='secao_servico')
        self.assertEqual([a for a in alertas if 'fila' in a],
                         ['apirecintos_inspecoesnaoinvasivas: 1 eventos ainda não lidos (fila)'])

    def test_loop_antigo_rodando_junto(self):
        with self.engine.begin() as c:   # como o loop antigo faria: mexe no ciclo v2 e liga escaneamento
            c.execute(gc.CICLOS.update().where(gc.CICLOS.c.ID == 77).values(last_modified=AGORA + timedelta(minutes=3)))
            c.execute(gc.ESCANEAMENTOS.insert().values(ciclo_de_vida_id=77, escaneamento_id=998,
                                                       data_escaneamento=AGORA))
        _, alertas = self.relatorio(secao='secao_servico')
        self.assertTrue(any('loop antigo' in a and a.startswith('1 ciclos') for a in alertas))

    def test_recinto_sem_imagens(self):
        with self.engine.begin() as c:
            c.execute(gc.ESCANEAMENTOS.insert(), [dict(ciclo_de_vida_id=77, escaneamento_id=500 + i,
                                                       data_escaneamento=AGORA, codigoRecinto='8931318',
                                                       confianca='alta') for i in range(ac.ESCANEAMENTOS_MINIMOS)])
        _, alertas = self.relatorio(secao='secao_escaneamentos')
        self.assertTrue(any(a.startswith('recinto 8931318: só 0 (0%)') for a in alertas))

    def test_api_parada(self):
        _, alertas = self.relatorio(secao='secao_servico')   # agora = 20/03; pesagem mais nova em 10/03
        self.assertIn('apirecintos_pesagensveiculo: o evento mais novo é de 10/03/2026 09:00 — a API Recintos '
                      'parou de receber esta tabela?', alertas)

    def test_abertos_que_deviam_ser_abandono(self):
        # 90 dias depois: CTRV no operador sem saída (desembarque 10/03) e CTRA no EADI (entrada 04/03) passam de
        # 93 dias; o CTRC (saiu do operador, em trânsito) não conta
        _, alertas = self.relatorio(agora=AGORA + timedelta(days=90), secao='secao_situacao')
        self.assertIn('2 ciclos v2 abertos no operador (sem saída) ou no recinto seco há mais de 93 dias — deviam '
                      'ser abandono', alertas)
        _, alertas = self.relatorio(secao='secao_situacao')
        self.assertFalse([a for a in alertas if 'abandono' in a])

    def test_abertos_no_operador_por_operador(self):
        texto, _ = self.relatorio(secao='secao_situacao')
        # CTRV (vazio, BTP, desembarque 10/03): aberto no operador há mais de 7 dias
        self.assertRegex(texto, r'8931359\s+\|\s+BTP\s+\|\s+1\s+\|\s+1\s+\|\s+1\s+\|\s+0')

    def test_sem_ce_por_fluxo(self):
        texto, _ = self.relatorio(secao='secao_ce')
        self.assertRegex(texto, r'aberto\s+\|\s+indefinido\s+\|\s+1')   # CTRC, aguardando entrada em seco

    def test_aberto_sem_revisita(self):
        _, alertas = self.relatorio(agora=AGORA + timedelta(hours=13), secao='secao_situacao')
        self.assertTrue(any('sem revisita há mais de 12 h' in a for a in alertas))


class TestFuncoes(unittest.TestCase):

    def test_segunda_feira_da_semana(self):
        self.assertEqual(ac._segunda('2026-10-02'), '2026-09-28')
        self.assertEqual(ac._segunda('2026-09-28 10:00:00'), '2026-09-28')

    def test_percentual(self):
        self.assertEqual(ac._pct(1, 4), '1 (25%)')
        self.assertEqual(ac._pct(0, 0), '0')
