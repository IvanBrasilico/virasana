"""Testes do núcleo da heurística do Ciclo de Vida (linha_do_tempo, ces_do_ciclo, heuristica_ciclo).

Casos tirados das amostras reais do diagnóstico de 01/10/2026 (passagem no mesmo operador, TRA passando
pelo Ecoporto, escaneamento terceirizado) e das situações descritas pelo usuário (saída omitida e
contêiner que volta vazio, saída muitos dias depois, abandono, baldeação).
"""
import unittest
from datetime import date, datetime, timedelta
from itertools import count

from virasana.analytic_scripts.ciclo_de_vida import ces_do_ciclo as cdc
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import processa_conteiner

SB, BTP, EMB, ECO, RIO = '8931356', '8931359', '8931404', '8931318', '7921303'   # operadores
SECO, SECO2, DEPOT = '8931339', '8933001', '8939000'                             # não operadores
TRANSBRASA, MULTILOG = '8931305', '8933201'                                      # não operadores
ESCOPO = {SB, BTP, EMB, ECO}
OPERADORES = ESCOPO | {RIO}
AGORA = datetime(2026, 10, 1, 12)
M, H1, H2, X = '152600000000001', '152600000000002', '152600000000003', '152600000000099'
_ids = count(1)


def ev(tipo, quando, recinto, vazio=None, ce=None, id_=None):
    return ldt.Evento(tipo, id_ or next(_ids), quando, recinto, vazio, ce)


def D(quando, recinto=SB, **kw):
    return ev(ldt.DESEMBARQUE, quando, recinto, **kw)


def E(quando, recinto=SB, **kw):
    return ev(ldt.EMBARQUE, quando, recinto, **kw)


def ENT(quando, recinto, **kw):
    return ev(ldt.ENTRADA, quando, recinto, **kw)


def S(quando, recinto=SB, **kw):
    return ev(ldt.SAIDA, quando, recinto, **kw)


def INSP(quando, recinto, **kw):
    return ev(ldt.INSPECAO, quando, recinto, **kw)


def PES(quando, recinto, **kw):
    return ev(ldt.PESAGEM, quando, recinto, **kw)


def t(mes, dia, hora=8, minuto=0):
    return datetime(2026, mes, dia, hora, minuto)


def um_ciclo(eventos, familia=None):
    familias = None
    if familia is not None:
        familias = {e.id: familia for e in eventos if e.tipo == ldt.DESEMBARQUE}
    [ciclo] = ldt.ciclos_do_conteiner(eventos, ESCOPO, OPERADORES, AGORA, familias)
    return ciclo


class TestFluxos(unittest.TestCase):

    def test_passagem_no_mesmo_operador(self):
        # amostra real: desembarque -> embarque 4 dias depois, mesmo CE declarado, sem gate no meio
        ciclo = um_ciclo([D(t(9, 26, 9, 15), ce=X), E(t(9, 30, 15, 21), ce=X)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.PASSAGEM, ldt.CONCLUIDO))
        self.assertEqual(ciclo.embarque.confianca, ldt.ALTA)

    def test_tra_passando_pelo_ecoporto_com_scanner_no_gate(self):
        # amostra real (ciclo 336904): o algoritmo antigo fechou o "TRA" no Ecoporto, que é operador
        entrada_tra = ENT(t(9, 29, 17, 14), SECO, ce=X)
        inspecao = INSP(t(9, 29, 17, 4), SECO, ce=X)
        ciclo = um_ciclo([D(t(9, 27, 21, 23), EMB), S(t(9, 29, 15, 59), EMB, ce=X), ENT(t(9, 29, 16, 58), ECO),
                          inspecao, S(t(9, 29, 17, 7), ECO), entrada_tra])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.TRA, ldt.ABERTO, 'no recinto seco'))
        self.assertIs(ciclo.entrada_seco.evento, entrada_tra)
        [esc] = ciclo.escaneamentos
        self.assertIs(esc.evento, inspecao)
        self.assertEqual(esc.confianca, ldt.ALTA)
        self.assertIn('antes da entrada (scanner no gate)', esc.motivos)
        self.assertIn('passagem pelo operador 8931318', ciclo.alertas)

    def test_operador_com_escaneamento_terceirizado(self):
        # amostra real (ciclo 318863): saída do operador e inspeção no Ecoporto 2 h depois, com o CE
        inspecao = INSP(t(9, 24, 10, 4), ECO, ce=H1)
        eventos = [D(t(9, 23, 12, 10)), S(t(9, 24, 8, 11)), inspecao]
        ciclo = um_ciclo(eventos)
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.OPERADOR, ldt.CONCLUIDO,
                                                                       'entregue pelo operador'))
        self.assertEqual(ciclo.escaneamentos[0].confianca, ldt.MEDIA)   # em trânsito, sem CE para corroborar
        self.assertEqual(um_ciclo(eventos, familia={M, H1}).escaneamentos[0].confianca, ldt.ALTA)

    def test_saida_muitos_dias_depois_valida_pela_continuidade(self):
        ciclo = um_ciclo([D(t(8, 1)), S(t(8, 25))])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertEqual(ciclo.saida.confianca, ldt.MEDIA)
        self.assertIn('saída 24 dias após o desembarque', ciclo.saida.motivos)

    def test_saida_omitida_e_conteiner_volta_vazio(self):
        # princípio do usuário: a saída "encontrada" 21 dias depois é de OUTRA operação
        ciclo = um_ciclo([D(t(8, 1)), ENT(t(8, 20), SB, vazio=True), S(t(8, 22))])
        self.assertEqual(ciclo.situacao, ldt.INCOMPLETO)
        self.assertIn('reentrada no operador', ciclo.motivo)
        self.assertIsNone(ciclo.saida)

    def test_devolucao_de_vazio_nao_e_tra_e_nao_leva_o_scanner_do_deposito(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), INSP(t(9, 3, 17, 30), DEPOT, vazio=True),
                          ENT(t(9, 3, 18), DEPOT, vazio=True)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertIn('vazio', ciclo.motivo)
        self.assertIsNone(ciclo.entrada_seco)
        self.assertEqual(ciclo.escaneamentos, [])

    def test_volta_ao_operador_do_desembarque_e_outra_operacao(self):
        # sombra de 01/10 (ECMU7356412, FFAU6617369): saiu e, dias depois, voltou vazio ao mesmo terminal
        ciclo = um_ciclo([D(t(9, 17)), S(t(9, 18, 22)), INSP(t(9, 29, 16), SB, vazio=True),
                          ENT(t(9, 29, 17), SB, vazio=True), INSP(t(9, 30, 16), SB, vazio=True)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertIn('voltou ao operador do desembarque', ciclo.motivo)
        self.assertEqual(ciclo.escaneamentos, [])   # nem o scanner do gate da volta nem o de depois

    def test_volta_ao_operador_do_desembarque_depois_de_outro_operador(self):
        ciclo = um_ciclo([D(t(9, 1), BTP), S(t(9, 2), BTP), ENT(t(9, 2, 10), EMB), ENT(t(9, 12), BTP)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertIn('voltou ao operador do desembarque', ciclo.motivo)
        self.assertIn('saída do operador 8931404 não informada', ciclo.alertas)

    def test_transito_longo_ainda_tra(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 2, 20), SECO2)])
        self.assertEqual(ciclo.fluxo, ldt.TRA)
        self.assertIn('trânsito de 10 h', ciclo.entrada_seco.motivos)

    def test_entrada_fora_do_prazo_de_transito(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 5), SECO2)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertIsNone(ciclo.entrada_seco)
        self.assertTrue(any('fora do prazo de trânsito' in a for a in ciclo.alertas))

    def test_tra_completo(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 2, 12), SECO2), INSP(t(9, 2, 13), SECO2),
                          S(t(9, 6), SECO2)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.TRA, ldt.CONCLUIDO,
                                                                       'saída do recinto seco'))
        self.assertEqual(ciclo.escaneamentos[0].confianca, ldt.ALTA)

    def test_tra_sem_saida_do_seco_e_embarque_depois(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 2, 12), SECO2), E(t(9, 20), SB)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.TRA, ldt.INCOMPLETO))
        self.assertIn('saída do recinto seco não informada', ciclo.motivo)

    def test_saida_do_operador_omitida_e_entrada_direta_no_seco(self):
        ciclo = um_ciclo([D(t(9, 1)), ENT(t(9, 6), SECO2)])
        self.assertEqual(ciclo.fluxo, ldt.TRA)
        self.assertEqual(ciclo.entrada_seco.confianca, ldt.MEDIA)
        self.assertIn('saída do operador não informada', ciclo.alertas)

    def test_baldeacao(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 2, 12), BTP), E(t(9, 4), BTP)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.BALDEACAO, ldt.CONCLUIDO))
        self.assertEqual(ciclo.embarque.confianca, ldt.ALTA)

    def test_abandono_e_aberto(self):
        self.assertEqual(um_ciclo([D(t(6, 1))]).situacao, ldt.ABANDONO)
        aberto = um_ciclo([D(t(9, 29))])
        self.assertEqual((aberto.situacao, aberto.motivo), (ldt.ABERTO, 'no operador'))
        recente = um_ciclo([D(t(9, 29)), S(t(10, 1, 8))])
        self.assertEqual(recente.situacao, ldt.ABERTO)   # ainda pode entrar num recinto seco
        self.assertIn('aguardando', recente.motivo)


class TestOmissoesEPassagens(unittest.TestCase):
    """Decisões do usuário de 02/10/2026: entrada implícita e escaneamento fora antes do TRA."""

    def test_entrada_implicita_pela_inspecao(self):
        # Transbrasa: informa o escaneamento e a saída, mas não a entrada
        inspecao = INSP(t(9, 2, 11), TRANSBRASA)
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), inspecao, S(t(9, 8), TRANSBRASA)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.TRA, ldt.CONCLUIDO,
                                                                       'saída do recinto seco'))
        self.assertEqual((ciclo.entrada_seco.evento.implicita_de, ciclo.entrada_seco.evento.data),
                         (ldt.INSPECAO, inspecao.data))
        self.assertEqual(ciclo.entrada_seco.confianca, ldt.MEDIA)
        self.assertIn('entrada não informada — deduzida da inspeção no recinto', ciclo.entrada_seco.motivos)
        [esc] = ciclo.escaneamentos
        self.assertEqual((esc.evento, esc.confianca), (inspecao, ldt.ALTA))
        self.assertIn('no recinto da etapa (entrada implícita)', esc.motivos)

    def test_entrada_implicita_pela_pesagem(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), PES(t(9, 2, 11), TRANSBRASA), S(t(9, 8), TRANSBRASA)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.TRA, ldt.CONCLUIDO))
        self.assertEqual(ciclo.entrada_seco.evento.implicita_de, ldt.PESAGEM)
        self.assertIn('entrada em 8931305 não informada — deduzida da pesagem', ciclo.alertas)
        self.assertEqual(ciclo.escaneamentos, [])

    def test_saida_sem_entrada_e_sem_indicio_continua_ignorada(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), S(t(9, 8), TRANSBRASA)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.OPERADOR, ldt.CONCLUIDO))
        self.assertIn('saida em 8931305 sem entrada — ignorado', ciclo.alertas)

    def test_pesagem_fora_do_transito_nao_conta(self):
        ciclo = um_ciclo([D(t(9, 1)), PES(t(9, 1, 12), TRANSBRASA), S(t(9, 2), TRANSBRASA)])
        self.assertEqual((ciclo.situacao, ciclo.motivo), (ldt.ABERTO, 'no operador'))

    def test_escaneamento_no_ecoporto_antes_do_tra_com_entrada_omitida(self):
        # BTP -> Ecoporto (escaneia; a entrada não é informada) -> TRA
        inspecao = INSP(t(9, 20, 18, 25), ECO, ce=X)
        entrada_tra = ENT(t(9, 20, 18, 47), SECO)
        ciclo = um_ciclo([D(t(9, 18), BTP), S(t(9, 20, 18, 9), BTP), inspecao, S(t(9, 20, 18, 35), ECO),
                          entrada_tra])
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.TRA, ldt.ABERTO))
        self.assertIs(ciclo.entrada_seco.evento, entrada_tra)
        self.assertIn('entrada em 8931318 não informada — deduzida da inspeção', ciclo.alertas)
        [esc] = ciclo.escaneamentos
        self.assertIs(esc.evento, inspecao)
        self.assertIn('na passagem pelo operador 8931318 (escaneamento fora)', esc.motivos)

    def test_passagem_rapida_por_recinto_nao_operador(self):
        # BTP -> MULTILOG (entra, escaneia e sai em 30 min) -> TRA: o TRA é o segundo recinto
        inspecao = INSP(t(9, 2, 10, 30), MULTILOG)
        entrada_tra = ENT(t(9, 2, 11, 30), SECO)
        ciclo = um_ciclo([D(t(9, 1), BTP), S(t(9, 2, 10), BTP), ENT(t(9, 2, 10, 20), MULTILOG), inspecao,
                          S(t(9, 2, 10, 50), MULTILOG), entrada_tra, S(t(9, 9), SECO)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.TRA, ldt.CONCLUIDO,
                                                                       'saída do recinto seco'))
        self.assertIs(ciclo.entrada_seco.evento, entrada_tra)
        self.assertEqual(ciclo.saida_seco.evento.recinto, SECO)
        self.assertIn('passagem rápida pelo recinto 8933201', ciclo.alertas)
        [esc] = ciclo.escaneamentos
        self.assertIn('na passagem rápida pelo recinto 8933201 (escaneamento fora)', esc.motivos)

    def test_estada_curta_sem_nova_entrada_e_o_tra(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 2, 11), SECO2), S(t(9, 2, 13), SECO2)])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.TRA, ldt.CONCLUIDO,
                                                                       'saída do recinto seco'))
        # nova entrada VAZIA logo depois: a estada curta foi o TRA (o vazio é outra operação)
        vazio = um_ciclo([D(t(9, 1)), S(t(9, 2, 10)), ENT(t(9, 2, 11), SECO2), S(t(9, 2, 13), SECO2),
                          INSP(t(9, 2, 13, 50), DEPOT, vazio=True), ENT(t(9, 2, 14), DEPOT, vazio=True)])
        self.assertEqual((vazio.fluxo, vazio.situacao, vazio.entrada_seco.evento.recinto),
                         (ldt.TRA, ldt.CONCLUIDO, SECO2))
        self.assertEqual(vazio.escaneamentos, [])   # o scanner do depósito é da outra operação

    def test_estada_curta_recente_aguarda(self):
        ciclo = ldt.ciclos_do_conteiner([D(t(10, 1, 1)), S(t(10, 1, 2)), ENT(t(10, 1, 3), SECO2),
                                         S(t(10, 1, 4), SECO2)], ESCOPO, OPERADORES, t(10, 1, 6))[0]
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.TRA, ldt.ABERTO))
        self.assertIn('estada curta', ciclo.motivo)


class TestRemocaoABordo(unittest.TestCase):
    """Desembarque -> embarque -> novo desembarque no mesmo operador, logo depois e sem gate no meio: o contêiner não
    saiu da operação (pedido do usuário, 02/10/2026)."""

    def ciclos(self, eventos):
        return ldt.ciclos_do_conteiner(eventos, ESCOPO, OPERADORES, AGORA)

    def test_remocao_continua_o_ciclo_do_primeiro_desembarque(self):
        inspecao, saida, entrada = INSP(t(9, 20, 10), SB), S(t(9, 24), SB), ENT(t(9, 24, 12), SECO)
        segundo = D(t(9, 21, 14), SB)
        [ciclo] = self.ciclos([D(t(9, 20)), inspecao, E(t(9, 21, 8), SB), segundo, saida, entrada])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.motivo), (ldt.TRA, ldt.ABERTO, 'no recinto seco'))
        self.assertIsNone(ciclo.embarque)
        self.assertEqual([lig.evento for lig in ciclo.escaneamentos], [inspecao])   # o escaneamento não se perde
        self.assertEqual((ciclo.saida.evento, ciclo.entrada_seco.evento), (saida, entrada))
        self.assertEqual(ciclo.remocoes, [segundo])
        self.assertIn('remoção a bordo em 8931356: embarque e novo desembarque em 6 h — o ciclo continua',
                      ciclo.alertas)

    def test_remocoes_encadeadas_e_depois_embarque_de_verdade(self):
        embarque = E(t(9, 25), SB)
        [ciclo] = self.ciclos([D(t(9, 20)), E(t(9, 20, 10), SB), D(t(9, 20, 12), SB), E(t(9, 21, 9), SB),
                               D(t(9, 21, 10), SB), embarque])
        self.assertEqual((ciclo.fluxo, ciclo.situacao, ciclo.embarque.evento), (ldt.PASSAGEM, ldt.CONCLUIDO, embarque))
        self.assertEqual(len(ciclo.remocoes), 2)

    def test_novo_desembarque_depois_do_prazo_e_outra_operacao(self):
        ciclos = self.ciclos([D(t(9, 20)), E(t(9, 21), SB), D(t(9, 22, 9), SB)])   # 25 h depois do embarque
        self.assertEqual([(c.fluxo, c.situacao) for c in ciclos],
                         [(ldt.PASSAGEM, ldt.CONCLUIDO), (ldt.INDEFINIDO, ldt.ABERTO)])
        self.assertEqual([c.remocoes for c in ciclos], [[], []])

    def test_com_gate_no_meio_ou_em_outro_operador_nao_e_remocao(self):
        # saiu e voltou ao operador antes do embarque: o novo desembarque é outra operação
        ciclos = self.ciclos([D(t(9, 1)), S(t(9, 2)), ENT(t(9, 20), SB), E(t(9, 20, 6), SB), D(t(9, 20, 9), SB)])
        self.assertEqual(len(ciclos), 2)
        # embarque num operador e desembarque em outro
        self.assertEqual(len(self.ciclos([D(t(9, 20)), E(t(9, 21), SB), D(t(9, 21, 3), BTP)])), 2)
        # sem desembarque antes, o embarque e o desembarque seguintes não são remoção
        self.assertEqual(len(self.ciclos([E(t(9, 21), SB), D(t(9, 21, 3), SB)])), 1)


class TestSegmentacaoEConfianca(unittest.TestCase):

    def test_cada_desembarque_abre_um_ciclo(self):
        s1, s2 = S(t(7, 2)), S(t(8, 3))
        ciclos = ldt.ciclos_do_conteiner([D(t(7, 1)), s1, D(t(8, 1)), s2], ESCOPO, OPERADORES, AGORA)
        self.assertEqual([c.saida.evento for c in ciclos], [s1, s2])

    def test_desembarque_fora_do_escopo_nao_abre_ciclo_mas_corta(self):
        ciclos = ldt.ciclos_do_conteiner([D(t(7, 1)), D(t(7, 20), RIO), S(t(7, 22), RIO)], ESCOPO, OPERADORES,
                                         AGORA)
        self.assertEqual(len(ciclos), 1)
        self.assertIsNone(ciclos[0].saida)   # a saída no Rio é de outra operação

    def test_retificacao_prevalece(self):
        original, retificacao = S(t(9, 2), id_=5005), S(t(9, 2), id_=9009)
        ciclo = um_ciclo([D(t(9, 1)), retificacao, original])
        self.assertIs(ciclo.saida.evento, retificacao)

    def test_confianca_das_inspecoes(self):
        ciclo = um_ciclo([D(t(9, 1, 10)), INSP(t(9, 1, 9, 30), SB), INSP(t(9, 1, 12), SB, ce=X),
                          INSP(t(9, 1, 13), SB, vazio=True), INSP(t(9, 1, 14), SB, ce=H1)], familia={M, H1})
        confianca = [(e.confianca, e.motivos[-1]) for e in ciclo.escaneamentos]
        self.assertEqual(confianca, [(ldt.MEDIA, 'antes do desembarque (relógio)'),
                                     (ldt.BAIXA, 'CE declarado de OUTRA família'),
                                     (ldt.MEDIA, 'contêiner marcado vazio'),
                                     (ldt.ALTA, 'CE declarado da família do CE do ciclo')])

    def test_saida_com_ce_de_outra_familia_nao_conclui(self):
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 2), ce=X)], familia={M, H1})
        self.assertEqual(ciclo.saida.confianca, ldt.BAIXA)
        self.assertEqual(ciclo.situacao, ldt.ABERTO)
        self.assertIn('saída duvidosa', ciclo.motivo)

    def test_saida_duvidosa_nao_conclui_nem_com_entrada_vazia_depois(self):
        # sombra de 01/10 (ZCSU6632611): saída com CE de outra família seguida de entrada vazia
        ciclo = um_ciclo([D(t(9, 1)), S(t(9, 14), ce=X), ENT(t(9, 15), DEPOT, vazio=True)], familia={M, H1})
        self.assertEqual(ciclo.situacao, ldt.ABERTO)
        self.assertIn('saída duvidosa', ciclo.motivo)

    def test_embarque_com_ce_de_outra_familia_e_incompleto(self):
        ciclo = um_ciclo([D(t(9, 1)), E(t(9, 20), ce=X)], familia={M, H1})
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.INDEFINIDO, ldt.INCOMPLETO))
        # também depois de sair (baldeação) ou de entrar em outro operador
        for eventos in ([D(t(9, 1)), S(t(9, 2)), E(t(9, 3), BTP, ce=X)],
                        [D(t(9, 1)), S(t(9, 2)), ENT(t(9, 2, 5), BTP), E(t(9, 4), BTP, ce=X)]):
            with self.subTest(eventos=len(eventos)):
                self.assertEqual(um_ciclo(eventos, familia={M, H1}).situacao, ldt.INCOMPLETO)

    def test_ce_declarado_fora_do_mercante_nao_e_outra_familia(self):
        eventos = [D(t(9, 1)), INSP(t(9, 1, 12), SB, ce=X), S(t(9, 2), ce=X)]
        familias = {eventos[0].id: {M, H1}}
        [ciclo] = ldt.ciclos_do_conteiner(eventos, ESCOPO, OPERADORES, AGORA, familias, conhecidos={M, H1})
        self.assertEqual([lig.confianca for lig in ciclo.ligacoes()], [ldt.MEDIA, ldt.MEDIA])
        self.assertIn('CE declarado fora do Mercante', ciclo.saida.motivos)
        self.assertEqual(ciclo.situacao, ldt.CONCLUIDO)
        # existe no Mercante, de outra família: desconfia
        [ciclo] = ldt.ciclos_do_conteiner(eventos, ESCOPO, OPERADORES, AGORA, familias, conhecidos={M, H1, X})
        self.assertEqual(ciclo.saida.confianca, ldt.BAIXA)


class TestCEsDoCiclo(unittest.TestCase):
    DESEMBARQUE = t(9, 10)

    def candidato(self, ce, master=None, trafego='5', dias=2):
        return cdc.CandidatoCE(ce, master, trafego, (date(2026, 9, 10) + timedelta(days=dias),))

    def test_familia_com_filhote_como_principal(self):
        candidatos = [self.candidato(M), self.candidato(H1, M)]
        r = cdc.resolve_ces(self.DESEMBARQUE, candidatos)
        self.assertEqual((r.principal, r.ces, r.confianca), (H1, {M: 'master', H1: 'filhote'}, ldt.MEDIA))
        r = cdc.resolve_ces(self.DESEMBARQUE, candidatos, declarados=['15.2600.0000.000-01'])  # declarou o master
        self.assertEqual((r.principal, r.confianca), (H1, ldt.ALTA))

    def test_varios_filhotes_principal_e_o_declarado(self):
        candidatos = [self.candidato(M), self.candidato(H1, M), self.candidato(H2, M)]
        self.assertEqual(cdc.resolve_ces(self.DESEMBARQUE, candidatos).principal, H1)
        self.assertEqual(cdc.resolve_ces(self.DESEMBARQUE, candidatos, [H2]).principal, H2)

    def test_exportacao_descartada(self):
        r = cdc.resolve_ces(self.DESEMBARQUE, [self.candidato(X, trafego='7')])
        self.assertIsNone(r.principal)
        self.assertIn('1 CE(s) de exportação descartado(s)', r.motivos)

    def test_janela_da_escala(self):
        self.assertEqual(cdc.resolve_ces(self.DESEMBARQUE, [self.candidato(X, dias=12)]).principal, X)
        self.assertIsNone(cdc.resolve_ces(self.DESEMBARQUE, [self.candidato(X, dias=20)]).principal)
        r = cdc.resolve_ces(self.DESEMBARQUE, [self.candidato(X, dias=20)], declarados=[X])
        self.assertEqual((r.principal, r.confianca), (X, ldt.MEDIA))
        self.assertIn('escala a 20 dias', r.motivos[-1])

    def test_duas_familias(self):
        candidatos = [self.candidato(M, dias=3), self.candidato(X, dias=1)]
        r = cdc.resolve_ces(self.DESEMBARQUE, candidatos)
        self.assertEqual((r.principal, r.confianca), (X, ldt.BAIXA))
        r = cdc.resolve_ces(self.DESEMBARQUE, candidatos, declarados=[M])
        self.assertEqual((r.principal, r.confianca), (M, ldt.ALTA))

    def test_familia_inclui_master_sem_o_conteiner(self):
        candidatos = [self.candidato(H1, M)]
        r = cdc.resolve_ces(self.DESEMBARQUE, candidatos)
        self.assertEqual(r.ces, {H1: 'filhote'})
        self.assertEqual(cdc.familia(r, candidatos), {H1, M})


    def test_familia_inclui_irmaos_sem_o_conteiner(self):
        irmao = cdc.CandidatoCE(H2, M, '5', (date(2026, 9, 12),), tem_conteiner=False)
        candidatos = [self.candidato(H1, M), irmao]
        r = cdc.resolve_ces(self.DESEMBARQUE, candidatos)
        self.assertEqual(r.ces, {H1: 'filhote'})   # o irmão não entra na escolha (não tem o contêiner)
        self.assertEqual(cdc.familia(r, candidatos), {H1, M, H2})
        r = cdc.resolve_ces(self.DESEMBARQUE, [self.candidato(M), irmao])   # escolhido o master (simples)
        self.assertEqual(cdc.familia(r, [self.candidato(M), irmao]), {M, H2})


class TestProcessaConteiner(unittest.TestCase):

    def test_duas_passagens_corroboram_pela_familia(self):
        candidatos = [cdc.CandidatoCE(M, None, '5', (date(2026, 9, 25),)),
                      cdc.CandidatoCE(H1, M, '5', (date(2026, 9, 25),))]
        eventos = [D(t(9, 23, 12), ce=M), S(t(9, 24, 8)), INSP(t(9, 24, 10), ECO, ce=H1),
                   INSP(t(9, 24, 11), ECO, ce=X)]
        [(ciclo, ces)] = processa_conteiner(eventos, candidatos, ESCOPO, OPERADORES, AGORA)
        self.assertEqual((ces.principal, ces.confianca), (H1, ldt.ALTA))   # declarado (M) é da família
        self.assertEqual([e.confianca for e in ciclo.escaneamentos], [ldt.ALTA, ldt.BAIXA])
        self.assertEqual(ciclo.situacao, ldt.CONCLUIDO)

    def test_inspecao_declarando_irmao_e_da_familia(self):
        # sombra de 01/10 (ECMU7308179): inspeção no cais 10 min depois do desembarque, CE de um irmão
        candidatos = [cdc.CandidatoCE(H1, M, '5', (date(2026, 9, 7),)),
                      cdc.CandidatoCE(H2, M, '5', (date(2026, 9, 7),), tem_conteiner=False)]
        eventos = [D(t(9, 7, 17), ce=H1), INSP(t(9, 7, 17, 10), SB, ce=H2), S(t(9, 14, 23))]
        [(ciclo, ces)] = processa_conteiner(eventos, candidatos, ESCOPO, OPERADORES, AGORA)
        self.assertEqual(ces.principal, H1)
        self.assertEqual([e.confianca for e in ciclo.escaneamentos], [ldt.ALTA])


if __name__ == '__main__':
    unittest.main()
