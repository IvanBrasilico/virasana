# coding: utf-8
"""Escaneamento só com IMAGEM (decisão do usuário, 15/10/2026): o escâner escaneou e a inspeção não foi transmitida —
a imagem do contêiner no Mongo no horário da passagem por outro recinto (ou da chegada ao seco) vira escaneamento do
ciclo, com `escaneamento_id` negativo. Núcleo (janelas), Mongo (imagens) e gravação pelo serviço.

Cenário do serviço (test_servico_ciclo_de_vida) com mais dois contêineres:
  CFIN  desembarca na BTP, sai, passa 10 min no ECOPORTO (sem inspeção) e some: imagem 4 min depois da entrada
  CSEC  desembarca na BTP, sai e entra no EADI (TRA) sem inspeção: imagem 30 min depois da entrada
"""
import unittest
from datetime import datetime, timedelta

from bson import ObjectId
from sqlalchemy import select, text

from virasana.analytic_scripts.ciclo_de_vida import gravacao_ciclo as gc
from virasana.analytic_scripts.ciclo_de_vida import imagens_ciclo as ic
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
from virasana.tests.test_servico_ciclo_de_vida import AGORA, BTP, DESDE, EADI, ECO, Relogio, _evento, _insere, d, \
    monta_banco

from bhadrasana.models.apirecintos import AcessoVeiculo, EmbarqueDesembarque, InspecaoNaoInvasiva  # noqa: I100

IMAGEM_PASSAGEM, IMAGEM_CHEGADA = ObjectId(), ObjectId()


def ciclo_com(passagens=(), escaneamentos=(), entrada_seco=None, saida_seco=None) -> ldt.CicloCalculado:
    ciclo = ldt.CicloCalculado(ldt.Evento(ldt.DESEMBARQUE, 1, d(1), BTP), passagens=list(passagens))
    ciclo.escaneamentos = [ldt.Ligacao(ldt.Evento(ldt.INSPECAO, 9, quando, ECO), confianca)
                           for quando, confianca in escaneamentos]
    if entrada_seco:
        ciclo.entrada_seco = ldt.Ligacao(ldt.Evento(ldt.ENTRADA, 2, entrada_seco, EADI), ldt.ALTA)
    if saida_seco:
        ciclo.saida_seco = ldt.Ligacao(ldt.Evento(ldt.SAIDA, 3, saida_seco, EADI), ldt.ALTA)
    return ciclo


class TestJanelas(unittest.TestCase):

    def test_passagem_sem_inspecao(self):
        [janela] = ic.janelas_sem_inspecao(ciclo_com(passagens=[(ECO, d(4, 9), d(4, 9, 10))]))
        self.assertEqual(janela, ic.Janela(ECO, d(4, 8, 45), d(4, 9, 25), True))

    def test_passagem_com_inspecao_ligada_nao_procura(self):
        # inclusive a do Ecoporto transmitida com o código do TRA de destino (scanner no gate)
        self.assertEqual(ic.janelas_sem_inspecao(ciclo_com(passagens=[(ECO, d(4, 9), d(4, 9, 10))],
                                                           escaneamentos=[(d(4, 9, 5), ldt.MEDIA)])), [])
        # a de confiança baixa (outra família) não conta
        self.assertEqual(len(ic.janelas_sem_inspecao(ciclo_com(passagens=[(ECO, d(4, 9), d(4, 9, 10))],
                                                               escaneamentos=[(d(4, 9, 5), ldt.BAIXA)]))), 1)

    def test_chegada_ao_seco(self):
        [janela] = ic.janelas_sem_inspecao(ciclo_com(entrada_seco=d(4, 10)))
        self.assertEqual(janela, ic.Janela(EADI, d(4, 8), d(5, 10), False))
        [curta] = ic.janelas_sem_inspecao(ciclo_com(entrada_seco=d(4, 10), saida_seco=d(4, 20)))
        self.assertEqual(curta.fim, d(4, 20, 15))
        self.assertEqual(ic.janelas_sem_inspecao(ciclo_com(entrada_seco=d(4, 10),
                                                           escaneamentos=[(d(6, 10), ldt.ALTA)])), [])

    def test_id_da_imagem_negativo_e_estavel(self):
        imagem = '6704f1a2b3c4d5e6f7a8b9c0'
        self.assertEqual(ic.id_da_imagem(imagem), -int('3c4d5e6f7a8b9c0', 16))   # os 15 últimos (60 bits: cabe)
        self.assertLess(ic.id_da_imagem(imagem), 0)
        self.assertGreater(ic.id_da_imagem('f' * 24), -2 ** 63)
        self.assertEqual(ic.id_da_imagem(imagem), ic.id_da_imagem(str(imagem)))


class TestNucleoGuardaPassagens(unittest.TestCase):

    def test_passagem_pelo_operador_e_pelo_seco(self):
        ops = {BTP, ECO}
        eventos = [ldt.Evento(ldt.DESEMBARQUE, 1, d(1), BTP), ldt.Evento(ldt.SAIDA, 2, d(2), BTP),
                   ldt.Evento(ldt.ENTRADA, 3, d(2, 9), ECO), ldt.Evento(ldt.SAIDA, 4, d(2, 9, 10), ECO)]
        [ciclo] = ldt.ciclos_do_conteiner(eventos, ops, ops, AGORA)
        self.assertEqual(ciclo.passagens, [(ECO, d(2, 9), d(2, 9, 10))])
        rapida = eventos[:2] + [ldt.Evento(ldt.ENTRADA, 3, d(2, 9), EADI), ldt.Evento(ldt.SAIDA, 4, d(2, 9, 30), EADI),
                                ldt.Evento(ldt.ENTRADA, 5, d(2, 11), '8931339')]
        [ciclo] = ldt.ciclos_do_conteiner(rapida, ops, ops, AGORA)
        self.assertEqual(ciclo.passagens, [(EADI, d(2, 9), d(2, 9, 30))])


class TestImagensNoMongo(unittest.TestCase):

    def setUp(self):
        _, self.mongo = monta_banco()
        for imagem, quando, conteiner in ((IMAGEM_PASSAGEM, d(4, 12, 4), 'CFIN'), (ObjectId(), d(4, 14), 'CFIN')):
            self.mongo['fs.files'].insert_one({'_id': imagem, 'metadata': {'numeroinformado': conteiner,
                                                                           'dataescaneamento': quando}})

    def test_primeira_imagem_da_janela_com_a_defasagem_do_recinto(self):
        janela = ic.Janela(ECO, d(4, 8, 45), d(4, 9, 25), True)       # Ecoporto: +3 h
        self.assertEqual(ic.imagens_sem_inspecao(self.mongo, 'CFIN', [janela]),
                         [(janela, d(4, 9, 4), str(IMAGEM_PASSAGEM))])
        self.assertEqual(ic.imagens_sem_inspecao(self.mongo, 'OUTRO', [janela]), [])
        self.assertEqual(ic.imagens_sem_inspecao(None, 'CFIN', [janela]), [])


def acrescenta(engine, mongo):
    acesso = dict(operacao='C', vazioConteiner=False)
    with engine.begin() as c:
        _insere(c, EmbarqueDesembarque, [_evento(801, 'CFIN', d(3), BTP, embarqueDesembarque='D'),
                                         _evento(802, 'CSEC', d(3), BTP, embarqueDesembarque='D')])
        _insere(c, AcessoVeiculo, [
            _evento(811, 'CFIN', d(4, 8), BTP, direcao='S', **acesso),
            _evento(812, 'CFIN', d(4, 9), ECO, direcao='E', **acesso),
            _evento(813, 'CFIN', d(4, 9, 10), ECO, direcao='S', **acesso),
            _evento(814, 'CSEC', d(4, 8), BTP, direcao='S', **acesso),
            _evento(815, 'CSEC', d(4, 10), EADI, direcao='E', **acesso)])
    for imagem, quando, conteiner in ((IMAGEM_PASSAGEM, d(4, 12, 4), 'CFIN'),     # Ecoporto +3 h: 09:04
                                      (IMAGEM_CHEGADA, d(4, 13, 30), 'CSEC')):    # EADI sem medição: +3 h → 10:30
        mongo['fs.files'].insert_one({'_id': imagem, 'metadata': {'numeroinformado': conteiner,
                                                                  'dataescaneamento': quando}})


class TestGravacao(unittest.TestCase):

    def setUp(self):
        self.engine, self.mongo = monta_banco()
        acrescenta(self.engine, self.mongo)
        self.processa(self.mongo)

    def processa(self, mongo):
        sv.Servico(self.engine, mongo, relogio=Relogio(AGORA)).processa(['CFIN', 'CSEC'], desde=DESDE)

    def escaneamentos(self, conteiner):
        with self.engine.connect() as c:
            return [dict(linha._mapping) for linha in c.execute(
                select(gc.ESCANEAMENTOS).join(gc.CICLOS, gc.CICLOS.c.ID == gc.ESCANEAMENTOS.c.ciclo_de_vida_id)
                .where(gc.CICLOS.c.numeroConteiner == conteiner).order_by(gc.ESCANEAMENTOS.c.escaneamento_id))]

    def test_passagem_sem_inspecao_vira_escaneamento_fora_so_com_imagem(self):
        [esc] = self.escaneamentos('CFIN')
        self.assertEqual((esc['escaneamento_id'], esc['imagem_id'], esc['codigoRecinto'], esc['confianca'],
                          bool(esc['fora'])),
                         (ic.id_da_imagem(str(IMAGEM_PASSAGEM)), str(IMAGEM_PASSAGEM), ECO, ldt.MEDIA, True))
        self.assertEqual(sv._data(esc['data_escaneamento']), d(4, 9, 4))
        self.assertEqual(esc['motivo'], f'{ic.SEM_INSPECAO}; na passagem pelo recinto {ECO} (escaneamento fora)')

    def test_chegada_ao_seco_sem_inspecao(self):
        [esc] = self.escaneamentos('CSEC')
        self.assertLess(esc['escaneamento_id'], 0)
        self.assertEqual((esc['codigoRecinto'], bool(esc['fora'])), (EADI, False))
        self.assertIn('na chegada ao recinto seco', esc['motivo'])

    def test_reprocessar_nao_duplica_e_sem_mongo_mantem(self):
        self.processa(self.mongo)
        self.processa(None)
        self.assertEqual([len(self.escaneamentos(c)) for c in ('CFIN', 'CSEC')], [1, 1])

    def test_inspecao_que_chega_depois_substitui_a_imagem_sem_inspecao(self):
        with self.engine.begin() as c:
            _insere(c, InspecaoNaoInvasiva, [_evento(901, 'CSEC', d(4, 10, 30), EADI, vazio=False)])
        self.processa(self.mongo)
        [esc] = self.escaneamentos('CSEC')
        self.assertEqual((esc['escaneamento_id'], esc['imagem_id']), (901, str(IMAGEM_CHEGADA)))

    def test_contagem_na_volta(self):
        with self.engine.connect() as c:
            negativos = c.execute(text('SELECT COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos '
                                       'WHERE escaneamento_id < 0')).scalar()
        self.assertEqual(negativos, 2)


if __name__ == '__main__':
    unittest.main()
