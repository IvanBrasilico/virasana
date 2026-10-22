"""PORTO do desembarque na escolha do CE do ciclo de vida (22/10/2026; CICLO_DE_VIDA.md, "Caso CAAU5545018").

Regras (`ces_do_ciclo.resolve_ces(..., porto=)` e `carga_ciclo.CargaCiclo`):
  - o CE cuja carga SAI do porto do desembarque (origem = porto) não é candidato;
  - as datas são as das escalas no porto do desembarque (sem escala nele, todas; porto desconhecido: Santos);
  - entre várias famílias plausíveis sem declaração que desempate, a de destino no porto vence a escala mais próxima;
  - o porto de cada desembarque é o da escala declarada nele; sem ela, o mais visto no recinto no lote;
  - o manifesto sem escala ligada no Mercante fica com o início da operação no porto de descarregamento.
"""
import unittest
from datetime import date, datetime

from virasana.analytic_scripts.ciclo_de_vida import ces_do_ciclo as cdc
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import banco_do_caso
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import CargaCiclo, ConsultaSQL
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import CandidatoCE, resolve_ces

DESEMBARQUE = datetime(2026, 10, 7, 16)
IMP, CAB, OUTRO = '152600000000001', '072600000000002', '152600000000003'
EXP_VIX = '122607000000004'


def candidato(ce, *escalas, origem='CNSHA', destino='BRSSZ', trafego='5', master=None, manifesto=None):
    return CandidatoCE(ce, master, trafego, escalas=tuple(escalas), origem=origem, destino=destino,
                       manifesto=manifesto)


class TestCargaQueSai(unittest.TestCase):

    def test_cabotagem_saindo_do_porto_e_descartada(self):
        cabotagem = candidato(CAB, (date(2026, 10, 15), 'BRSSZ'), origem='BRSSZ', destino='BRSUA', trafego='3')
        importacao = candidato(IMP)   # sem escala (manifesto sem escala ligada)
        resolvidos = resolve_ces(DESEMBARQUE, [cabotagem, importacao], [IMP], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.confianca), (IMP, cdc.MEDIA))
        self.assertIn('1 CE(s) com a carga saindo de BRSSZ descartado(s)', resolvidos.motivos)

    def test_sem_porto_vale_a_regra_de_antes(self):
        cabotagem = candidato(CAB, (date(2026, 10, 15), 'BRSSZ'), origem='BRSSZ', destino='BRSUA', trafego='3')
        resolvidos = resolve_ces(DESEMBARQUE, [cabotagem, candidato(IMP)], [IMP])
        self.assertEqual(resolvidos.principal, CAB)   # o caso CAAU antes da correção

    def test_cabotagem_que_chega_ao_porto_continua_candidata(self):
        chegando = candidato(CAB, (date(2026, 10, 6), 'BRSSZ'), origem='BRSUA', destino='BRSSZ', trafego='3')
        self.assertEqual(resolve_ces(DESEMBARQUE, [chegando], porto='BRSSZ').principal, CAB)

    def test_sem_candidato_que_fica(self):
        saindo = candidato(CAB, (date(2026, 10, 7), 'BRSSZ'), origem='BRSSZ', trafego='3')
        resolvidos = resolve_ces(DESEMBARQUE, [saindo], [CAB], porto='BRSSZ')
        self.assertIsNone(resolvidos.principal)


class TestDatasNoPorto(unittest.TestCase):

    def test_datas_no_porto(self):
        escalas = [(date(2026, 10, 1), 'BRSSZ'), (date(2026, 10, 9), 'BRRIO'), (date(2026, 10, 3), 'BRSSZ')]
        self.assertEqual(cdc.datas_no_porto(escalas, 'BRRIO'), [date(2026, 10, 9)])
        self.assertEqual(cdc.datas_no_porto(escalas), [date(2026, 10, 1), date(2026, 10, 3)])   # desconhecido: Santos
        self.assertEqual(cdc.datas_no_porto(escalas, 'BRSUA'), [d for d, _ in escalas])   # sem escala nele: todas
        self.assertEqual(cdc.datas_no_porto([], 'BRSSZ'), [])

    def test_escala_do_porto_do_desembarque_decide_a_janela(self):
        """Manifesto que passa por Santos (30 dias depois) e pelo Rio (no dia): no Rio é plausível; em Santos não."""
        rio = candidato(IMP, (date(2026, 11, 6), 'BRSSZ'), (date(2026, 10, 7), 'BRRIO'), destino='BRRIO')
        self.assertEqual(resolve_ces(DESEMBARQUE, [rio], porto='BRRIO').confianca, cdc.MEDIA)
        self.assertIsNone(resolve_ces(DESEMBARQUE, [rio], porto='BRSSZ').principal)

    def test_candidato_sem_escalas_usa_as_datas_de_antes(self):
        antigo = CandidatoCE(IMP, None, '5', (date(2026, 10, 7),))
        self.assertEqual(cdc.datas_do_candidato(antigo, 'BRRIO'), [date(2026, 10, 7)])
        self.assertEqual(resolve_ces(DESEMBARQUE, [antigo], porto='BRRIO').principal, IMP)


class TestDestinoDesempata(unittest.TestCase):

    def setUp(self):
        self.passando = candidato(OUTRO, (date(2026, 10, 7), 'BRSSZ'), destino='BRPNG')   # escala mais próxima
        self.ficando = candidato(IMP, (date(2026, 10, 5), 'BRSSZ'), destino='BRSSZ')

    def test_destino_no_porto_vence_a_escala_mais_proxima(self):
        resolvidos = resolve_ces(DESEMBARQUE, [self.passando, self.ficando], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.confianca), (IMP, cdc.MEDIA))
        self.assertEqual(resolvidos.motivos, ['mais de uma família possível; escolhida a de destino em BRSSZ'])

    def test_declarado_vence_o_destino(self):
        resolvidos = resolve_ces(DESEMBARQUE, [self.passando, self.ficando], [OUTRO], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.confianca), (OUTRO, cdc.ALTA))

    def test_sem_porto_ou_com_dois_no_destino_vale_a_escala_mais_proxima(self):
        self.assertEqual(resolve_ces(DESEMBARQUE, [self.passando, self.ficando]).principal, OUTRO)
        sem_destino = candidato(IMP, (date(2026, 10, 5), 'BRSSZ'), destino=None)
        self.assertEqual(resolve_ces(DESEMBARQUE, [self.passando, sem_destino]).confianca, cdc.BAIXA)
        outro_ficando = candidato('152600000000009', (date(2026, 10, 3), 'BRSSZ'), destino='BRSSZ')
        resolvidos = resolve_ces(DESEMBARQUE, [self.passando, self.ficando, outro_ficando], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.confianca), (OUTRO, cdc.BAIXA))


class TestConteinerConsolidado(unittest.TestCase):
    """C7 (23/10/2026, aprovado pelo usuário): BLs únicos do MESMO manifesto com o mesmo contêiner são uma família —
    todos no ciclo (o NAVI-I acha o ciclo pelo card de qualquer um)."""

    def setUp(self):
        perto = (date(2026, 10, 6), 'BRSSZ')
        self.a, self.b = candidato(IMP, perto, manifesto='MI'), candidato(OUTRO, perto, manifesto='MI')
        self.outro_manifesto = candidato('152600000000005', perto, manifesto='MX')

    def test_uma_familia_com_o_declarado_de_principal(self):
        resolvidos = resolve_ces(DESEMBARQUE, [self.a, self.b], [OUTRO], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.confianca), (OUTRO, cdc.ALTA))
        self.assertEqual(resolvidos.ces, {IMP: cdc.SIMPLES, OUTRO: cdc.SIMPLES})

    def test_sem_declaracao_o_primeiro_e_media(self):
        resolvidos = resolve_ces(DESEMBARQUE, [self.b, self.a], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.confianca), (IMP, cdc.MEDIA))

    def test_manifestos_diferentes_continuam_familias_diferentes(self):
        resolvidos = resolve_ces(DESEMBARQUE, [self.a, self.outro_manifesto], porto='BRSSZ')
        self.assertEqual((len(resolvidos.ces), resolvidos.confianca), (1, cdc.BAIXA))

    def test_master_e_filhotes_nao_se_misturam_com_bl_unico_do_manifesto(self):
        perto = (date(2026, 10, 6), 'BRSSZ')
        master = candidato('152600000000010', perto, manifesto='MI')
        filhote = candidato('152600000000011', perto, master='152600000000010', manifesto='MI')
        resolvidos = resolve_ces(DESEMBARQUE, [master, filhote, self.a], ['152600000000011'], porto='BRSSZ')
        self.assertEqual(resolvidos.ces, {'152600000000010': cdc.MASTER, '152600000000011': cdc.FILHOTE})

    def test_sem_manifesto_cada_ce_e_uma_familia(self):
        sem = [candidato(IMP, (date(2026, 10, 6), 'BRSSZ')), candidato(OUTRO, (date(2026, 10, 6), 'BRSSZ'))]
        self.assertEqual(resolve_ces(DESEMBARQUE, sem, porto='BRSSZ').confianca, cdc.BAIXA)


class TestExportacaoEmTransito(unittest.TestCase):
    """C6 (23/10/2026, aprovado pelo usuário): exportação de OUTRO porto com escala no porto do desembarque (baldeação,
    ex.: Vitória -> Benin via Santos) é a carga seguindo viagem — não entra no ciclo, mas corrobora o embarque."""

    def exportacao(self, origem='BRVIX', *escalas):
        return candidato(EXP_VIX, *(escalas or [(date(2026, 10, 6), 'BRSSZ')]), origem=origem, destino='BJCOO',
                         trafego='7')

    def test_em_transito_vai_para_o_seguimento(self):
        resolvidos = resolve_ces(DESEMBARQUE, [self.exportacao()], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.seguimento), (None, [EXP_VIX]))
        self.assertIn('1 exportação(ões) de outro porto em trânsito (seguimento)', resolvidos.motivos)
        self.assertIn(EXP_VIX, cdc.familia(resolvidos))

    def test_saida_e_transito_juntos_no_seguimento(self):
        saindo = candidato(CAB, (date(2026, 10, 15), 'BRSSZ'), origem='BRSSZ', trafego='3')
        resolvidos = resolve_ces(DESEMBARQUE, [saindo, self.exportacao()], porto='BRSSZ')
        self.assertEqual(resolvidos.seguimento, sorted([CAB, EXP_VIX]))

    def test_o_que_nao_e_transito(self):
        casos = {'exportação do próprio porto': (self.exportacao('BRSSZ'), 'BRSSZ'),
                 'escala longe': (self.exportacao('BRVIX', (date(2026, 2, 3), 'BRSSZ')), 'BRSSZ'),
                 'porto desconhecido': (self.exportacao(), None),
                 'sem escala': (candidato(EXP_VIX, origem='BRVIX', trafego='7'), 'BRSSZ'),
                 'sem origem': (self.exportacao(''), 'BRSSZ')}
        for nome, (exportacao, porto) in casos.items():
            with self.subTest(caso=nome):
                self.assertEqual(resolve_ces(DESEMBARQUE, [exportacao], porto=porto).seguimento, [])

    def test_baldeacao_fecha_como_passagem(self):
        """O caso real (MRSU7476660): uma cabotagem ANTIGA do mesmo contêiner saindo de Santos (junho) não é a perna
        seguinte desta visita; a exportação de Vitória em trânsito é, e o embarque que a declara fecha a passagem."""
        from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
        from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import processa_conteiner
        sb = '8931356'
        antiga = candidato(CAB, (date(2026, 6, 10), 'BRSSZ'), origem='BRSSZ', destino='BRMAO', trafego='3')
        eventos = [ldt.Evento(ldt.DESEMBARQUE, 1, DESEMBARQUE, sb, ce_declarado=EXP_VIX),
                   ldt.Evento(ldt.EMBARQUE, 2, datetime(2026, 10, 9, 10), sb, ce_declarado=EXP_VIX)]
        [(ciclo, ces)] = processa_conteiner(eventos, [self.exportacao(), antiga], {sb}, {sb}, datetime(2026, 10, 21),
                                            conhecidos={EXP_VIX, CAB}, portos={1: 'BRSSZ'})
        self.assertEqual((ces.principal, ces.seguimento), (None, [EXP_VIX]))
        self.assertEqual((ciclo.fluxo, ciclo.situacao), (ldt.PASSAGEM, ldt.CONCLUIDO))


class TestPernaSeguinteNaJanela(unittest.TestCase):

    def test_so_a_saida_com_escala_na_janela_e_perna_seguinte(self):
        perto = candidato(CAB, (date(2026, 10, 15), 'BRSSZ'), origem='BRSSZ', trafego='3')
        longe = candidato('072600000000009', (date(2026, 6, 10), 'BRSSZ'), origem='BRSSZ', trafego='3')
        resolvidos = resolve_ces(DESEMBARQUE, [perto, longe, candidato(IMP)], [IMP], porto='BRSSZ')
        self.assertEqual((resolvidos.principal, resolvidos.seguimento), (IMP, [CAB]))
        self.assertIn('2 CE(s) com a carga saindo de BRSSZ descartado(s)', resolvidos.motivos)


class TestBaldeacao(unittest.TestCase):
    """Desembarca em Santos e logo embarca na cabotagem para Suape: o ciclo é do CE de importação e o embarque, que
    declara o CE da cabotagem (a perna seguinte), fecha a passagem — não é "CE de outra família"."""

    def processa(self, portos):
        from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
        from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import processa_conteiner
        btp = '8931359'
        eventos = [ldt.Evento(ldt.DESEMBARQUE, 1, DESEMBARQUE, btp),
                   ldt.Evento(ldt.EMBARQUE, 2, datetime(2026, 10, 9, 15), btp, ce_declarado=CAB)]
        candidatos = [candidato(IMP, (date(2026, 10, 5), 'BRSSZ')),
                      candidato(CAB, (date(2026, 10, 8), 'BRSSZ'), origem='BRSSZ', destino='BRSUA', trafego='3')]
        [(ciclo, ces)] = processa_conteiner(eventos, candidatos, {btp}, {btp}, datetime(2026, 10, 21),
                                            conhecidos={IMP, CAB}, portos=portos)
        return ciclo, ces, ldt

    def test_embarque_com_o_ce_da_perna_seguinte_fecha_a_passagem(self):
        ciclo, ces, ldt = self.processa({1: 'BRSSZ'})
        self.assertEqual((ces.principal, ces.seguimento), (IMP, [CAB]))
        self.assertEqual((ciclo.situacao, ciclo.fluxo), (ldt.CONCLUIDO, ldt.PASSAGEM))
        self.assertIn(CAB, cdc.familia(ces))

    def test_sem_porto_a_cabotagem_declarada_continua_vencendo(self):
        _, ces, _ = self.processa({})
        self.assertEqual((ces.principal, ces.seguimento), (CAB, []))


def _evento(id_, conteiner, recinto, escala=None):
    return {'id': id_, 'numeroConteiner': conteiner, 'dataHoraOcorrencia': '2026-10-07T16:00:00',
            'codigoRecinto': recinto, 'tipoOperacao': 'I', 'embarqueDesembarque': 'D', 'escala': escala}


CASO = {
    'recintos': [{'id': 1, 'nome': 'SBP', 'cod_siscomex': '8931356', 'e_operador': 1, 'cod_unidade': '0817800'},
                 {'id': 2, 'nome': 'MULTIRIO', 'cod_siscomex': '7921303', 'e_operador': 1, 'cod_unidade': '0717600'}],
    'tabelas': {
        'apirecintos_embarquedesembarque': [
            _evento(1, 'C1', '8931356', 'ES'), _evento(2, 'C2', '8931356'), _evento(3, 'C3', '7921303'),
            _evento(4, 'C4', '8931356', 'FORA'),    # escala fora do escalasresumo: vale o porto do recinto
            _evento(5, 'C5', '8931356', 'ER'),      # escala de outro porto no mesmo recinto: vale a do evento
            _evento(6, 'C6', '8931356', 'ES')],
        'escalasresumo': [{'numeroDaEscala': 'ES', 'portoDaEscala': 'BRSSZ', 'dataPrevistaAtracacao': '2026-10-07'},
                          {'numeroDaEscala': 'EC', 'portoDaEscala': 'BRSSZ', 'dataPrevistaAtracacao': '2026-10-15'},
                          {'numeroDaEscala': 'EC2', 'portoDaEscala': 'BRSUA', 'dataPrevistaAtracacao': '2026-10-27'},
                          {'numeroDaEscala': 'ER', 'portoDaEscala': 'BRRIO', 'dataPrevistaAtracacao': '2026-10-07'}],
        'escalamanifestoresumo': [{'ID': 1, 'manifesto': 'MC', 'escala': 'EC'},
                                  {'ID': 2, 'manifesto': 'MC', 'escala': 'EC2'}],
        'manifestosresumo': [{'numero': 'MI', 'portoDescarregamento': 'BRSSZ', 'dataInicioOperacao': '2026-10-07'},
                             {'numero': 'MC', 'portoDescarregamento': 'BRSUA', 'dataInicioOperacao': '2026-10-15'},
                             {'numero': 'MX', 'portoDescarregamento': 'BRSSZ', 'dataInicioOperacao': ''}],
        'conhecimentosresumo': [
            {'numeroCEmercante': IMP, 'tipoTrafego': '5', 'manifestoCE': 'MI', 'portoOrigemCarga': 'EGDAM',
             'portoDestFinal': 'BRSSZ'},
            {'numeroCEmercante': CAB, 'tipoTrafego': '3', 'manifestoCE': 'MC', 'portoOrigemCarga': 'BRSSZ',
             'portoDestFinal': 'BRSUA'},
            {'numeroCEmercante': OUTRO, 'tipoTrafego': '5', 'manifestoCE': 'MX', 'portoOrigemCarga': 'CNSHA',
             'portoDestFinal': 'BRSSZ'}],
        'itensresumo': [{'numeroCEmercante': IMP, 'codigoConteiner': 'C1'},
                        {'numeroCEmercante': CAB, 'codigoConteiner': 'C1'},
                        {'numeroCEmercante': OUTRO, 'codigoConteiner': 'C2'}],
    },
}


class TestCarga(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with banco_do_caso(CASO).connect() as conexao:
            cls.carga = CargaCiclo(ConsultaSQL(conexao), datetime(2026, 10, 1), datetime(2026, 10, 21))
            cls.carga.eventos(['C1', 'C2', 'C3', 'C4', 'C5', 'C6'])
            cls.dos_recintos = cls.carga.portos_dos_recintos(datetime(2026, 10, 1))
            cls.so_do_rio = cls.carga.portos_dos_recintos(datetime(2026, 10, 1), ['7921303'])
            cls.nenhum = cls.carga.portos_dos_recintos(datetime(2026, 10, 1), [])
            cls.depois = cls.carga.portos_dos_recintos(datetime(2026, 10, 8))
            cls.com_mapa = cls.carga.portos_dos_desembarques({'7921303': 'BRRIO', '8931356': 'BRXXX'})
            cls.portos = cls.carga.portos_dos_desembarques()
            cls.candidatos = cls.carga.candidatos(['C1', 'C2'])

    def test_porto_pela_escala_declarada_e_pelo_recinto(self):
        self.assertEqual(self.portos, {1: 'BRSSZ', 2: 'BRSSZ', 3: None, 4: 'BRSSZ', 5: 'BRRIO', 6: 'BRSSZ'})

    def test_porto_de_cada_recinto_pelas_escalas_do_periodo(self):
        self.assertEqual(self.dos_recintos, {'8931356': 'BRSSZ'})   # 2 escalas em Santos x 1 no Rio: a mais vista
        self.assertEqual((self.so_do_rio, self.nenhum, self.depois), ({}, {}, {}))   # o Rio não declara escala

    def test_mapa_do_recinto_vem_antes_do_lote_e_depois_da_escala_do_evento(self):
        self.assertEqual(self.com_mapa, {1: 'BRSSZ', 2: 'BRXXX', 3: 'BRRIO', 4: 'BRXXX', 5: 'BRRIO', 6: 'BRSSZ'})

    def test_candidatos_com_origem_destino_e_escalas_com_porto(self):
        por_ce = {c.ce: c for c in self.candidatos['C1']}
        self.assertEqual(por_ce[CAB].escalas, ((date(2026, 10, 15), 'BRSSZ'), (date(2026, 10, 27), 'BRSUA')))
        self.assertEqual((por_ce[CAB].origem, por_ce[CAB].destino), ('BRSSZ', 'BRSUA'))
        self.assertEqual(por_ce[CAB].datas_escala, (date(2026, 10, 15),))   # as de Santos, como antes

    def test_manifesto_sem_escala_fica_com_o_inicio_da_operacao(self):
        por_ce = {c.ce: c for c in self.candidatos['C1']}
        self.assertEqual(por_ce[IMP].escalas, ((date(2026, 10, 7), 'BRSSZ'),))
        self.assertEqual(self.candidatos['C2'][0].escalas, ())   # sem escala e sem início da operação


class TestServicoPortos(unittest.TestCase):
    """O serviço recalcula o porto de cada recinto uma vez por HORAS_PORTOS; o reprocesso de um caso usa o mapa
    guardado; uma falha da consulta não para o ciclo."""

    def setUp(self):
        from unittest import mock
        from virasana.analytic_scripts.ciclo_de_vida import servico_ciclo_de_vida as sv
        self.sv, self.mock = sv, mock
        self.agora = [datetime(2026, 10, 21, 12)]
        self.engine = banco_do_caso(CASO)

    def servico(self, **kwargs):
        return self.sv.Servico(self.engine, relogio=lambda: self.agora[0], **kwargs)

    def test_cache_por_dia(self):
        servico = self.servico()
        with self.engine.connect() as conexao, self.mock.patch.object(
                CargaCiclo, 'portos_dos_recintos', return_value={'8931356': 'BRSSZ'}) as consulta:
            self.assertEqual(servico.portos_dos_recintos(conexao), {'8931356': 'BRSSZ'})
            self.agora[0] = datetime(2026, 10, 22, 11)
            servico.portos_dos_recintos(conexao)
            self.assertEqual(consulta.call_count, 1)
            self.agora[0] = datetime(2026, 10, 22, 13)
            servico.portos_dos_recintos(conexao)
            self.assertEqual(consulta.call_count, 2)
            self.assertEqual(consulta.call_args[0][0], datetime(2026, 10, 15, 13))   # 7 dias

    def test_mapa_fixo_do_caso(self):
        with self.engine.connect() as conexao, self.mock.patch.object(CargaCiclo, 'portos_dos_recintos') as consulta:
            self.assertEqual(self.servico(portos_dos_recintos={'X': 'BRPEC'}).portos_dos_recintos(conexao),
                             {'X': 'BRPEC'})
            self.assertEqual(consulta.call_count, 0)

    def test_falha_nao_para_o_ciclo(self):
        with self.engine.connect() as conexao, self.mock.patch.object(
                CargaCiclo, 'portos_dos_recintos', side_effect=RuntimeError('caiu')), \
                self.assertLogs(level='ERROR'):
            self.assertEqual(self.servico().portos_dos_recintos(conexao), {})


if __name__ == '__main__':
    unittest.main()
