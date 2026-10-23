"""Sinais de declaração inventada — regras puras (analytic_scripts/declaracao/regras_declaracao.py; itens 16, 17 e 18
do BACKLOG.md, plano aprovado em 23/10/2026; 2ª versão depois da 1ª rodada em produção). Casos montados com o formato
real do Mercante (números em texto com 3 casas, CEs de 15 dígitos, famílias master/filhote).
"""
from virasana.navii.busca_navii import tabela_recintos  # noqa: F401 — 1º: ajusta o sys.path
import unittest  # noqa: I100
from collections import Counter

from virasana.analytic_scripts.declaracao import regras_declaracao as rd

MBL, HBL1, HBL2 = '152605000000011', '152605000000012', '152605000000013'
MBL3, MHBL3, HBL3 = '152605000000021', '152605000000022', '152605000000023'


def item(ce, conteiner, tipo='22G1', peso='10000.000', cubagem='25.000', embalagem='', uso=''):
    return {'ce': ce, 'conteiner': conteiner, 'tipo': tipo, 'peso': peso, 'cubagem': cubagem,
            'embalagem': embalagem, 'uso_parcial': uso}


def ncm(ce, conteiner, codigo, embalagem='01', volumes=''):
    return {'ce': ce, 'conteiner': conteiner, 'ncm': codigo, 'embalagem': embalagem, 'volumes': volumes}


def conteiner(**campos):
    padrao = dict(conteiner='MSCU1234566', familia='1', ces=('1',), tipo='22G1', chave='22G', peso=10000.0,
                  origem_peso='item do CE', cubagem=25.0, origem_cubagem='item do CE',
                  ncms=(('84713012', '01'),), embalagem='', uso_parcial=False)
    padrao.update(campos)
    return rd.Conteiner(**padrao)


class TestNumerosDoMercante(unittest.TestCase):

    def test_numero(self):
        self.assertEqual([rd.numero(v) for v in ('25040.000', '33,5', '1.234,5', 2350, '', None, 'abc')],
                         [25040.0, 33.5, 1234.5, 2350.0, None, None, None])

    def test_formato(self):
        self.assertEqual([rd.formato(v) for v in ('', '0', '-1', '25040.000', '33,2', '33.25', '1.234,5', '25', 'x')],
                         ['vazio', 'zero', 'negativo', 'decimal com 3 casas (padrão do Mercante)',
                          'decimal com vírgula', 'decimal com ponto', 'milhar e decimal', 'inteiro', 'inválido'])

    def test_br_pct_ncm_e_uso_parcial(self):
        self.assertEqual((rd.br(28300), rd.br(33.2, 1), rd.br(None), rd.pct(0.0123, 2)),
                         ('28.300', '33,2', '-', '1,23%'))
        self.assertEqual((rd.ncm('8471.30.12'), rd.ncm('847'), rd.ncm(None)), ('84713012', None, None))
        self.assertEqual((rd.ncm('9999'), rd.ncm('0000'), rd.ncm('0101')), (None, None, '0101'))   # capítulos 01-97
        self.assertTrue(rd.uso_parcial(' true '))      # o Mercante informa true / false (1ª rodada)
        self.assertFalse(rd.uso_parcial('false'))


class TestTipoECapacidade(unittest.TestCase):

    def test_chave_do_tipo(self):
        self.assertEqual([rd.chave_tipo(c) for c in ('45G1', '22g0', '2210', '40HC', '40DV', '20OT', '45HC', 'L5G1',
                                                     'XPTO', None)],
                         ['45G', '22G', '22G', '45G', '42G', '22U', 'L5G', 'L5G', None, None])

    def test_capacidade_ocupacao_e_carga(self):
        self.assertEqual(rd.capacidade('22G')['m3'], 33.2)
        self.assertIsNone(rd.capacidade('22T'))                  # tanque: sem fonte, fica de fora
        self.assertIsNone(rd.capacidade('_fonte'))
        self.assertAlmostEqual(rd.ocupacao(41.5, '22G'), 1.25)
        self.assertIsNone(rd.ocupacao(41.5, '22T'))
        self.assertIsNone(rd.ocupacao(0, '22G'))
        self.assertAlmostEqual(rd.carga(28300, '22G'), 1.0)


class TestConteineresDaFamilia(unittest.TestCase):

    def test_master_e_filhotes_o_peso_e_a_cubagem_do_master_e_os_ncms_dos_filhotes(self):
        master_de = {MBL: None, HBL1: MBL, HBL2: MBL}
        itens = [item(MBL, 'MSCU1234566', peso='24000.000', cubagem='30.000'),
                 item(HBL1, 'MSCU1234566', peso='8000.000', cubagem='10.000', tipo=''),
                 item(HBL2, 'mscu1234566', peso='16000.000', cubagem='20.000', tipo='')]
        ncms = [ncm(HBL1, 'MSCU1234566', '6109.10.00'), ncm(HBL2, 'MSCU1234566', '61091000')]
        [c] = rd.conteineres(itens, ncms, master_de)
        self.assertEqual((c.familia, c.ces, c.chave, c.peso, c.origem_peso, c.cubagem),
                         (MBL, (MBL, HBL1, HBL2), '22G', 24000.0, 'item do master', 30.0))
        self.assertEqual((c.ncm_unico, c.tamanho), ('61091000', '2'))
        self.assertIsNone(conteiner(ncms=(('97970000', ''),)).ncm_unico)    # bagagem: fora do 18 (7ª versão)
        self.assertIsNone(conteiner(ncms=(('9797', ''),)).ncm_unico)

    def test_sem_o_item_do_master_soma_os_filhotes(self):
        master_de = {HBL1: MBL, HBL2: MBL}
        [c] = rd.conteineres([item(HBL1, 'C1', peso='8000', cubagem='10'),
                              item(HBL2, 'C1', peso='16000', cubagem='')], [], master_de)
        self.assertEqual((c.peso, c.origem_peso, c.cubagem, c.origem_cubagem),
                         (24000.0, 'soma de 2 filhotes', 10.0, 'item do filhote'))

    def test_familia_de_tres_niveis_e_a_do_master_do_topo(self):
        master_de = {MBL3: None, MHBL3: MBL3, HBL3: MHBL3}
        conteineres = rd.conteineres([item(MBL3, 'C3', peso='22000'), item(MHBL3, 'C3', peso='22000'),
                                      item(HBL3, 'C3', peso='22000')], [], master_de)
        self.assertEqual([(c.familia, c.ces, c.peso, c.origem_peso) for c in conteineres],
                         [(MBL3, (MBL3, MHBL3, HBL3), 22000.0, 'item do master')])

    def test_ncm_sem_conteiner_vai_para_o_unico_do_ce_e_uso_parcial(self):
        [c] = rd.conteineres([item('1', 'C1', uso='true')], [ncm('1', '', '84713012'), ncm('1', '', '847')], {})
        self.assertEqual((c.ncms, c.uso_parcial), ((('84713012', '01'),), True))
        dois = rd.conteineres([item('2', 'C1'), item('2', 'C2')], [ncm('2', '', '84713012')], {})
        self.assertEqual([c.ncms for c in dois], [(), ()])       # dois contêineres: o NCM sem contêiner não vai


class TestQualidade16(unittest.TestCase):

    def test_cubagem_e_peso_acima_do_que_o_tipo_comporta(self):
        sinais = dict(rd.sinais_capacidade(conteiner(cubagem=41.5, peso=35000.0)))
        self.assertEqual(set(sinais), {rd.OCUPACAO, rd.CARGA_MAXIMA})
        self.assertIn("41,5 m³ num 20', uso geral (dry) — capacidade de 33,2 m³ (125%)", sinais[rd.OCUPACAO])
        self.assertIn('carga máxima de 28.300 kg (124%)', sinais[rd.CARGA_MAXIMA])
        self.assertEqual(rd.sinais_capacidade(conteiner(cubagem=33.0, peso=30000.0)), [])   # 106%: na folga
        self.assertEqual(rd.sinais_capacidade(conteiner(cubagem=41.5, uso_parcial=True)), [])
        self.assertEqual(rd.sinais_capacidade(conteiner(cubagem=41.5, chave='22T', tipo='22T0')), [])
        self.assertNotIn(rd.OCUPACAO, rd.SINAIS)                  # qualidade do dado, não sinal de risco


class TestCubagemCopiada16e(unittest.TestCase):
    """Pedido do usuário (23/10/2026): vários contêineres com a mesma mercadoria, quantidades diferentes e a mesma
    cubagem — no mínimo, dado inventado."""

    def ce(self, pesos, cubagens, ncms=('3901',), volumes=None, uso=None, sem_conteiner=False):
        itens = [item('X', f'C{n}', tipo='45G1', peso=peso, cubagem=cubagem, uso=(uso or {}).get(n, 'false'))
                 for n, (peso, cubagem) in enumerate(zip(pesos, cubagens))]
        linhas = [ncm('X', '' if sem_conteiner else f'C{n}', codigo, volumes=(volumes or {}).get(n, ''))
                  for n in range(len(pesos)) for codigo in ncms]
        return rd.cubagem_repetida(itens, linhas)

    def test_mesma_cubagem_pesos_diferentes(self):
        avaliavel, repeticao = self.ce(['10000.000', '15000.000', '20000.000'], ['68.000'] * 3)
        self.assertTrue(avaliavel)
        self.assertEqual((repeticao.conteineres, repeticao.cubagem), (3, 68.0))
        self.assertEqual(repeticao.motivo,
                         '3 contêineres com NCM 3901 e a mesma cubagem (68,0 m³), pesos de 10.000 a 20.000 kg')

    def test_mesma_cubagem_e_mesmo_peso_nao_marca(self):
        self.assertEqual(self.ce(['20000.000', '20500.000'], ['68.000'] * 2), (True, None))   # 2,5%: carga igual

    def test_volumes_diferentes_com_o_mesmo_peso(self):
        _, repeticao = self.ce(['20000.000'] * 2, ['68.000'] * 2, volumes={0: '100.000', 1: '400.000'})
        self.assertIn('pesos de 20.000 a 20.000 kg; volumes de 100 a 400', repeticao.motivo)

    def test_mercadorias_diferentes_ou_cubagens_diferentes_nao_marcam(self):
        itens = [item('X', 'C1', peso='10000', cubagem='68'), item('X', 'C2', peso='20000', cubagem='68')]
        self.assertEqual(rd.cubagem_repetida(itens, [ncm('X', 'C1', '3901'), ncm('X', 'C2', '8471')]), (True, None))
        self.assertEqual(self.ce(['10000.000', '20000.000'], ['60.000', '68.000']), (True, None))

    def test_ncm_sem_conteiner_vale_para_todos_os_conteineres_do_ce(self):
        _, repeticao = self.ce(['5000.000', '9000.000'], ['33.000'] * 2, sem_conteiner=True)
        self.assertEqual(repeticao.conteineres, 2)

    def test_uso_parcial_e_um_conteiner_so_nao_sao_avaliaveis(self):
        self.assertEqual(self.ce(['5000.000', '9000.000'], ['33.000'] * 2, uso={1: 'true'}), (False, None))
        self.assertEqual(self.ce(['5000.000'], ['33.000']), (False, None))
        self.assertEqual(self.ce(['5000.000', '9000.000'], ['', '']), (False, None))      # sem cubagem

    def test_itens_do_mesmo_conteiner_somam(self):
        itens = [item('X', 'C1', peso='5000', cubagem='34'), item('X', 'C1', peso='5000', cubagem='34'),
                 item('X', 'C2', peso='20000', cubagem='68')]
        _, repeticao = rd.cubagem_repetida(itens, [ncm('X', 'C1', '3901'), ncm('X', 'C2', '3901')])
        self.assertIn('pesos de 10.000 a 20.000 kg', repeticao.motivo)


class TestSinal17(unittest.TestCase):

    def test_formato_e_prefixo(self):
        self.assertEqual([rd.formato_bl(v) for v in ('', '123456789', 'MAEU123456789', 'HLCUSHA2307ABCD1',
                                                     'MEDUAB123456', 'SUDU-21ABC', 'ABC', '1ABC')],
                         ['vazio', 'só dígitos', '4 letras + dígitos', '7+ letras + resto', '6 letras + resto',
                          '4 letras + misto', 'só letras', 'começa com dígito'])
        self.assertEqual(rd.prefixo_bl('HLCU SHA 2307'), 'HLCU')
        self.assertIsNone(rd.prefixo_bl('BOOKING'))                # palavra, sem dígito

    def test_prefixo_raro_so_em_empresa_de_prefixo_dominante(self):
        bls = [rd.BL(str(n), '10', 'EMP', f'MAEU{n:09d}') for n in range(200)]
        bls += [rd.BL('x', '10', 'EMP', 'ZZZU000000001'), rd.BL('h', '12', 'EMP', 'QQQQ1')]
        agencia = [rd.BL(f'a{n}', '10', 'AGE', f'{("AAAA", "BBBB")[n % 2]}{n:06d}') for n in range(100)]
        agencia.append(rd.BL('y', '10', 'AGE', 'CCCC000001'))
        mapa = rd.mapa_prefixos(bls + agencia)
        self.assertEqual(mapa['EMP'], Counter({'MAEU': 200, 'ZZZU': 1}))      # o HBL fica de fora
        [(sinal, motivo)] = rd.sinais_bl(bls[200], mapa)
        self.assertEqual(sinal, rd.PREFIXO)
        self.assertIn('prefixo ZZZU em 0,5% dos BLs da empresa EMP (o comum: MAEU, 100%)', motivo)
        self.assertEqual(rd.sinais_bl(bls[201], mapa), [])          # HBL: número do agente
        self.assertEqual(rd.sinais_bl(bls[0], mapa), [])
        self.assertEqual(rd.sinais_bl(agencia[-1], mapa), [])       # a agência numera de vários jeitos: nada


class TestSinais18(unittest.TestCase):

    def test_tipico_robusto_com_piso_de_dispersao(self):
        referencia = rd.tipico([10000.0] * 40, 'x')
        self.assertEqual((referencia.n, round(10 ** referencia.mediana), referencia.mad), (40, 10000, rd.MAD_MINIMO))
        self.assertAlmostEqual(rd.desvios(10000 * 10 ** 0.15, referencia), 3.0)
        self.assertIsNone(rd.tipico([0, None]))
        self.assertEqual(set(rd.tipicos({'a': [1.0] * 30, 'b': [1.0] * 29})), {'a'})

    def test_medidas_do_mais_especifico_ao_mais_geral(self):
        c = conteiner(peso=10000.0, cubagem=25.0)
        self.assertEqual(rd.medidas_ncm(c), [
            (rd.PESO_NCM, '2|84713012', 10000.0), (rd.PESO_NCM, '2|847130', 10000.0),
            (rd.PESO_NCM, '2|8471', 10000.0), (rd.DENSIDADE_NCM, '2|84713012', 400.0),
            (rd.DENSIDADE_NCM, '2|847130', 400.0), (rd.DENSIDADE_NCM, '2|8471', 400.0)])   # densidade por tamanho
        self.assertEqual(rd.medidas_ncm(conteiner(chave=None, tipo=None)), [])   # sem tamanho, sem medida
        self.assertIsNone(rd.densidade(conteiner(cubagem=41.5)))             # cubagem impossível: sem densidade
        self.assertEqual(rd.medidas_ncm(conteiner(ncms=(('84713012', ''), ('61091000', '')))), [])
        self.assertEqual(rd.medidas_ncm(conteiner(uso_parcial=True)), [])
        self.assertEqual(rd.medidas_ncm(conteiner(peso=73.0)), [])              # em toneladas?
        self.assertEqual(rd.medidas_ncm(conteiner(peso=35000.0)), [])           # 124% da carga máxima do 20': o 16b
        self.assertNotEqual(rd.medidas_ncm(conteiner(peso=30000.0)), [])        # 106%: na folga

    def test_atipico_com_a_direcao(self):
        tabelas = {rd.PESO_NCM: {'2|8471': rd.tipico([10000.0 + n for n in range(40)], '2|8471')},
                   rd.DENSIDADE_NCM: {'2|84713012': rd.tipico([400.0] * 40, '2|84713012')}}
        [(sinal, motivo)] = rd.sinais_ncm(conteiner(peso=30000.0, cubagem=75.0), tabelas)  # densidade 400: típica
        self.assertEqual(sinal, '18a+')
        self.assertIn("30.000 kg; típico do NCM 8471 em contêiner de 20': 10.004 a 10.036 kg (40 contêineres)", motivo)
        sinais = dict(rd.sinais_ncm(conteiner(peso=10010.0, cubagem=36.0), tabelas))           # 278 kg/m³
        self.assertEqual(set(sinais), {'18b-'})
        self.assertEqual(set(rd.SINAIS), {'16e', '17a', '18a+', '18a-', '18b+', '18b-'})


class TestDesviosECubagemRedonda(unittest.TestCase):

    def test_desvios_por_medida_e_cubagem_redonda(self):
        tabelas = {rd.PESO_NCM: {'2|8471': rd.tipico([10000.0] * 40, '2|8471')}}
        achados = rd.desvios_ncm(conteiner(peso=10000.0 * 10 ** 0.15), tabelas)
        self.assertEqual(list(achados), [rd.PESO_NCM])                         # sem típico de densidade
        chave, _, distancia = achados[rd.PESO_NCM]
        self.assertEqual(chave, '2|8471')
        self.assertAlmostEqual(distancia, 3.0)
        self.assertEqual([rd.cubagem_redonda(v) for v in (68.0, 67.7, 0, None)], [True, False, False, False])
        self.assertEqual([rd.cubagem_simbolica(v, chave) for v, chave in (
            (1.0, '45G'), (7.0, '45G'), (8.0, '45G'), (2.0, '22T'), (5.0, '22T'), (None, '22G'))],
            [True, True, False, True, False, False])                        # 10% de 76,3 m³; 3 m³ sem capacidade


class TestValidacao(unittest.TestCase):

    def test_vezes_a_base(self):
        self.assertAlmostEqual(rd.vezes(5, 50, 100, 10000), 10.0)
        self.assertIsNone(rd.vezes(5, 50, 0, 10000))
        self.assertIsNone(rd.vezes(0, 0, 100, 10000))


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
