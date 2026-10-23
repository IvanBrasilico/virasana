"""Sinais de declaração inventada — regras puras (analytic_scripts/declaracao/regras_declaracao.py; itens 16, 17 e 18
do BACKLOG.md, plano aprovado em 23/10/2026). Casos montados com o formato real do Mercante (números em texto, CEs de
15 dígitos, famílias master/filhote); os casos reais entram quando o diagnóstico rodar em produção.
"""
from virasana.navii.busca_navii import tabela_recintos  # noqa: F401 — 1º: ajusta o sys.path
import unittest  # noqa: I100
from collections import Counter

from virasana.analytic_scripts.declaracao import regras_declaracao as rd

MBL, HBL1, HBL2 = '152605000000011', '152605000000012', '152605000000013'
MBL3, MHBL3, HBL3 = '152605000000021', '152605000000022', '152605000000023'


def item(ce, conteiner, tipo='22G1', peso='10000', cubagem='25', embalagem='', uso=''):
    return {'ce': ce, 'conteiner': conteiner, 'tipo': tipo, 'peso': peso, 'cubagem': cubagem,
            'embalagem': embalagem, 'uso_parcial': uso}


def ncm(ce, conteiner, codigo, embalagem='01'):
    return {'ce': ce, 'conteiner': conteiner, 'ncm': codigo, 'embalagem': embalagem}


def conteiner(**campos):
    padrao = dict(conteiner='MSCU1234566', familia='1', ces=('1',), tipo='22G1', chave='22G', peso=10000.0,
                  origem_peso='item do CE', cubagem=25.0, origem_cubagem='item do CE',
                  ncms=(('84713012', '01'),), embalagem='', uso_parcial=False)
    padrao.update(campos)
    return rd.Conteiner(**padrao)


class TestNumerosDoMercante(unittest.TestCase):

    def test_numero(self):
        self.assertEqual([rd.numero(v) for v in ('33,5', '33.5', '1.234,5', 2350, '', None, 'abc')],
                         [33.5, 33.5, 1234.5, 2350.0, None, None, None])

    def test_formato(self):
        self.assertEqual([rd.formato(v) for v in ('', '0', '-1', '2.350', '33,2', '33.25', '1.234,5', '25', 'x')],
                         ['vazio', 'zero', 'negativo', 'ponto com 3 casas (milhar?)', 'decimal com vírgula',
                          'decimal com ponto', 'milhar e decimal', 'inteiro', 'inválido'])

    def test_br_pct_ncm_e_uso_parcial(self):
        self.assertEqual((rd.br(28300), rd.br(33.2, 1), rd.br(None), rd.pct(0.0123, 2)),
                         ('28.300', '33,2', '-', '1,23%'))
        self.assertEqual((rd.ncm('8471.30.12'), rd.ncm('847'), rd.ncm(None)), ('84713012', None, None))
        self.assertTrue(rd.uso_parcial(' s '))
        self.assertFalse(rd.uso_parcial('N'))


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
        self.assertEqual(rd.nome_grupo('R'), 'refrigerado (reefer)')


class TestConteineresDaFamilia(unittest.TestCase):

    def test_master_e_filhotes_o_peso_e_a_cubagem_do_master_e_os_ncms_dos_filhotes(self):
        master_de = {MBL: None, HBL1: MBL, HBL2: MBL}
        itens = [item(MBL, 'MSCU1234566', peso='24.000,0', cubagem='30'),
                 item(HBL1, 'MSCU1234566', peso='8000', cubagem='10', tipo=''),
                 item(HBL2, 'mscu1234566', peso='16000', cubagem='20', tipo='')]
        ncms = [ncm(HBL1, 'MSCU1234566', '6109.10.00'), ncm(HBL2, 'MSCU1234566', '61091000')]
        [c] = rd.conteineres(itens, ncms, master_de)
        self.assertEqual((c.familia, c.ces, c.chave, c.peso, c.origem_peso, c.cubagem),
                         (MBL, (MBL, HBL1, HBL2), '22G', 24000.0, 'item do master', 30.0))
        self.assertEqual((c.ncm_unico, c.capitulos, c.tamanho), ('61091000', {'61'}, '2'))

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
        [c] = rd.conteineres([item('1', 'C1', uso='S')], [ncm('1', '', '84713012'), ncm('1', '', '847')], {})
        self.assertEqual((c.ncms, c.uso_parcial), ((('84713012', '01'),), True))
        dois = rd.conteineres([item('2', 'C1'), item('2', 'C2')], [ncm('2', '', '84713012')], {})
        self.assertEqual([c.ncms for c in dois], [(), ()])       # dois contêineres: o NCM sem contêiner não vai


class TestSinais16(unittest.TestCase):

    def test_cubagem_e_peso_acima_do_que_o_tipo_comporta(self):
        sinais = dict(rd.sinais_capacidade(conteiner(cubagem=41.5, peso=35000.0)))
        self.assertEqual(set(sinais), {rd.OCUPACAO, rd.CARGA_MAXIMA})
        self.assertIn("41,5 m³ num 20', uso geral (dry) — capacidade de 33,2 m³ (125%)", sinais[rd.OCUPACAO])
        self.assertIn('carga máxima de 28.300 kg (124%)', sinais[rd.CARGA_MAXIMA])
        self.assertEqual(rd.sinais_capacidade(conteiner(cubagem=33.0, peso=30000.0)), [])   # 106%: na folga
        self.assertEqual(rd.sinais_capacidade(conteiner(cubagem=41.5, uso_parcial=True)), [])
        self.assertEqual(rd.sinais_capacidade(conteiner(cubagem=41.5, chave='22T', tipo='22T0')), [])

    def test_pares_raros_por_carga(self):
        contagens = Counter({('84', 'G'): 995, ('84', 'K'): 1, ('84', 'R'): 4, ('03', 'G'): 3})
        self.assertEqual(rd.pares_raros(contagens, fracao=0.002, minimo=500), {('84', 'K')})
        c = conteiner(tipo='22K1', chave='22K', ncms=(('84713012', '99'),))
        sinais = rd.sinais_pares(c, frozenset({('84', 'K')}), frozenset({('84', '99')}))
        self.assertEqual([s for s, _ in sinais], [rd.TIPO_CARGA, rd.EMBALAGEM_CARGA])
        self.assertIn("20', tanque com carga do capítulo 84", sinais[0][1])
        self.assertEqual(rd.pares_embalagem_carga(conteiner(uso_parcial=True)), set())


class TestSinais17(unittest.TestCase):

    def test_formato_prefixo_e_cidade(self):
        self.assertEqual([rd.formato_bl(v) for v in ('', '123456789', 'MAEU123456789', 'HLCUSHA2307ABCD1',
                                                     'MEDUAB123456', 'SUDU-21ABC', 'ABC', '1ABC')],
                         ['vazio', 'só dígitos', '4 letras + dígitos', '7+ letras (prefixo + cidade?)',
                          '6 letras + resto', '4 letras + misto', 'só letras', 'começa com dígito'])
        self.assertEqual((rd.prefixo_bl('HLCU SHA 2307'), rd.cidade_bl('HLCUSHA2307'), rd.cidade_bl('MAEU12')),
                         ('HLCU', 'SHA', None))
        self.assertIsNone(rd.prefixo_bl('BOOKING'))                # palavra, sem dígito
        self.assertEqual((rd.cidade_do_porto('CNSHA'), rd.cidade_do_porto('X')), ('SHA', None))

    def test_prefixo_raro_para_a_empresa_e_cidade_que_nao_bate(self):
        bls = [rd.BL(str(n), '10', 'EMP', f'MAEU{n:09d}', 'CNSHA') for n in range(200)]
        bls += [rd.BL('x', '10', 'EMP', 'ZZZU000000001', 'CNSHA'), rd.BL('h', '12', 'EMP', 'QQQQ1', 'CNSHA')]
        bls += [rd.BL(f'h{n}', '11', 'HAP', f'HLCUSHA{n:06d}', 'CNSHA') for n in range(40)]
        bls.append(rd.BL('y', '11', 'HAP', 'HLCUNGB000001', 'CNSHA'))
        mapa, taxas = rd.mapa_prefixos(bls), rd.taxas_cidade(bls)
        self.assertEqual(mapa['EMP'], Counter({'MAEU': 200, 'ZZZU': 1}))      # o HBL fica de fora
        self.assertEqual(taxas['HLCU'], (40, 41))
        cidades = rd.prefixos_com_cidade(taxas)
        self.assertEqual(cidades, {'HLCU'})
        [(sinal, motivo)] = rd.sinais_bl(bls[200], mapa, cidades)
        self.assertEqual(sinal, rd.PREFIXO)
        self.assertIn('prefixo ZZZU em 0,5% dos BLs da empresa EMP (o comum: MAEU, 100%)', motivo)
        self.assertEqual(rd.sinais_bl(bls[-1], mapa, cidades),
                         [(rd.CIDADE, 'BL HLCUNGB000001: o número indica NGB; origem declarada CNSHA')])
        self.assertEqual(rd.sinais_bl(bls[201], mapa, cidades), [])          # HBL: número do agente
        self.assertEqual(rd.sinais_bl(bls[0], mapa, cidades), [])


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
            (rd.PESO_NCM, '2|8471', 10000.0), (rd.DENSIDADE_NCM, '84713012', 400.0),
            (rd.DENSIDADE_NCM, '847130', 400.0), (rd.DENSIDADE_NCM, '8471', 400.0)])
        self.assertIsNone(rd.densidade(conteiner(cubagem=41.5)))             # cubagem impossível: sem densidade
        self.assertEqual(rd.medidas_ncm(conteiner(ncms=(('84713012', ''), ('61091000', '')))), [])
        self.assertEqual(rd.medidas_ncm(conteiner(uso_parcial=True)), [])

    def test_peso_atipico_para_o_ncm(self):
        tabelas = {rd.PESO_NCM: {'2|8471': rd.tipico([10000.0 + n for n in range(40)], '2|8471')},
                   rd.DENSIDADE_NCM: {'84713012': rd.tipico([400.0] * 40, '84713012')}}
        [(sinal, motivo)] = rd.sinais_ncm(conteiner(peso=30000.0, cubagem=75.0), tabelas)  # densidade 400: típica
        self.assertEqual(sinal, rd.PESO_NCM)
        self.assertIn("30.000 kg; típico do NCM 8471 em contêiner de 20': 10.004 a 10.036 kg (40 contêineres)", motivo)
        self.assertEqual(rd.sinais_ncm(conteiner(peso=10010.0, cubagem=25.0), tabelas), [])


class TestValidacao(unittest.TestCase):

    def test_vezes_a_populacao(self):
        self.assertAlmostEqual(rd.vezes(5, 50, 100, 10000), 10.0)
        self.assertIsNone(rd.vezes(5, 50, 0, 10000))
        self.assertIsNone(rd.vezes(0, 0, 100, 10000))


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
