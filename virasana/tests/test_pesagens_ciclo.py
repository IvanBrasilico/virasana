"""Regras puras das pesagens no Ciclo de Vida (analytic_scripts/ciclo_de_vida/pesagens_ciclo.py)."""
import unittest
from datetime import datetime, timedelta

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc

T0 = datetime(2026, 10, 18, 10)


def pesagem(horas=0, placa='ABC1D23', semirreboque='', conteiner='', peso=15000.0, recinto='8931356'):
    return {'data': T0 + timedelta(hours=horas), 'placa': placa, 'semirreboque': semirreboque,
            'conteiner': conteiner, 'peso': peso, 'recinto': recinto}


class TestDivisao(unittest.TestCase):

    def test_maior_divisao_com_quase_todos(self):
        self.assertEqual(pc.divisao_da_balanca([25120, 25140, 18000, 30020]), 20)
        self.assertEqual(pc.divisao_da_balanca([25000, 30000, 41500.0]), 100 if 41500 % 100 == 0 else None)
        self.assertEqual(pc.divisao_da_balanca([25070, 30030]), 10)
        self.assertEqual(pc.divisao_da_balanca([35070, 25200]), 70)            # balança antiga de 70 kg
        self.assertEqual(pc.divisao_da_balanca([20 * n for n in range(1000, 1099)] + [25125]), 20)   # 99%: fica

    def test_sem_divisao(self):
        self.assertIsNone(pc.divisao_da_balanca([]))
        self.assertIsNone(pc.divisao_da_balanca([0, None]))
        self.assertIsNone(pc.divisao_da_balanca([1235.5, 2000]))               # fracionado
        self.assertIsNone(pc.divisao_pelas_contagens({100: 5}, 0))
        self.assertEqual(pc.divisao_pelas_contagens({100: 1, 50: 1, 20: 99, 10: 100}, 100), 20)

    def test_fora_da_divisao(self):
        self.assertTrue(pc.fora_da_divisao(1235, 20))
        self.assertFalse(pc.fora_da_divisao(1240, 20))
        self.assertFalse(pc.fora_da_divisao(1235, None))
        self.assertFalse(pc.fora_da_divisao(1235, 1))
        self.assertTrue(pc.fora_da_divisao(1235.5, 1))                         # a balança de 1 kg não mostra
        self.assertFalse(pc.fora_da_divisao(None, 20))


class TestTaraDoConteiner(unittest.TestCase):

    def test_informada_ou_tipica(self):
        self.assertEqual(pc.tara_do_conteiner('2200', '22G1'), (2200.0, 'informada'))
        self.assertEqual(pc.tara_do_conteiner('2.250,0'.replace('.', ''), '22G1'), (2250.0, 'informada'))
        self.assertEqual(pc.tara_do_conteiner('', '45G1'), (3800.0, 'típica do tamanho'))
        self.assertEqual(pc.tara_do_conteiner('99999', 'L5G1'), (4500.0, 'típica do tamanho'))   # implausível
        self.assertEqual(pc.tara_do_conteiner('abc', '22G1'), (2200.0, 'típica do tamanho'))
        self.assertEqual(pc.tara_do_conteiner(None, None), (None, 'sem tara'))


class TestDigitacao(unittest.TestCase):

    def test_ordem_de_grandeza(self):
        self.assertEqual(pc.padrao_digitacao(12350, 1235), 'ordem de grandeza (÷10)')   # exemplo do usuário
        self.assertEqual(pc.padrao_digitacao(2510, 25120), 'ordem de grandeza (×10)')
        self.assertEqual(pc.padrao_digitacao(251, 25140), 'ordem de grandeza (×100)')

    def test_digitos(self):
        self.assertEqual(pc.padrao_digitacao(25100, 52150), 'dígitos vizinhos trocados')   # 25100 -> 52100
        self.assertIsNone(pc.padrao_digitacao(25100, 21540))      # 21500 trocado, mas só -14%: moderada (P0b)
        self.assertIsNone(pc.padrao_digitacao(27990, 30450))      # P0: 29790 "explicava" +9%
        self.assertEqual(pc.padrao_digitacao(25100, 2100), pc.UM_DIGITO)               # sem o 5
        self.assertEqual(pc.padrao_digitacao(2100, 25100), pc.UM_DIGITO)               # do medido ao declarado

    def test_diferenca_real_nao_e_digitacao(self):
        self.assertIsNone(pc.padrao_digitacao(25100, 31000))
        self.assertIsNone(pc.padrao_digitacao(25100, 28100))      # um dígito TROCADO não conta
        self.assertIsNone(pc.padrao_digitacao(0, 1000))
        self.assertIsNone(pc.padrao_digitacao(25100, 25200))      # quase iguais: 25010 (trocados) não explica melhor


class TestClassifica(unittest.TestCase):

    def test_categorias(self):
        self.assertEqual(pc.classifica(25100, 25300)[0], pc.COERENTE)
        self.assertEqual(pc.classifica(25100, 25300), (pc.COERENTE, '+0.8%'))
        self.assertEqual(pc.classifica(12350, 1240)[0], pc.DIGITACAO)
        self.assertEqual(pc.classifica(10000, 20500, tipo_iso='22G1'), (pc.DOIS, 'medido 2.0× o declarado'))
        self.assertEqual(pc.classifica(10000, 20500, tipo_iso='45G1')[0], pc.DIVERGENCIA)   # 40': não é par
        self.assertEqual(pc.classifica(25100, 31000), (pc.DIVERGENCIA, '+24%'))
        self.assertEqual(pc.classifica(None, 31000)[0], pc.SEM_DADO)
        self.assertEqual(pc.classifica(25100, None)[0], pc.SEM_DADO)

    def test_fora_da_comparacao(self):
        # P0: 9.470 − 7.310 − 2.150 = 10 kg x 4.904 declarados virava "÷100"
        self.assertEqual(pc.classifica(4904, 10), (pc.VAZIO_NA_PESAGEM, 'medido 10 kg x declarado 4904 kg'))
        self.assertEqual(pc.classifica(25100, -5)[0], pc.VAZIO_NA_PESAGEM)
        self.assertEqual(pc.classifica(25100, 800)[0], pc.VAZIO_NA_PESAGEM)
        self.assertEqual(pc.classifica(25100, 801)[0], pc.DIVERGENCIA)                    # 801 kg já não é vazio
        self.assertEqual(pc.classifica(20939, -3620)[0], pc.SEM_CONTEINER)               # bruto ≈ conjunto
        self.assertEqual(pc.classifica(25100, -801)[0], pc.SEM_CONTEINER)
        self.assertEqual(pc.classifica(25100, -800)[0], pc.VAZIO_NA_PESAGEM)             # limite
        self.assertNotEqual(pc.classifica(100, 5000)[0], pc.DECLARADO_INVALIDO)          # 100 kg já vale
        self.assertEqual(pc.classifica(1, 3870), (pc.DECLARADO_INVALIDO, 'declarado 1 kg'))   # P0: virava "÷100"
        self.assertEqual(pc.classifica(99, 99)[0], pc.DECLARADO_INVALIDO)
        self.assertEqual(pc.classifica(500, 450)[0], pc.COERENTE)                         # carga leve coerente

    def test_moderada_ate_20_por_cento(self):
        self.assertEqual(pc.classifica(27990, 30450), (pc.MODERADA, '+8.8%'))
        self.assertEqual(pc.classifica(10000, 12000), (pc.MODERADA, '+20.0%'))
        self.assertEqual(pc.classifica(10000, 12100)[0], pc.DIVERGENCIA)
        self.assertEqual(pc.classifica(10000, 8000)[0], pc.MODERADA)

    def test_tolerancia_cresce_com_a_divisao(self):
        self.assertEqual(pc.tolerancia(1000), 300)
        self.assertEqual(pc.tolerancia(30000), 1500)
        self.assertEqual(pc.tolerancia(1000, 100), 500)


class TestTaraDoConjunto(unittest.TestCase):

    def test_ordem_das_fontes(self):
        self.assertEqual(pc.tara_do_conjunto(15000, 14000, 13000), (15000, pc.PAR))
        self.assertEqual(pc.tara_do_conjunto(None, 14000, 13000), (14000, pc.VAZIA))
        self.assertEqual(pc.tara_do_conjunto(None, None, 13000), (13000, pc.API))
        self.assertEqual(pc.tara_do_conjunto(None, None, 4630), (None, 'sem tara'))       # Suape: a do contêiner
        self.assertEqual(pc.tara_do_conjunto(None, None, 7999), (None, 'sem tara'))
        self.assertEqual(pc.tara_do_conjunto(None, None, 8000), (8000, pc.API))
        self.assertEqual(pc.tara_do_conjunto(None, None, 30001), (None, 'sem tara'))
        self.assertEqual(pc.tara_do_conjunto(None, None, 30000), (30000, pc.API))
        self.assertEqual(pc.tara_do_conjunto(52040, 14000, None), (14000, pc.VAZIA))      # Pecém: "par" de 52 t
        self.assertEqual(pc.tara_do_conjunto(None, 4370, 15000), (15000, pc.API))         # "vazia" de 4,4 t
        self.assertEqual(pc.tara_do_conjunto(None, None, None), (None, 'sem tara'))


class TestPesoDaFamilia(unittest.TestCase):

    def test_master_filhotes_e_simples(self):
        self.assertEqual(pc.peso_da_familia([('M', None, '24000'), ('F1', 'M', '8000'), ('F2', 'M', '16000')]),
                         (24000.0, 'item do master'))
        self.assertEqual(pc.peso_da_familia([('F1', 'M', '8000'), ('F2', 'M', '16000,5')]),
                         (24000.5, 'soma de 2 filhotes'))
        self.assertEqual(pc.peso_da_familia([('F1', 'M', '8000')]), (8000.0, 'item do filhote'))
        self.assertEqual(pc.peso_da_familia([('S', None, '22900')]), (22900.0, 'item do CE'))
        self.assertEqual(pc.peso_da_familia([('S', None, None), ('F1', 'M', '')]), (None, 'sem peso'))


class TestContexto(unittest.TestCase):

    def test_mesma_operacao(self):
        r1, r2, seco = '8931356', '8931359', '8932000'
        d3 = [(T0 - timedelta(days=3), r1, True)]
        portuarios = {r1, r2}
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, d3, portuarios, 'S'), pc.RETIRADA)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, d3, portuarios, 'E'), pc.VOLTA_AO_OPERADOR)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, d3, portuarios), pc.OPERADOR_SEM_ACESSO)
        self.assertEqual(pc.contexto_da_pesagem(T0, seco, d3, portuarios, 'E'), pc.CHEGADA_AO_SECO)
        self.assertEqual(pc.contexto_da_pesagem(T0, r2, d3, portuarios, 'E'), pc.OUTRO_OPERADOR)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, [(T0 - timedelta(days=3), r1, True),
                                                         (T0 - timedelta(days=1), r1, False)]), pc.SEM_DESEMBARQUE)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, [(T0 - timedelta(days=61), r1, True)]), pc.SEM_DESEMBARQUE)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, [(T0 - timedelta(days=60), r1, True)], direcao='S'),
                         pc.RETIRADA)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, [(T0 + timedelta(hours=1), r1, True)]), pc.SEM_DESEMBARQUE)
        self.assertEqual(pc.contexto_da_pesagem(T0, r1, []), pc.SEM_DESEMBARQUE)
        self.assertEqual(pc.OPERACOES_DA_IMPORTACAO, (pc.RETIRADA, pc.OPERADOR_SEM_ACESSO, pc.CHEGADA_AO_SECO))


class TestParTara(unittest.TestCase):

    def test_mesmo_conjunto_vazio_mais_proximo(self):
        carregada = pesagem(conteiner='GSLU2053602', peso=40000, semirreboque='XYZ9A87')
        candidatas = [
            pesagem(horas=-3, peso=15100, semirreboque='XYZ9A87'),
            pesagem(horas=1, peso=15000, semirreboque='XYZ9A87'),               # a mais próxima
            pesagem(horas=0.5, peso=15050, semirreboque='OUT1A23'),             # outro semirreboque
            pesagem(horas=0.2, peso=15020, conteiner='TGHU1111111'),            # com contêiner
            pesagem(horas=0.1, peso=15010, placa='OUT9Z99'),                    # outra placa
            pesagem(horas=0.3, peso=15300, recinto='7921302'),                  # outro recinto
            pesagem(horas=0.4, peso=0),                                         # sem peso
        ]
        self.assertEqual(pc.pareia_tara(carregada, candidatas, horas=12)['peso'], 15000)
        self.assertIsNone(pc.pareia_tara(carregada, candidatas[:1], horas=2))   # longe demais

    def test_mesmo_conteiner_mais_leve_e_o_par(self):
        # P0b: chegando para retirar, a pesagem já traz o número do contêiner — é a do conjunto sem ele
        carregada = pesagem(conteiner='GSLU2053602', peso=40000)
        chegada = pesagem(horas=-0.5, conteiner='GSLU2053602', peso=15000)
        self.assertEqual(pc.pareia_tara(carregada, [chegada], 12)['peso'], 15000)
        quase = pesagem(horas=-0.5, conteiner='GSLU2053602', peso=39001)
        self.assertIsNone(pc.pareia_tara(carregada, [quase], 12))
        self.assertEqual(pc.pareia_tara(carregada, [pesagem(horas=-0.5, conteiner='GSLU2053602', peso=39000)],
                                        12)['peso'], 39000)
        self.assertTrue(pc.e_a_carregada(carregada, [chegada, carregada], 12))
        self.assertFalse(pc.e_a_carregada(chegada, [chegada, carregada], 12))
        self.assertTrue(pc.e_a_carregada(chegada, [carregada], 0.4))                    # fora da visita
        self.assertTrue(pc.e_a_carregada(chegada, [pesagem(horas=0.5, conteiner='OUTR1234567', peso=40000)], 12))
        self.assertFalse(pc.e_a_carregada(pesagem(peso=39000, conteiner='X'), [pesagem(peso=40000, conteiner='X')],
                                          12))
        self.assertTrue(pc.e_a_carregada(pesagem(peso=39001, conteiner='X'), [pesagem(peso=40000, conteiner='X')],
                                         12))

    def test_semirreboque_ausente_nao_impede(self):
        carregada = pesagem(conteiner='GSLU2053602', peso=40000)
        self.assertEqual(pc.pareia_tara(carregada, [pesagem(horas=1, semirreboque='XYZ9A87')], 12)['peso'], 15000)


OPERADOR, SECO = '8931356', '8932000'
D0 = datetime(2026, 10, 10, 8)           # desembarque
SAIDA, ENTRADA = datetime(2026, 10, 14, 8), datetime(2026, 10, 14, 11)
DECLARADO = pc.Declarado('CE1', 22000.0, 'item do master', 3800.0, 'informada', '45G1')


def p(id_, data, peso, recinto=OPERADOR, conteiner='CTRA', placa='AAA1A11', tara_conjunto=None):
    return {'id': id_, 'data': data, 'recinto': recinto, 'placa': placa, 'semirreboque': '', 'conteiner': conteiner,
            'peso': peso, 'tara_conjunto': tara_conjunto, 'captura': True}


def lig(data, recinto, confianca=ldt.ALTA, tipo=ldt.SAIDA):
    return ldt.Ligacao(ldt.Evento(tipo, 1, data, recinto), confianca) if data else None


def ciclo(saida=None, seco=None, saida_seco=None, embarque=None):
    return ldt.CicloCalculado(ldt.Evento(ldt.DESEMBARQUE, 1, D0, OPERADOR), saida=saida, entrada_seco=seco,
                              saida_seco=lig(saida_seco, SECO), embarque=lig(embarque, OPERADOR, tipo=ldt.EMBARQUE))


class TestEtapas(unittest.TestCase):

    def test_etapas_do_ciclo(self):
        tra = ciclo(lig(SAIDA, OPERADOR), lig(ENTRADA, SECO, ldt.MEDIA, ldt.ENTRADA))
        self.assertEqual(pc.etapas_do_ciclo(tra), pc.Etapas(OPERADOR, D0, SAIDA, SECO, ENTRADA, None))   # aberto
        self.assertEqual(pc.etapas_do_ciclo(ciclo(lig(SAIDA, OPERADOR))).fim, SAIDA)                   # operador
        concluido = ciclo(lig(SAIDA, OPERADOR), lig(ENTRADA, SECO, tipo=ldt.ENTRADA), datetime(2026, 10, 20))
        self.assertEqual(pc.etapas_do_ciclo(concluido).fim, datetime(2026, 10, 20))
        self.assertEqual(pc.etapas_do_ciclo(ciclo(embarque=datetime(2026, 10, 12))).fim, datetime(2026, 10, 12))
        baixa = ciclo(lig(SAIDA, OPERADOR, ldt.BAIXA), lig(ENTRADA, SECO, ldt.BAIXA, ldt.ENTRADA))
        self.assertEqual(pc.etapas_do_ciclo(baixa), pc.Etapas(OPERADOR, D0))     # ligações baixas não contam

    def test_operacao_da_pesagem(self):
        etapas = pc.Etapas(OPERADOR, D0, SAIDA, SECO, ENTRADA, datetime(2026, 10, 20))
        minuto = timedelta(minutes=1)
        self.assertIsNone(pc.operacao_da_pesagem(p(1, D0 - minuto, 1), etapas))                 # antes do D
        self.assertEqual(pc.operacao_da_pesagem(p(1, SAIDA - 30 * minuto, 1), etapas), pc.RETIRADA)
        self.assertEqual(pc.operacao_da_pesagem(p(1, SAIDA + 31 * minuto, 1), etapas), pc.OUTRA)
        self.assertEqual(pc.operacao_da_pesagem(p(1, SAIDA, 1, recinto=SECO), etapas), pc.OUTRA)
        self.assertEqual(pc.operacao_da_pesagem(p(1, ENTRADA + 30 * minuto, 1, recinto=SECO), etapas),
                         pc.CHEGADA_AO_SECO)
        self.assertEqual(pc.operacao_da_pesagem(p(1, ENTRADA, 1), etapas), pc.OUTRA)     # no operador
        self.assertEqual(pc.operacao_da_pesagem(p(1, datetime(2026, 10, 20, 0, 30), 1), etapas), pc.OUTRA)
        self.assertIsNone(pc.operacao_da_pesagem(p(1, datetime(2026, 10, 20, 0, 31), 1), etapas))   # depois do fim
        self.assertEqual(pc.operacao_da_pesagem(p(1, datetime(2027, 1, 1), 1), pc.Etapas(OPERADOR, D0)), pc.OUTRA)


class TestComparaECiclo(unittest.TestCase):

    def test_compara_pelo_par_ou_pela_api(self):
        carregada, par = p(2, SAIDA, 40000), p(1, SAIDA - timedelta(minutes=20), 15000)
        campos = pc.compara(carregada, [par, carregada], DECLARADO, 20)
        self.assertEqual((campos['tara_pesagem_id'], campos['origem_tara_conjunto'], campos['peso_medido']),
                         (1, pc.PAR, 21200))
        self.assertEqual((campos['classificacao'], campos['diferenca_kg'], campos['divisao']), (pc.COERENTE, -800, 20))
        self.assertAlmostEqual(campos['razao'], 21200 / 22000)
        self.assertAlmostEqual(campos['diferenca_pct'], -800 / 220)
        api = pc.compara(p(2, SAIDA, 40000, tara_conjunto=16000), [], DECLARADO)
        self.assertEqual((api['tara_pesagem_id'], api['origem_tara_conjunto'], api['peso_medido']),
                         (None, pc.API, 20200))
        self.assertEqual(pc.compara(p(2, SAIDA, 40000), [], DECLARADO)['detalhe'], 'sem tara do conjunto')
        sem_peso = pc.compara(carregada, [par], pc.Declarado(None, None, 'sem peso', 3800.0, 'informada'))
        self.assertEqual((sem_peso['classificacao'], sem_peso['detalhe']),
                         (pc.SEM_DADO, 'sem peso declarado ou pesado'))
        self.assertTrue(pc.compara(p(2, SAIDA, 40005), [par], DECLARADO, 10)['fora_da_divisao'])
        leve = p(1, SAIDA - timedelta(minutes=20), 4000)                     # "par" implausível: vale a API
        api_e_par = pc.compara(p(2, SAIDA, 40000, tara_conjunto=15000), [leve], DECLARADO)
        self.assertEqual((api_e_par['tara_pesagem_id'], api_e_par['origem_tara_conjunto']), (None, pc.API))
        self.assertFalse(pc.compara(p(2, SAIDA, 40010), [par], DECLARADO, 10)['fora_da_divisao'])

    def test_linhas_do_ciclo_principal_e_confirmacao(self):
        etapas = pc.Etapas(OPERADOR, D0, SAIDA, SECO, ENTRADA)
        pesagens = [p(1, SAIDA - timedelta(minutes=20), 15000),                    # chegando para retirar
                    p(2, SAIDA + timedelta(minutes=1), 40000),                     # a carregada
                    p(3, ENTRADA + timedelta(minutes=1), 40100, SECO, tara_conjunto=15000),
                    p(4, D0 + timedelta(days=1), 30000, placa='BBB2B22'),          # no pátio
                    p(5, D0 - timedelta(days=1), 30000)]                           # antes do desembarque
        linhas = {linha['pesagem_id']: linha for linha in pc.linhas_do_ciclo(etapas, pesagens, pesagens, DECLARADO)}
        self.assertEqual(sorted(linhas), [1, 2, 3, 4])
        self.assertEqual((linhas[1]['operacao'], linhas[1]['diagnostico']), (pc.RETIRADA, 'sem_conteiner'))
        self.assertEqual((linhas[2]['classificacao'], linhas[2]['principal'], linhas[2]['tara_pesagem_id']),
                         (pc.COERENTE, True, 1))
        self.assertEqual(linhas[2]['confirmacao'], pc.CONFIRMADA)
        self.assertEqual((linhas[3]['operacao'], linhas[3]['principal'], linhas[3]['origem_tara_conjunto']),
                         (pc.CHEGADA_AO_SECO, False, pc.API))
        self.assertEqual((linhas[4]['operacao'], linhas[4].get('classificacao')), (pc.OUTRA, None))
        self.assertEqual(linhas[4]['peso_bruto'], 30000)

    def test_principal_pula_sem_dado_e_outra_que_diverge(self):
        etapas = pc.Etapas(OPERADOR, D0, SAIDA, SECO, ENTRADA)
        sem_tara = p(2, SAIDA, 40000)
        chegada = p(3, ENTRADA, 33000, SECO, tara_conjunto=15000)
        linhas = pc.linhas_do_ciclo(etapas, [sem_tara, chegada], [sem_tara, chegada], DECLARADO)
        self.assertEqual([(linha['pesagem_id'], linha['principal']) for linha in linhas], [(2, False), (3, True)])
        self.assertNotIn('confirmacao', linhas[1])                   # a outra não mediu
        diverge = pc.linhas_do_ciclo(etapas, [p(2, SAIDA, 40000, tara_conjunto=15000), chegada], [], DECLARADO)
        self.assertEqual(diverge[0]['confirmacao'], 'a outra pesagem do ciclo mediu 14200 kg (+7000)')
        so_outra = pc.linhas_do_ciclo(etapas, [p(4, D0 + timedelta(days=1), 1)], [], DECLARADO)
        self.assertEqual([linha['principal'] for linha in so_outra], [False])


if __name__ == '__main__':
    unittest.main()
