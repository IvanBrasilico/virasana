"""CASOS REAIS do ciclo de vida: o corpus de regressão (22–23/10/2026; CICLO_DE_VIDA.md, "Casos reais de regressão").

Cada caso é um JSON extraído de produção por `caso_ciclo.py` (só as colunas que o ciclo de vida lê; sem CPF, CNPJ ou
nome — regra do usuário para dados publicados no repositório; com o porto de cada recinto, como o serviço o calcula)
e tem um GABARITO por desembarque: CE principal (ou o conjunto dos CEs igualmente válidos, num empate legítimo),
fluxo, situação e confiança, com o PORQUÊ do caso. O teste reprocessa cada caso com o serviço do ciclo de vida (código
atual) num SQLite em memória e confere:
  - o gabarito de cada desembarque, e que não aparece nem some ciclo; um desembarque `pendente` registra um defeito
    conhecido: o gabarito é o CERTO e o teste exige que o código ainda erre — quando acertar, pede para tirar a marca;
  - as INVARIANTES (`invariantes_ciclo.py`) em todos os ciclos de todos os casos — valem até com dado ruim;
  - que todo JSON em `dados/casos_ciclo/` está no gabarito (caso extraído e esquecido não passa).

Para acrescentar casos: uma linha em `dados/casos_ciclo/casos.txt` ("nome contêiner [contêiner ...]  # por quê"),
extrair no servidor (`caso_ciclo.py --casos ... --pasta casos_ciclo`), copiar os JSON para `dados/casos_ciclo/`, ler o
`.log` de cada um (o porquê de cada escolha) e escrever o gabarito abaixo.
"""
import json
import unittest
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, FrozenSet, Optional, Union

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.caso_ciclo import (
    banco_do_caso, ciclos_do_servico, le_casos, portos_do_caso,
)
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import normaliza_ce
from virasana.analytic_scripts.ciclo_de_vida.invariantes_ciclo import contexto_do_caso, violacoes

DADOS = Path(__file__).parent / 'dados'
CORPUS = DADOS / 'casos_ciclo'


@dataclass(frozen=True)
class Esperado:
    principal: Union[None, str, FrozenSet[str]]   # conjunto = empate legítimo (vale qualquer um)
    fluxo: str
    situacao: str
    confianca: Optional[str] = None   # None = não confere a confiança
    pendente: Optional[str] = None    # defeito conhecido, ainda não corrigido (o gabarito é o certo)
    familia: Optional[FrozenSet[str]] = None   # os CEs gravados do ciclo (None = não confere)


@dataclass(frozen=True)
class Caso:
    arquivo: str                      # relativo a `dados/`
    porque: str                       # o que o caso ensina (o limite que ele marca)
    esperados: Dict[int, Esperado]    # id do desembarque -> gabarito (todos os ciclos do caso)


def ces_do_manifesto(arquivo: str, manifesto: str) -> FrozenSet[str]:
    """Os CEs de um manifesto no caso — empate legítimo: vários BLs únicos do mesmo contêiner (carga consolidada)."""
    caso = json.loads((DADOS / arquivo).read_text(encoding='utf-8'))
    return frozenset(c['numeroCEmercante'] for c in caso['tabelas']['conhecimentosresumo']
                     if c['manifestoCE'] == manifesto)


O, T, P, B, IND = ldt.OPERADOR, ldt.TRA, ldt.PASSAGEM, ldt.BALDEACAO, ldt.INDEFINIDO
ABE, CON, INC = ldt.ABERTO, ldt.CONCLUIDO, ldt.INCOMPLETO
ALTA, MEDIA, BAIXA = ldt.ALTA, ldt.MEDIA, ldt.BAIXA
IMPORTACAO_CAAU = '152605316468892'
CONSOLIDADO_SUAPE = ces_do_manifesto('casos_ciclo/perna_seguinte_suape.json', '0726301788489')

CASOS: Dict[str, Caso] = {
    'CAAU5545018': Caso(
        'caso_ciclo_CAAU5545018.json',
        'Cabotagem que SAI de Santos (origem BRSSZ) não é o CE do desembarque; o manifesto do CE de importação não tem '
        'escala ligada no Mercante e vale o início da operação; o CE declarado no desembarque e no CLIA confirma.',
        {12212834: Esperado(IMPORTACAO_CAAU, T, CON, ALTA),      # CAAU5545018
         12212278: Esperado(IMPORTACAO_CAAU, T, CON, ALTA)}),    # FANU1174955
    'aguardando_seco': Caso(
        'casos_ciclo/aguardando_seco.json',
        'Saiu do operador e ainda não entrou num seco: aberto, aguardando a entrada (prazo do trânsito).',
        {12183097: Esperado('152605311046876', IND, ABE, ALTA)}),
    'cabotagem_que_sai_btp': Caso(
        'casos_ciclo/cabotagem_que_sai_btp.json',
        'O único CE com o contêiner nos itens é a cabotagem que SAI de Santos depois (perna seguinte): o desembarque '
        'na BTP fica sem CE (a inspeção declara um CE fora do Mercante). Um sai por estrada (entregue); o outro '
        'embarca na Embraport (baldeação).',
        {11953341: Esperado(None, B, CON),     # CAAU6628668 26/09
         11953108: Esperado(None, O, CON)}),   # MRSU4528707 26/09
    'cabotagem_que_sai_embraport': Caso(
        'casos_ciclo/cabotagem_que_sai_embraport.json',
        'Contêineres vazios no Mercante desembarcados na Embraport: o CE gravado antes era a cabotagem que SAI de '
        'Santos semanas depois; agora sem CE. O HAMU4699375 chega a Suape nessa cabotagem, e lá ela é o CE certo.',
        {11993321: Esperado(None, O, CON),                            # FTAU1593723 27/09
         11893528: Esperado(None, O, CON),                            # HAMU4699375 23/09 (Santos)
         12528811: Esperado('072603336079596', IND, ABE, ALTA),         # HAMU4699375 21/10 (Suape)
         12092424: Esperado(None, O, CON)}),                          # MNBU9227107 02/10
    'conteiner_consolidado': Caso(
        'casos_ciclo/conteiner_consolidado.json',
        'C7: dois BLs únicos do mesmo manifesto com o mesmo contêiner (carga consolidada), os dois declarados: uma '
        'família com os dois CEs (o card de cada um acha o ciclo), confiança ALTA; TRA concluído. Antes: duas '
        'famílias, empate e confiança baixa, e só um CE no ciclo.',
        {12259198: Esperado('152605317227201', T, CON, ALTA,
                            familia=frozenset({'152605317227201', '152605317233368'}))}),
    'ecoporto_escaneamento_fora': Caso(
        'casos_ciclo/ecoporto_escaneamento_fora.json',
        'Escaneamento na passagem rápida pelo Ecoporto antes do seco; TRA concluído, confiança alta.',
        {12215435: Esperado('152605323126829', T, CON, ALTA)}),
    'embarque_exportacao_outro': Caso(
        'casos_ciclo/embarque_exportacao_outro.json',
        'C6: exportação de OUTRO porto (Vitória -> Benin / EUA) em baldeação em Santos: desce e reembarca declarando '
        'o CE de exportação; não há importação (sem CE), e o ciclo é passagem concluída — o CE de exportação em '
        'trânsito corrobora o embarque. Antes: incompleto ("outra família"), porque uma cabotagem ANTIGA do contêiner '
        'saindo de Santos entrava na perna seguinte. Em 23/08 o MRSU7476660 é cabotagem Manaus -> Vitória passando '
        'por Santos (porto intermediário).',
        {12227546: Esperado(None, P, CON),                          # CMAU3225772 08/10
         11090139: Esperado('122603260789008', P, CON, MEDIA),      # MRSU7476660 23/08
         11913450: Esperado(None, P, CON)}),                        # MRSU7476660 24/09
    'embraport_sem_ce': Caso(
        'casos_ciclo/embraport_sem_ce.json',
        'A Embraport não declara CE: passagem (desembarque e embarque no mesmo operador) sem CE no Mercante.',
        {11925061: Esperado(None, P, CON)}),
    'entrada_implicita': Caso(
        'casos_ciclo/entrada_implicita.json',
        'Entrada no Ecoporto não informada, deduzida da pesagem; Ecoporto em papel de TRA; master/filhote.',
        {11894985: Esperado('152605305103200', T, CON, ALTA)}),
    'multirio_sem_ce': Caso(
        'casos_ciclo/multirio_sem_ce.json',
        'A MultiRio não declara CE: duas passagens do mesmo contêiner, sem CE.',
        {11420662: Esperado(None, P, CON),     # 07/09
         12143604: Esperado(None, P, CON)}),   # 03/10
    'operador_em_papel_de_tra': Caso(
        'casos_ciclo/operador_em_papel_de_tra.json',
        'Outro operador de Santos (Embraport; Santos Brasil) em papel de TRA: o ciclo conclui na saída dele.',
        {11937316: Esperado('152605286892462', T, CON, ALTA),    # BAOU3800744
         11906575: Esperado('152605305974856', T, CON, ALTA)}),  # BMOU5617445 (master/filhote)
    'operador_entregue': Caso(
        'casos_ciclo/operador_entregue.json',
        'Entregue pelo operador (BTP), família master/filhote: o principal é o filhote.',
        {12014781: Esperado('152605310385600', O, CON, ALTA)}),
    'passagem_ce_novo_confere': Caso(
        'casos_ciclo/passagem_ce_novo_confere.json',
        'CE de passagem (tráfego 9) achado pela escala/início da operação; a escala declarada no desembarque confere.',
        {11908282: Esperado('002609041170805', P, CON, MEDIA),   # FSCU8280571
         11908337: Esperado('002609041170554', IND, ABE, MEDIA)}),  # MRKU4972961
    'passagem_ce_novo_nao_confere': Caso(
        'casos_ciclo/passagem_ce_novo_nao_confere.json',
        'Baldeação Buenos Aires -> Buenaventura na Embraport: o CE de passagem é de um manifesto de baldeação emitido '
        'em Santos, por isso a escala do desembarque não confere — e ainda assim é o CE certo.',
        {12274092: Esperado('002609043801047', P, CON, MEDIA),   # MRKU7971613
         12274113: Esperado('002609043801047', P, CON, MEDIA)}),  # MRKU9673388
    'pecem_duas_familias': Caso(
        'casos_ciclo/pecem_duas_familias.json',
        'Pecém: dois CEs de importação Jedá -> Manaus por contêiner, em manifestos diferentes e sem escala em Pecém — '
        'empate legítimo (mesma distância de escala), confiança BAIXA; baldeação para Manaus.',
        {11932902: Esperado(frozenset({'012605287865277', '012605301862802'}), P, CON, BAIXA),   # MMAU1065037
         11933669: Esperado(frozenset({'012605287863223', '012605301861407'}), IND, ABE, BAIXA),   # MRKU6602987
         11933169: Esperado(frozenset({'012605287864971', '012605301862640'}), P, CON, BAIXA),   # MTSU9615182
         11933279: Esperado(frozenset({'012605287874349', '012605301871631'}), P, CON, BAIXA)}),  # SUDU6238839
    'pecem_entregue': Caso(
        'casos_ciclo/pecem_entregue.json',
        'Pecém: entregue pelo operador e depois voltou ao operador do desembarque (devolução do vazio).',
        {12021635: Esperado('042605304231392', O, CON, ALTA)}),
    'perna_seguinte_pecem': Caso(
        'casos_ciclo/perna_seguinte_pecem.json',
        'Pecém: o CE é a cabotagem Santos -> Pecém que chega (escala em Pecém na véspera); a cabotagem Pecém -> '
        'Salvador que SAI depois é a perna seguinte (era a gravada).',
        {12243351: Esperado('042603318336807', IND, ABE, MEDIA)}),
    'perna_seguinte_suape': Caso(
        'casos_ciclo/perna_seguinte_suape.json',
        'Santos: importação entregue. Suape: chega na cabotagem Santos -> Suape, num manifesto com 75 BLs únicos do '
        'mesmo contêiner (C7: uma família, todos no ciclo; confiança média, nenhum declarado); a cabotagem que SAI '
        'de Suape depois é a perna seguinte (era a gravada).',
        {11874736: Esperado('152603296561009', O, CON, ALTA),                                         # Santos 21/09
         12150835: Esperado(CONSOLIDADO_SUAPE, O, CON, MEDIA, familia=CONSOLIDADO_SUAPE)}),            # Suape 03/10
    'porto_intermediario_rio': Caso(
        'casos_ciclo/porto_intermediario_rio.json',
        'Importação para Vitória (filhotes) com desembarques intermediários em Santos e no Rio, passagem nos dois: o '
        'CE de importação vale nos dois (porto intermediário).',
        {11369745: Esperado('122605285739529', P, CON, MEDIA),   # GCXU5651625 05/09 Santos
         11890063: Esperado('122605285739529', P, CON, MEDIA),   # GCXU5651625 22/09 Rio
         11368837: Esperado('122605303297022', P, CON, MEDIA),   # MRSU8255642 05/09 Santos
         11890105: Esperado('122605303297022', P, CON, MEDIA)}),  # MRSU8255642 22/09 Rio
    'porto_intermediario_suape': Caso(
        'casos_ciclo/porto_intermediario_suape.json',
        'Importação para Navegantes (filhotes): desce e reembarca em Suape em menos de 1 h (a escala declarada não '
        'está no Mercante — vale o porto do recinto) e desce em Santos, onde o CE é declarado (alta).',
        {12363956: Esperado('182605311807607', P, CON, MEDIA),   # MSMU8710664 14/10 Suape
         12519526: Esperado('182605311807607', IND, ABE, ALTA),    # MSMU8710664 22/10 Santos
         12356204: Esperado('182605320726789', P, CON, MEDIA),   # TGHU8767660 14/10 Suape
         12520067: Esperado('182605320726789', IND, ABE, ALTA)}),  # TGHU8767660 22/10 Santos
    'reentrada_no_operador': Caso(
        'casos_ciclo/reentrada_no_operador.json',
        'Saída do operador não informada: o contêiner reentra no mesmo operador — incompleto; vazio no Mercante, '
        'sem CE.',
        {12076886: Esperado(None, O, INC)}),
    'saida_duvidosa': Caso(
        'casos_ciclo/saida_duvidosa.json',
        'Saída com CE de outra família: aberto para revisão (Suape; Rio). O CHIU9036227 passa antes pela BTP '
        '(passagem concluída, mesmo CE).',
        {12059154: Esperado('072603313540009', O, ABE, ALTA),    # CAIU3022807 Suape
         12247722: Esperado('132605327169064', P, CON, ALTA),    # CHIU9036227 BTP
         12407706: Esperado('132605327169064', O, ABE, ALTA)}),  # CHIU9036227 Rio
    'seco_sem_saida': Caso(
        'casos_ciclo/seco_sem_saida.json',
        'Saída do recinto seco não informada (o contêiner entra na BTP depois): TRA incompleto.',
        {11894752: Esperado('152605304654904', T, INC, ALTA)}),
    'tra_master_filhote': Caso(
        'casos_ciclo/tra_master_filhote.json',
        'TRA para o seco com família master/filhote: o principal é o filhote, confiança alta.',
        {12066118: Esperado('152605307719730', T, CON, ALTA)}),
    'vazio_entrada_seguinte': Caso(
        'casos_ciclo/vazio_entrada_seguinte.json',
        'Entregue pelo operador sem a saída informada: a entrada seguinte com o contêiner vazio conclui. O BEAU2601538 '
        'tem antes uma passagem (cabotagem).',
        {12260007: Esperado('152603318702114', O, CON, ALTA),    # APHU7283233
         10824408: Esperado('012605240858690', P, CON, MEDIA),   # BEAU2601538 10/08
         12065008: Esperado(None, O, CON)}),                     # BEAU2601538 01/10
    'voltou_ao_operador': Caso(
        'casos_ciclo/voltou_ao_operador.json',
        'Entregue e depois voltou ao operador do desembarque (devolução do vazio): conclui na saída.',
        {11885725: Esperado('152603299171767', O, CON, ALTA)}),
}


def processa(caso: dict):
    """(ciclos do serviço por desembarque, portos dos desembarques) do caso."""
    engine = banco_do_caso(caso)
    return ciclos_do_servico(engine, caso), portos_do_caso(engine, caso)


def diferenca(esperado: Esperado, ciclo: Optional[dict]) -> Optional[str]:
    """O que o ciclo obtido tem de diferente do gabarito (None = confere)."""
    if ciclo is None:
        return 'sem ciclo'
    principal = normaliza_ce(ciclo['numeroCEmercante'])
    confere_principal = principal in esperado.principal if isinstance(esperado.principal, frozenset) \
        else principal == esperado.principal
    obtido = (principal, ciclo['fluxo'], ciclo['situacao'], ciclo['confianca_ce'] if esperado.confianca else None)
    confere_familia = esperado.familia is None or frozenset(ciclo['familia']) == esperado.familia
    if confere_principal and confere_familia and obtido[1:] == (esperado.fluxo, esperado.situacao, esperado.confianca):
        return None
    return f'{ciclo["numeroConteiner"]}: esperado {esperado}, obtido {obtido}'


class TestCasosReais(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.resultados = {}
        for nome, caso in CASOS.items():
            dados = json.loads((DADOS / caso.arquivo).read_text(encoding='utf-8'))
            cls.resultados[nome] = (dados, *processa(dados))

    def test_gabarito(self):
        for nome, caso in CASOS.items():
            ciclos = self.resultados[nome][1]
            for desembarque, esperado in caso.esperados.items():
                with self.subTest(caso=nome, desembarque=desembarque):
                    erro = diferenca(esperado, ciclos.get(desembarque))
                    if esperado.pendente:
                        self.assertTrue(erro, f'defeito pendente corrigido ({esperado.pendente}): tirar o "pendente"')
                    else:
                        self.assertIsNone(erro, caso.porque)

    def test_nao_aparece_nem_some_ciclo(self):
        for nome, caso in CASOS.items():
            with self.subTest(caso=nome):
                self.assertEqual(sorted(self.resultados[nome][1]), sorted(caso.esperados))

    def test_invariantes_em_todos_os_ciclos(self):
        for nome, (dados, ciclos, portos) in self.resultados.items():
            info, itens = contexto_do_caso(dados)
            for desembarque, ciclo in ciclos.items():
                with self.subTest(caso=nome, desembarque=desembarque):
                    self.assertEqual(violacoes(ciclo, info, itens, portos.get(desembarque)), [])

    def test_todo_caso_extraido_esta_no_gabarito(self):
        registrados = {caso.arquivo for caso in CASOS.values()}
        extraidos = {f'casos_ciclo/{caminho.name}' for caminho in CORPUS.glob('*.json')}
        self.assertEqual(sorted(extraidos - registrados), [])

    def test_lista_de_casos_a_extrair_e_valida(self):
        casos = le_casos((CORPUS / 'casos.txt').read_text(encoding='utf-8').splitlines())
        nomes = [nome for nome, _ in casos]
        self.assertEqual(len(nomes), len(set(nomes)), 'nome de caso repetido')
        self.assertTrue(all(len(c) == 11 for _, conteineres in casos for c in conteineres))


def _ciclo(**campos):
    base = {'numeroConteiner': 'C1', 'numeroCEmercante': '152600000000001', 'confianca_ce': 'alta',
            'familia': ['152600000000001'], 'situacao': 'aberto', 'fluxo': 'indefinido',
            'data_desembarque': datetime(2026, 10, 7, 10)}
    return {**base, **campos}


INFO = {'152600000000001': {'trafego': '5', 'origem': 'CNSHA'},
        '152600000000007': {'trafego': '7', 'origem': 'BRSSZ'},
        '072600000000003': {'trafego': '3', 'origem': 'BRSSZ'}}
ITENS = {('152600000000001', 'C1'), ('152600000000007', 'C1'), ('072600000000003', 'C1')}


class TestInvariantes(unittest.TestCase):

    def test_ciclo_bom_nao_viola(self):
        self.assertEqual(violacoes(_ciclo(), INFO, ITENS, 'BRSSZ'), [])
        self.assertEqual(violacoes(_ciclo(numeroCEmercante=None, confianca_ce=None, familia=[]), INFO, ITENS), [])

    def test_principal(self):
        exportacao = _ciclo(numeroCEmercante='152600000000007', familia=['152600000000007'])
        self.assertEqual(violacoes(exportacao, INFO, ITENS), ['CE principal de exportação'])
        saindo = _ciclo(numeroCEmercante='072600000000003', familia=['072600000000003'])
        self.assertEqual(violacoes(saindo, INFO, ITENS, 'BRSSZ'), ['CE principal com a carga saindo de BRSSZ'])
        self.assertEqual(violacoes(saindo, INFO, ITENS), [])   # porto desconhecido: não verifica
        self.assertEqual(violacoes(_ciclo(numeroConteiner='C2'), INFO, ITENS),
                         ['contêiner fora dos itens do CE principal'])
        self.assertEqual(violacoes(_ciclo(familia=[], confianca_ce='talvez'), INFO, ITENS),
                         ['CE principal fora da família gravada', 'confiança inválida: talvez'])
        self.assertEqual(violacoes(_ciclo(numeroCEmercante=None), INFO, ITENS),
                         ['família sem CE principal', 'confiança alta sem CE principal'])

    def test_ordem_das_etapas(self):
        fora = _ciclo(data_saida=datetime(2026, 10, 9), data_entrada_seco=datetime(2026, 10, 8),
                      data_saida_seco=datetime(2026, 10, 12), data_embarque=datetime(2026, 10, 6))
        self.assertEqual(violacoes(fora, INFO, ITENS),
                         ['data_saida depois de data_entrada_seco', 'embarque antes do desembarque'])
        sem_saida = _ciclo(data_entrada_seco=datetime(2026, 10, 6))   # pula a saída e compara com o desembarque
        self.assertEqual(violacoes(sem_saida, INFO, ITENS), ['data_desembarque depois de data_entrada_seco'])
        self.assertEqual(violacoes(_ciclo(data_saida='2026-10-08T10:00:00'), INFO, ITENS), [])   # texto do JSON

    def test_conclusao(self):
        self.assertEqual(violacoes(_ciclo(situacao='concluido', fluxo='tra'), INFO, ITENS),
                         ['concluído (tra) sem data_saida_seco'])
        self.assertEqual(violacoes(_ciclo(situacao='concluido', fluxo='passagem'), INFO, ITENS),
                         ['concluído (passagem) sem data_embarque'])
        self.assertEqual(violacoes(_ciclo(situacao='concluido', fluxo='operador'), INFO, ITENS), [])   # saída omitida
        self.assertEqual(violacoes(_ciclo(situacao='concluido', fluxo='baldeacao',
                                          data_embarque=datetime(2026, 10, 9)), INFO, ITENS), [])

    def test_contexto_do_caso(self):
        caso = {'tabelas': {'conhecimentosresumo': [{'numeroCEmercante': '152600000000001', 'tipoTrafego': '5',
                                                     'portoOrigemCarga': 'cnsha'}],
                            'itensresumo': [{'numeroCEmercante': '152600000000001', 'codigoConteiner': 'C1 '}]}}
        self.assertEqual(contexto_do_caso(caso), ({'152600000000001': {'trafego': '5', 'origem': 'CNSHA'}},
                                                  {('152600000000001', 'C1')}))


class TestLeCasos(unittest.TestCase):

    def test_formato(self):
        linhas = ['# comentário', '', 'baldeacao_btp caau6628668 MRSU4528707  # por quê', 'outro HLBU9942493']
        self.assertEqual(le_casos(linhas), [('baldeacao_btp', ['CAAU6628668', 'MRSU4528707']),
                                            ('outro', ['HLBU9942493'])])
        with self.assertRaises(ValueError):
            le_casos(['so_o_nome'])


if __name__ == '__main__':
    unittest.main()
