"""CASOS REAIS do ciclo de vida: o corpus de regressão (22/10/2026; CICLO_DE_VIDA.md, "Casos reais de regressão").

Cada caso é um JSON extraído de produção por `caso_ciclo.py` (só as colunas que o ciclo de vida lê; sem CPF, CNPJ ou
nome — regra do usuário para dados publicados no repositório) e tem um GABARITO por desembarque: CE principal, fluxo,
situação (e, quando importa, a confiança), revisado pelo usuário. O teste reprocessa cada caso com o serviço do ciclo
de vida (código atual) num SQLite em memória e confere:
  - o gabarito — um caso `pendente` registra um defeito conhecido: o gabarito é o CERTO e o teste exige que o código
    ainda erre; quando acertar, o teste falha pedindo para tirar o `pendente`;
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
from typing import Dict, List, Optional

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
    principal: Optional[str]
    fluxo: str
    situacao: str
    confianca: Optional[str] = None   # None = não confere a confiança


@dataclass(frozen=True)
class Caso:
    arquivo: str                      # relativo a `dados/`
    porque: str                       # o que o caso ensina (o limite que ele marca)
    esperados: Dict[int, Esperado]    # id do desembarque -> gabarito
    pendente: Optional[str] = None    # defeito conhecido, ainda não corrigido


IMPORTACAO_CAAU = '152605316468892'
CASOS: Dict[str, Caso] = {
    'CAAU5545018': Caso(
        'caso_ciclo_CAAU5545018.json',
        'Cabotagem que SAI de Santos (origem BRSSZ) não é o CE do desembarque; o manifesto do CE de importação não tem '
        'escala ligada no Mercante e vale o início da operação; o CE declarado no desembarque e no CLIA confirma.',
        {12212834: Esperado(IMPORTACAO_CAAU, ldt.TRA, ldt.CONCLUIDO, ldt.ALTA),     # CAAU5545018
         12212278: Esperado(IMPORTACAO_CAAU, ldt.TRA, ldt.CONCLUIDO, ldt.ALTA)}),   # FANU1174955
}


def processa(caso: dict):
    """(ciclos do serviço por desembarque, portos dos desembarques) do caso."""
    engine = banco_do_caso(caso)
    return ciclos_do_servico(engine, caso), portos_do_caso(engine, caso)


def diferencas(esperados: Dict[int, Esperado], ciclos: Dict[int, dict]) -> List[str]:
    erros = []
    for desembarque, esperado in sorted(esperados.items()):
        ciclo = ciclos.get(desembarque)
        if ciclo is None:
            erros.append(f'desembarque {desembarque}: sem ciclo')
            continue
        obtido = Esperado(normaliza_ce(ciclo['numeroCEmercante']), ciclo['fluxo'], ciclo['situacao'],
                          ciclo['confianca_ce'] if esperado.confianca else None)
        if obtido != esperado:
            erros.append(f'desembarque {desembarque} ({ciclo["numeroConteiner"]}): esperado {esperado}, '
                         f'obtido {obtido}')
    return erros


class TestCasosReais(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.resultados = {}
        for nome, caso in CASOS.items():
            dados = json.loads((DADOS / caso.arquivo).read_text(encoding='utf-8'))
            cls.resultados[nome] = (dados, *processa(dados))

    def test_gabarito(self):
        for nome, caso in CASOS.items():
            with self.subTest(caso=nome):
                erros = diferencas(caso.esperados, self.resultados[nome][1])
                if caso.pendente:
                    self.assertTrue(erros, f'o defeito pendente foi corrigido ({caso.pendente}): tirar o "pendente"')
                else:
                    self.assertEqual(erros, [], caso.porque)

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
