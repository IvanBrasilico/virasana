# coding: utf-8
"""
DIAGNÓSTICO DAS PESAGENS (PesagemVeiculo, API Recintos) — fases P0 e P0b do plano aprovado em 20/10/2026
(CICLO_DE_VIDA.md, "Fora de Santos e pesagens"). Somente leitura. Regras em `pesagens_ciclo.py`.

  1. PREENCHIMENTO por recinto: pesagens, com contêiner, com placa, com semirreboque, captura automática,
     `taraConjunto`, tara do semirreboque, peso do manifesto, peso zero, retificação, contingência;
  2. DIVISÃO de cada balança (rodoviária = com placa; RTG = sem placa), por captura automática: quanto dos pesos é
     múltiplo de 100/70/50/20/10/5/2/1 kg, a divisão inferida e quantos ficam FORA dela (valor digitado);
  3. PESAGEM x GATE (amostra): distância ao acesso (efetivado) mais próximo da mesma placa no mesmo recinto;
  4. TARA (amostra de pesagens com contêiner e placa): par na mesma visita (o mesmo conjunto sem o contêiner — ou com
     o mesmo contêiner informado e mais leve —, mesmo recinto, até 12 h), pesagem sem contêiner do mesmo cavalo em até
     7 dias, `taraConjunto` da API — e quanto a `taraConjunto` diverge do par;
  5. RTG: pesagens com contêiner e sem placa (o peso já é o do contêiner);
  6. MEDIDO x DECLARADO (P0b): a OPERAÇÃO de cada pesagem — depois de um desembarque do contêiner, pela direção do
     acesso da mesma placa (seção 3): retirada no operador do desembarque, volta a ele, chegada a recinto seco, outro
     operador portuário; sem desembarque antes, não comparada. A carregada da visita é a mais pesada; tara do conjunto
     par > vazia > `taraConjunto` (usuário aprovou, 20/10/2026), só entre 8 e 30 t; declarado = peso do contêiner na
     família de CEs (item do master ou soma dos filhotes); medido = bruto − tara do conjunto − tara do contêiner.
     Quadros: geral por operação e classificação, por recinto, por ORIGEM DA TARA (só as operações da importação),
     origem do declarado, diferenças por operação e exemplos.

A amostra (seções 3 a 6) são os 15 recintos com mais pesagens com contêiner MAIS os operadores fora de Santos do escopo
e o Rio ICTSI. `--d-e-trocados 7921302` destroca o embarque/desembarque do ICTSI (D3) para achar a operação.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    DIAG=virasana/analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py
    virasana-venv/bin/python $DIAG --d-e-trocados 7921302 --saida pesagens2.log
    virasana-venv/bin/python $DIAG --dias 14 --amostra 300 --recintos 8931356 7921302 --saida pesagens.log
"""
import argparse
import random
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.carga_ciclo import (  # noqa: E402
    OPERADORES_D_E_TROCADOS, OPERADORES_FORA_DE_SANTOS, tipo_movimento,
)
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402
from virasana.navii.mercante_por_id import limita_tempo  # noqa: E402

HORAS_PAR = 12                 # par na mesma visita
DIAS_VAZIA = 7                 # pesagem sem contêiner do mesmo cavalo
HORAS_GATE = 6
DIAS_CE = 120                  # CE emitido até N dias antes da pesagem
RECINTOS_MAXIMO = 15           # recintos da amostra (os de mais pesagens com contêiner) ...
RECINTOS_SEMPRE = OPERADORES_FORA_DE_SANTOS | {'7921302'}     # ... mais estes (P0b)
LOTE_PLACAS = 100              # P0: a consulta por placa em lotes de 500 estourou o max_statement_time
DIRECAO_ATE_MINUTOS = 30       # acesso da mesma placa até isto da pesagem: dá a direção (P0: ~95% a até 2 min)
EXEMPLOS = 5
LIMITE_CONSULTA = 300
NAO_COMPARADA = 'não comparada'
FAIXAS_GATE = ((2, 'até 2 min'), (10, '2 a 10 min'), (60, '10 a 60 min'), (HORAS_GATE * 60, f'1 a {HORAS_GATE} h'))
PESAGENS = 'FROM apirecintos_pesagensveiculo WHERE dataHoraOcorrencia >= :inicio AND dataHoraOcorrencia < :fim'
COM_CONTEINER = "numeroConteiner IS NOT NULL AND numeroConteiner <> ''"
COM_PLACA = "placa IS NOT NULL AND placa <> ''"
COLUNAS_PESAGEM = ('id, dataHoraOcorrencia, codigoRecinto, placa, placaSemirreboque, numeroConteiner, '
                   'pesoBrutoBalanca, taraConjunto, capturaAutoPeso')


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _float(valor) -> Optional[float]:
    try:
        return float(valor) if valor is not None else None
    except (TypeError, ValueError):
        return None


def _pct(parte: int, total: int) -> str:
    return f'{parte} ({100 * parte / total:.0f}%)' if total else str(parte)


def _base(categoria: str) -> str:
    """A categoria sem a marca ' — pesagem fora da divisão'."""
    return categoria.split(' — ')[0]


def percentis(valores: List[float]) -> str:
    if not valores:
        return '-'
    valores = sorted(valores)

    def p(fracao):
        return valores[min(len(valores) - 1, int(round(fracao * (len(valores) - 1))))]
    return f'p10 {p(.1):,.0f} | mediana {statistics.median(valores):,.0f} | p90 {p(.9):,.0f}'.replace(',', '.')


def pesagem_de(linha) -> dict:
    id_, data, recinto, placa, semirreboque, conteiner, peso, tara_conjunto, captura = linha
    return {'id': id_, 'data': _dt(data), 'recinto': _texto(recinto), 'placa': _texto(placa),
            'semirreboque': _texto(semirreboque), 'conteiner': _texto(conteiner), 'peso': _float(peso),
            'tara_conjunto': _float(tara_conjunto), 'captura': bool(captura)}


class DiagnosticoPesagens(Diagnostico):

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, amostra: int = 200,
                 recintos: Optional[Sequence[str]] = None, agora: Optional[datetime] = None, semente: int = 42,
                 trocados=OPERADORES_D_E_TROCADOS):
        super().__init__(conexao, saida, inicio, fim, agora=agora)
        self.amostra, self.recintos_pedidos = amostra, list(recintos or [])
        self.trocados = frozenset(trocados or ())
        self.sorteio = random.Random(semente)
        self.nomes: Dict[str, str] = {}
        self.divisoes: Dict[str, Optional[int]] = {}
        self.carregadas: List[dict] = []
        self.candidatas: List[dict] = []
        self.por_placa: Dict[str, List[dict]] = {}
        self.direcoes: Dict[int, str] = {}          # id da pesagem -> 'E'/'S' do acesso da mesma placa (seção 3)
        self.portuarios: set = set()                # recintos com embarque/desembarque (operadores portuários)

    def _nome(self, recinto: str) -> str:
        return f'{recinto} {self.nomes.get(recinto, "")}'.strip()

    def _em_partes(self, sql, nome, valores, params=None, tamanho=LOTE_PLACAS) -> list:
        """Como `_por_lotes`, em partes menores."""
        valores, linhas = sorted(v for v in set(valores) if v), []
        for i in range(0, len(valores), tamanho):
            linhas += self.consulta(sql, {**(params or {}), nome: valores[i:i + tamanho]}, listas=(nome,))
        return linhas

    def carrega_nomes(self):
        self.nomes = {_texto(c): _texto(n) for c, n in self.consulta(
            "SELECT cod_siscomex, nome FROM ovr_recintos WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''")}

    # ---------------------------------------------------------------- 1
    def secao_preenchimento(self):
        self.titulo('1. PREENCHIMENTO DAS PESAGENS POR RECINTO')
        linhas = self.consulta(
            'SELECT codigoRecinto, COUNT(*), '  # nosec B608 - SQL fixo
            f'SUM(CASE WHEN {COM_CONTEINER} THEN 1 ELSE 0 END), SUM(CASE WHEN {COM_PLACA} THEN 1 ELSE 0 END), '
            "SUM(CASE WHEN placaSemirreboque IS NOT NULL AND placaSemirreboque <> '' THEN 1 ELSE 0 END), "
            'SUM(CASE WHEN capturaAutoPeso = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN taraConjunto > 0 THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN taraSemirreboque > 0 THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN pesoBrutoManifesto > 0 THEN 1 ELSE 0 END), '
            'SUM(CASE WHEN pesoBrutoBalanca IS NULL OR pesoBrutoBalanca <= 0 THEN 1 ELSE 0 END), '
            "SUM(CASE WHEN tipoOperacao = 'R' THEN 1 ELSE 0 END), SUM(CASE WHEN contingencia = 1 THEN 1 ELSE 0 END) "
            + PESAGENS + ' GROUP BY codigoRecinto ORDER BY COUNT(*) DESC')
        self.tabela(['recinto', 'pesagens', 'com contêiner', 'com placa', 'com semirreboque', 'captura automática',
                     'taraConjunto', 'tara semirreboque', 'peso manifesto', 'peso zero', 'retificação', 'contingência'],
                    [[self._nome(_texto(r)), total] + [_pct(int(v or 0), int(total)) for v in valores]
                     for r, total, *valores in linhas])
        return {_texto(r): (int(total), int(com or 0)) for r, total, com, *_ in linhas}

    # ---------------------------------------------------------------- 2
    def secao_divisao(self):
        self.titulo('2. DIVISÃO DE CADA BALANÇA (rodoviária = com placa; RTG = sem placa) — peso fora dela = digitado')
        somas = ', '.join(f'SUM(CASE WHEN pesoBrutoBalanca % {d} = 0 THEN 1 ELSE 0 END)' for d in pc.DIVISOES)
        linhas = self.consulta(
            f"SELECT codigoRecinto, CASE WHEN {COM_PLACA} THEN 'rodoviária' "  # nosec B608 - SQL fixo
            "ELSE 'RTG/sem placa' END AS balanca, "
            'CASE WHEN capturaAutoPeso = 1 THEN 1 ELSE 0 END AS captura, COUNT(*), '
            'SUM(CASE WHEN pesoBrutoBalanca <> ROUND(pesoBrutoBalanca) THEN 1 ELSE 0 END), ' + somas + ' '
            + PESAGENS + ' AND pesoBrutoBalanca > 0 GROUP BY codigoRecinto, balanca, captura '
            'ORDER BY codigoRecinto, balanca, captura')
        tabela = []
        for recinto, balanca, captura, total, fracionados, *multiplos in linhas:
            recinto, total = _texto(recinto), int(total)
            contagens = {d: int(n or 0) for d, n in zip(pc.DIVISOES, multiplos)}
            divisao = pc.divisao_pelas_contagens(contagens, total)
            if balanca == 'rodoviária' and (captura or recinto not in self.divisoes):   # com captura prevalece
                self.divisoes[recinto] = divisao
            fora = total - contagens.get(divisao, total) if divisao else None
            tabela.append([self._nome(recinto), balanca, 'sim' if captura else 'não', total,
                           f'{divisao} kg' if divisao else 'fracionado/variado', _pct(fora, total) if fora is not None
                           else '-', _pct(int(fracionados or 0), total)] +
                          [f'{100 * contagens[d] / total:.0f}%' for d in (100, 50, 20, 10)])
        self.tabela(['recinto', 'balança', 'captura automática', 'pesagens', 'divisão inferida', 'fora da divisão',
                     'kg fracionado', 'múlt. 100', 'múlt. 50', 'múlt. 20', 'múlt. 10'], tabela)
        self.escreve(f'Divisão = a maior em que >= {pc.PROPORCAO_DIVISAO:.0%} dos pesos são múltiplos. A da balança '
                     'rodoviária com captura automática é a usada nas seções seguintes.')

    # ---------------------------------------------------------------- amostra
    def recintos_da_amostra(self, volumes: Dict[str, tuple]) -> List[str]:
        if self.recintos_pedidos:
            return self.recintos_pedidos
        com_conteiner = sorted(((com, r) for r, (_, com) in volumes.items() if com), reverse=True)
        maiores = [r for _, r in com_conteiner[:RECINTOS_MAXIMO]]
        return maiores + sorted(r for r in RECINTOS_SEMPRE if r not in maiores and volumes.get(r, (0, 0))[1])

    def carrega_amostra(self, recintos: List[str]):
        """Até `amostra` pesagens com contêiner e placa por recinto (sorteio, semente fixa) e as pesagens das mesmas
        placas de 7 dias antes a 7 dias depois (candidatas a tara)."""
        ids = []
        for recinto in recintos:
            todos = [i for (i,) in self.consulta(
                f'SELECT id {PESAGENS} AND codigoRecinto = :recinto AND {COM_CONTEINER} AND {COM_PLACA}',  # nosec
                {'recinto': recinto})]
            ids += self.sorteio.sample(todos, min(self.amostra, len(todos)))
        self.carregadas = [pesagem_de(linha) for linha in self._por_lotes(
            f'SELECT {COLUNAS_PESAGEM} FROM apirecintos_pesagensveiculo WHERE id IN :ids', 'ids', ids)]  # nosec
        if not self.carregadas:
            return
        folga = timedelta(days=DIAS_VAZIA)
        params = {'de': min(p['data'] for p in self.carregadas) - folga,
                  'ate': max(p['data'] for p in self.carregadas) + folga}
        self.candidatas = [pesagem_de(linha) for linha in self._em_partes(
            f'SELECT {COLUNAS_PESAGEM} FROM apirecintos_pesagensveiculo WHERE placa IN :placas '  # nosec
            'AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia <= :ate', 'placas',
            {p['placa'] for p in self.carregadas}, params)]
        self.por_placa = defaultdict(list)
        for candidata in self.candidatas:
            self.por_placa[candidata['placa']].append(candidata)

    # ---------------------------------------------------------------- 3
    def secao_gate(self):
        self.titulo('3. PESAGEM x GATE — acesso efetivado mais próximo da mesma placa no mesmo recinto (amostra)')
        if not self.carregadas:
            self.escreve('Sem amostra.')
            return
        folga = timedelta(hours=HORAS_GATE)
        acessos = defaultdict(list)
        for placa, recinto, data, direcao in self._em_partes(
                'SELECT placa, codigoRecinto, dataHoraOcorrencia, direcao FROM apirecintos_acessosveiculo '
                "WHERE placa IN :placas AND operacao = 'C' AND dataHoraOcorrencia >= :de "
                'AND dataHoraOcorrencia <= :ate',
                'placas', {p['placa'] for p in self.carregadas},
                {'de': min(p['data'] for p in self.carregadas) - folga,
                 'ate': max(p['data'] for p in self.carregadas) + folga}):
            acessos[(_texto(placa), _texto(recinto))].append((_dt(data), _texto(direcao)))
        contagem = Counter()
        for pesagem in self.carregadas:
            distancias = [(abs((data - pesagem['data']).total_seconds()) / 60, direcao)
                          for data, direcao in acessos[(pesagem['placa'], pesagem['recinto'])]]
            proximo = min(distancias) if distancias else None
            if proximo and proximo[0] <= DIRECAO_ATE_MINUTOS:
                self.direcoes[pesagem['id']] = proximo[1]
            faixa = 'sem acesso em 6 h' if proximo is None or proximo[0] > HORAS_GATE * 60 else \
                next(nome for limite, nome in FAIXAS_GATE if proximo[0] <= limite)
            direcao = '' if proximo is None or faixa.startswith('sem') else \
                (' (entrada)' if proximo[1] == 'E' else ' (saída)')
            contagem[(pesagem['recinto'], faixa + direcao)] += 1
        por_recinto = Counter(p['recinto'] for p in self.carregadas)
        self.tabela(['recinto', 'acesso mais próximo', 'pesagens'],
                    [(self._nome(r), faixa, _pct(n, por_recinto[r])) for (r, faixa), n in sorted(contagem.items())])

    # ---------------------------------------------------------------- 4
    def taras(self) -> Dict[int, dict]:
        """{id da pesagem carregada: {par, vazia, api}} — o par na visita, a pesagem vazia em 7 dias, a da API."""
        resultado = {}
        for pesagem in self.carregadas:
            candidatas = self.por_placa.get(pesagem['placa'], [])
            par = pc.pareia_tara(pesagem, candidatas, HORAS_PAR)
            vazias = [c for c in candidatas if not c['conteiner'] and c['peso']]
            vazia = min(vazias, key=lambda c: abs((c['data'] - pesagem['data']).total_seconds())) if vazias else None
            if vazia and abs((vazia['data'] - pesagem['data']).total_seconds()) > DIAS_VAZIA * 86400:
                vazia = None
            resultado[pesagem['id']] = {'par': par, 'vazia': vazia, 'api': pesagem['tara_conjunto']}
        return resultado

    def secao_tara(self, taras: Dict[int, dict]):
        self.titulo(f'4. TARA DO CONJUNTO — par na visita (até {HORAS_PAR} h), vazia em {DIAS_VAZIA} dias, '
                    'taraConjunto da API (amostra)')
        if not self.carregadas:
            self.escreve('Sem amostra.')
            return
        contagem, divergencia_api, divergencia_vazia, valores_par = Counter(), [], [], []
        for pesagem in self.carregadas:
            tara = taras[pesagem['id']]
            fonte = ('par na visita' if tara['par'] else 'vazia em 7 dias' if tara['vazia'] else
                     'só taraConjunto' if tara['api'] else 'nenhuma')
            contagem[(pesagem['recinto'], fonte)] += 1
            if tara['par']:
                valores_par.append(tara['par']['peso'])
                if tara['api']:
                    divergencia_api.append(tara['api'] - tara['par']['peso'])
                if tara['vazia']:
                    divergencia_vazia.append(tara['vazia']['peso'] - tara['par']['peso'])
        por_recinto = Counter(p['recinto'] for p in self.carregadas)
        self.tabela(['recinto', 'melhor tara disponível', 'pesagens'],
                    [(self._nome(r), fonte, _pct(n, por_recinto[r])) for (r, fonte), n in sorted(contagem.items())])
        self.escreve()
        self.escreve(f'Tara do conjunto pelo par (kg): {percentis(valores_par)}')
        for nome, divergencia in (('taraConjunto da API', divergencia_api), ('vazia em 7 dias', divergencia_vazia)):
            acima = sum(1 for d in divergencia if abs(d) > 500)
            self.escreve(f'{nome} − par (kg), {len(divergencia)} casos: {percentis(divergencia)}; '
                         f'acima de 500 kg de diferença: {_pct(acima, len(divergencia))}')
        self._tara_da_api_por_recinto()

    def _tara_da_api_por_recinto(self):
        """A `taraConjunto` por recinto: P0b achou a tara do CONTÊINER nela (Suape, ~4,6 t)."""
        valores = defaultdict(list)
        for pesagem in self.carregadas:
            if pesagem['tara_conjunto']:
                valores[pesagem['recinto']].append(pesagem['tara_conjunto'])
        faixa = pc.TARA_CONJUNTO_PLAUSIVEL
        self.escreve()
        self.tabela(['recinto', 'com taraConjunto', 'taraConjunto (kg)', f'fora de {faixa[0]:.0f} a {faixa[1]:.0f} kg'],
                    [(self._nome(r), len(v), percentis(v).replace(' | ', ' / '),
                      _pct(sum(1 for x in v if not faixa[0] <= x <= faixa[1]), len(v)))
                     for r, v in sorted(valores.items())])

    # ---------------------------------------------------------------- 5
    def secao_rtg(self):
        self.titulo('5. RTG — pesagens com contêiner e SEM placa (o peso já é o do contêiner)')
        self.tabela(['recinto', 'pesagens', 'peso médio (kg)', 'menor', 'maior'], [
            (self._nome(_texto(r)), n, f'{float(media or 0):.0f}', f'{float(menor or 0):.0f}',
             f'{float(maior or 0):.0f}')
            for r, n, media, menor, maior in self.consulta(
                'SELECT codigoRecinto, COUNT(*), AVG(pesoBrutoBalanca), MIN(pesoBrutoBalanca), '  # nosec B608
                f'MAX(pesoBrutoBalanca) {PESAGENS} AND {COM_CONTEINER} '
                "AND (placa IS NULL OR placa = '') GROUP BY codigoRecinto ORDER BY COUNT(*) DESC")])

    # ---------------------------------------------------------------- 6
    def movimentos(self) -> Dict[str, List[Tuple[datetime, str, bool]]]:
        """{contêiner: [(data, recinto, é desembarque)]} desde DIAS_OPERACAO antes da 1ª pesagem da amostra."""
        movimentos = defaultdict(list)
        if not self.carregadas:
            return movimentos
        de = min(p['data'] for p in self.carregadas) - timedelta(days=pc.DIAS_OPERACAO)
        for conteiner, data, recinto, tipo in self._por_lotes(
                'SELECT numeroConteiner, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque '
                'FROM apirecintos_embarquedesembarque WHERE numeroConteiner IN :conteineres '
                'AND dataHoraOcorrencia >= :de',
                'conteineres', {p['conteiner'] for p in self.carregadas}, {'de': de}):
            recinto = _texto(recinto)
            self.portuarios.add(recinto)
            movimentos[_texto(conteiner)].append(
                (_dt(data), recinto, tipo_movimento(tipo, recinto, self.trocados) == ldt.DESEMBARQUE))
        return movimentos

    def declarados(self) -> Dict[str, List[tuple]]:
        """{contêiner: [(dataEmissao, CE, master, pesoBruto, tara, iso)]} dos CEs de importação (não exportação)."""
        por_conteiner = defaultdict(list)
        for conteiner, ce, master, peso, tara, iso, emissao in self._por_lotes(
                'SELECT i.codigoConteiner, c.numeroCEmercante, c.numeroCEMaster, i.pesoBruto, i.tara, i.isoCode, '
                'c.dataEmissao FROM itensresumo i '
                'JOIN conhecimentosresumo c ON c.numeroCEmercante = i.numeroCEmercante '
                "WHERE i.codigoConteiner IN :conteineres AND (c.tipoTrafego IS NULL OR c.tipoTrafego <> '7')",
                'conteineres', {p['conteiner'] for p in self.carregadas}):
            data = _data(emissao)
            if data:
                por_conteiner[_texto(conteiner)].append((data, _texto(ce), _texto(master) or None, peso, tara,
                                                         _texto(iso)))
        return por_conteiner

    @staticmethod
    def declarado_da_pesagem(pesagem: dict, declarados: Dict[str, List[tuple]]) -> Optional[dict]:
        """A família (master + filhotes) do CE emitido mais recentemente antes da pesagem (até DIAS_CE dias): CE, peso
        declarado do contêiner e a origem dele, tara e tipo ISO."""
        candidatos = [d for d in declarados.get(pesagem['conteiner'], [])
                      if pesagem['data'] - timedelta(days=DIAS_CE) <= d[0] <= pesagem['data']]
        if not candidatos:
            return None
        recente = max(candidatos, key=lambda d: d[0])
        chave = recente[2] or recente[1]
        familia = [d for d in candidatos if (d[2] or d[1]) == chave]
        peso, origem = pc.peso_da_familia([(ce, master, peso) for _, ce, master, peso, _, _ in familia])
        tara = next((d[4] for d in familia if pc.tara_do_conteiner(d[4], d[5])[1] == 'informada'), recente[4])
        iso = recente[5] or next((d[5] for d in familia if d[5]), '')
        return {'ce': recente[1], 'peso': peso, 'origem': origem, 'tara': tara, 'iso': iso}

    def mede(self, pesagem: dict, taras: Dict[int, dict], declarados, movimentos) -> dict:
        """{contexto, categoria, diferença %, origem da tara, origem do declarado, exemplo} de uma pesagem."""
        contexto = pc.contexto_da_pesagem(pesagem['data'], pesagem['recinto'], movimentos.get(pesagem['conteiner'], []),
                                          self.portuarios, self.direcoes.get(pesagem['id']))
        medida = {'contexto': contexto, 'categoria': NAO_COMPARADA, 'diferenca': None, 'tara': '-', 'declarado': '-',
                  'exemplo': ''}
        if contexto == pc.SEM_DESEMBARQUE:
            return medida
        if not pc.e_a_carregada(pesagem, self.por_placa.get(pesagem['placa'], []), HORAS_PAR):
            return {**medida, 'categoria': f'{pc.SEM_CONTEINER} (a carregada é a outra pesagem da visita)'}
        return {**medida, **self._compara(pesagem, taras[pesagem['id']], declarados)}

    def _compara(self, pesagem: dict, opcoes: dict, declarados) -> dict:
        conjunto, origem_tara = pc.tara_do_conjunto(opcoes['par'] and opcoes['par']['peso'],
                                                    opcoes['vazia'] and opcoes['vazia']['peso'], opcoes['api'])
        familia = self.declarado_da_pesagem(pesagem, declarados)
        if conjunto is None or familia is None or not pesagem['peso']:
            return {'categoria': pc.SEM_DADO + (' (sem tara)' if conjunto is None else ' (sem CE)'),
                    'tara': origem_tara}
        tara_conteiner, origem_conteiner = pc.tara_do_conteiner(familia['tara'], familia['iso'])
        medido = pesagem['peso'] - conjunto - (tara_conteiner or 0)
        declarado, divisao = familia['peso'], self.divisoes.get(pesagem['recinto'])
        categoria, detalhe = pc.classifica(declarado, medido, divisao, familia['iso'])
        if pc.fora_da_divisao(pesagem['peso'], divisao):
            categoria += ' — pesagem fora da divisão'
        exemplo = (f'{pesagem["conteiner"]} CE {familia["ce"]} em {pesagem["data"]:%d/%m %H:%M}: bruto '
                   f'{pesagem["peso"]:.0f} − conjunto {conjunto:.0f} ({origem_tara}) − contêiner '
                   f'{tara_conteiner or 0:.0f} ({origem_conteiner}) = {medido:.0f} x declarado {declarado or 0:.0f} '
                   f'({familia["origem"]}) — {detalhe}; captura automática {"sim" if pesagem["captura"] else "não"}')
        return {'categoria': categoria, 'tara': origem_tara, 'declarado': familia['origem'], 'exemplo': exemplo,
                'diferenca': 100 * (medido - declarado) / declarado if declarado else None}

    def secao_medido(self, taras: Dict[int, dict]):
        self.titulo('6. MEDIDO x DECLARADO (P0b) — por operação; bruto − tara do conjunto − tara do contêiner x '
                    'peso do contêiner na família de CEs')
        if not self.carregadas:
            self.escreve('Sem amostra.')
            return
        declarados, movimentos = self.declarados(), self.movimentos()
        medidas = [(p, self.mede(p, taras, declarados, movimentos)) for p in self.carregadas]
        geral = Counter((m['contexto'], m['categoria']) for _, m in medidas)
        self.tabela(['operação', 'classificação', 'pesagens'],
                    [(c, cat, _pct(n, len(medidas))) for (c, cat), n in sorted(geral.items())])
        self.escreve()
        por_recinto = Counter(p['recinto'] for p, _ in medidas)
        contagem = Counter((p['recinto'], m['contexto'], m['categoria']) for p, m in medidas)
        self.tabela(['recinto', 'operação', 'classificação', 'pesagens'],
                    [(self._nome(r), c, cat, _pct(n, por_recinto[r])) for (r, c, cat), n in sorted(contagem.items())])
        comparaveis = [(p, m) for p, m in medidas if _base(m['categoria']) in pc.COMPARAVEIS]
        da_importacao = [(p, m) for p, m in comparaveis if m['contexto'] in pc.OPERACOES_DA_IMPORTACAO]
        self._por_origem_da_tara(da_importacao)
        self._diferencas_e_exemplos(medidas, comparaveis)

    def _por_origem_da_tara(self, comparaveis):
        self.escreve()
        por_tara = Counter(m['tara'] for _, m in comparaveis)
        contagem = Counter((m['tara'], _base(m['categoria'])) for _, m in comparaveis)
        self.tabela(['origem da tara (operações da importação)', 'pesagens'] + list(pc.COMPARAVEIS),
                    [(t, n, *(_pct(contagem[(t, c)], n) for c in pc.COMPARAVEIS)) for t, n in sorted(por_tara.items())])
        self.escreve()
        self.tabela(['origem do peso declarado (operações da importação)', 'pesagens'],
                    sorted(Counter(m['declarado'] for _, m in comparaveis).items()))

    def _diferencas_e_exemplos(self, medidas, comparaveis):
        self.escreve()
        for contexto in (pc.RETIRADA, pc.OPERADOR_SEM_ACESSO, pc.CHEGADA_AO_SECO, pc.VOLTA_AO_OPERADOR,
                         pc.OUTRO_OPERADOR):
            diferencas = [m['diferenca'] for _, m in comparaveis if m['contexto'] == contexto]
            self.escreve(f'Diferença (medido − declarado) / declarado, %, {contexto}: {percentis(diferencas)}')
        exemplos = defaultdict(list)
        for pesagem, medida in medidas:
            chave = (pesagem['recinto'], medida['categoria'])
            if medida['exemplo'] and len(exemplos[chave]) < EXEMPLOS:
                exemplos[chave].append(medida['exemplo'])
        for (recinto, categoria), linhas in sorted(exemplos.items()):
            self.escreve()
            self.escreve(f'--- {self._nome(recinto)} — {categoria}')
            for linha in linhas:
                self.escreve(f'   {linha}')


def _data(valor) -> Optional[datetime]:
    try:
        return datetime.strptime(_texto(valor)[:10], '%Y-%m-%d')
    except ValueError:
        return None


def roda(diag: DiagnosticoPesagens):
    diag.carrega_nomes()
    volumes = diag.secao_preenchimento()
    diag.secao_divisao()
    diag.carrega_amostra(diag.recintos_da_amostra(volumes))
    diag.secao_gate()
    taras = diag.taras()
    diag.secao_tara(taras)
    diag.secao_rtg()
    diag.secao_medido(taras)
    diag.resumo(7)


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--dias', type=int, default=14, help='pesagens dos últimos N dias (padrão 14)')
    parser.add_argument('--amostra', type=int, default=200, help='pesagens com contêiner por recinto (padrão 200)')
    parser.add_argument('--recintos', nargs='+',
                        help=f'recintos da amostra (padrão: os {RECINTOS_MAXIMO} com mais pesagens com contêiner '
                             'e os operadores fora de Santos)')
    parser.add_argument('--d-e-trocados', nargs='+', metavar='CODIGO', default=sorted(OPERADORES_D_E_TROCADOS),
                        help='destroca embarque/desembarque nesses operadores (D3: o ICTSI 7921302 informa trocados)')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    engine = create_engine(uri_do_banco(args), pool_pre_ping=True)
    limita_tempo(engine, args.limite)
    agora = datetime.now()
    inicio = agora - timedelta(days=args.dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoPesagens(conexao, saida, inicio, agora, args.amostra, args.recintos, agora,
                                       trocados=args.d_e_trocados)
            diag.escreve(f'Diagnóstico das pesagens (P0b) — {agora:%Y-%m-%d %H:%M}; pesagens de {inicio:%Y-%m-%d} a '
                         f'{agora:%Y-%m-%d}; amostra de {args.amostra} por recinto; D/E destrocados: '
                         f'{", ".join(args.d_e_trocados) or "nenhum"}')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
