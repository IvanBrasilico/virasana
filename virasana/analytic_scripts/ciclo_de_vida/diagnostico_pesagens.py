# coding: utf-8
"""
DIAGNÓSTICO DAS PESAGENS (PesagemVeiculo, API Recintos) — fase P0 do plano aprovado em 20/10/2026 (CICLO_DE_VIDA.md,
"Fora de Santos e pesagens"). Somente leitura. Regras em `pesagens_ciclo.py`.

  1. PREENCHIMENTO por recinto: pesagens, com contêiner, com placa, com semirreboque, captura automática,
     `taraConjunto`, tara do semirreboque, peso do manifesto, peso zero, retificação, contingência;
  2. DIVISÃO de cada balança (rodoviária = com placa; RTG = sem placa), por captura automática: quanto dos pesos é
     múltiplo de 100/70/50/20/10/5/2/1 kg, a divisão inferida e quantos ficam FORA dela (valor digitado);
  3. PESAGEM x GATE (amostra): distância ao acesso (efetivado) mais próximo da mesma placa no mesmo recinto;
  4. TARA (amostra de pesagens com contêiner e placa): par na mesma visita (o mesmo conjunto sem contêiner, mesmo
     recinto, até 12 h), pesagem sem contêiner do mesmo cavalo em até 7 dias, `taraConjunto` da API — e quanto a
     `taraConjunto` diverge do par;
  5. RTG: pesagens com contêiner e sem placa (o peso já é o do contêiner);
  6. MEDIDO x DECLARADO (amostra com tara do conjunto ou RTG): CE de importação do contêiner (item no Mercante, CE
     emitido até 120 dias antes), tara do contêiner (Mercante ou típica do tamanho), medido = bruto − tara do conjunto
     − tara do contêiner x `Item.pesoBruto` (base do usuário); classificação PRÉVIA (tolerâncias provisórias) por
     recinto e exemplos.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    DIAG=virasana/analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py
    virasana-venv/bin/python $DIAG --saida pesagens.log
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

from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402
from virasana.navii.mercante_por_id import limita_tempo  # noqa: E402

HORAS_PAR = 12                 # par na mesma visita
DIAS_VAZIA = 7                 # pesagem sem contêiner do mesmo cavalo
HORAS_GATE = 6
DIAS_CE = 120                  # CE emitido até N dias antes da pesagem
RECINTOS_MAXIMO = 15           # recintos da amostra (os de mais pesagens com contêiner)
EXEMPLOS = 5
LIMITE_CONSULTA = 300
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
                 recintos: Optional[Sequence[str]] = None, agora: Optional[datetime] = None, semente: int = 42):
        super().__init__(conexao, saida, inicio, fim, agora=agora)
        self.amostra, self.recintos_pedidos = amostra, list(recintos or [])
        self.sorteio = random.Random(semente)
        self.nomes: Dict[str, str] = {}
        self.divisoes: Dict[str, Optional[int]] = {}
        self.carregadas: List[dict] = []
        self.candidatas: List[dict] = []

    def _nome(self, recinto: str) -> str:
        return f'{recinto} {self.nomes.get(recinto, "")}'.strip()

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
        return [r for _, r in com_conteiner[:RECINTOS_MAXIMO]]

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
        self.candidatas = [pesagem_de(linha) for linha in self._por_lotes(
            f'SELECT {COLUNAS_PESAGEM} FROM apirecintos_pesagensveiculo WHERE placa IN :placas '  # nosec
            'AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia <= :ate', 'placas',
            {p['placa'] for p in self.carregadas}, params)]

    # ---------------------------------------------------------------- 3
    def secao_gate(self):
        self.titulo('3. PESAGEM x GATE — acesso efetivado mais próximo da mesma placa no mesmo recinto (amostra)')
        if not self.carregadas:
            self.escreve('Sem amostra.')
            return
        folga = timedelta(hours=HORAS_GATE)
        acessos = defaultdict(list)
        for placa, recinto, data, direcao in self._por_lotes(
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
        por_placa = defaultdict(list)
        for candidata in self.candidatas:
            por_placa[candidata['placa']].append(candidata)
        resultado = {}
        for pesagem in self.carregadas:
            candidatas = por_placa[pesagem['placa']]
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
        contagem, divergencia_api, valores_par = Counter(), [], []
        for pesagem in self.carregadas:
            tara = taras[pesagem['id']]
            fonte = ('par na visita' if tara['par'] else 'vazia em 7 dias' if tara['vazia'] else
                     'só taraConjunto' if tara['api'] else 'nenhuma')
            contagem[(pesagem['recinto'], fonte)] += 1
            if tara['par']:
                valores_par.append(tara['par']['peso'])
                if tara['api']:
                    divergencia_api.append(tara['api'] - tara['par']['peso'])
        por_recinto = Counter(p['recinto'] for p in self.carregadas)
        self.tabela(['recinto', 'melhor tara disponível', 'pesagens'],
                    [(self._nome(r), fonte, _pct(n, por_recinto[r])) for (r, fonte), n in sorted(contagem.items())])
        self.escreve()
        self.escreve(f'Tara do conjunto pelo par (kg): {percentis(valores_par)}')
        acima = sum(1 for d in divergencia_api if abs(d) > 500)
        self.escreve(f'taraConjunto da API − par (kg), {len(divergencia_api)} casos: {percentis(divergencia_api)}; '
                     f'acima de 500 kg de diferença: {_pct(acima, len(divergencia_api))}')

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
    def declarados(self) -> Dict[str, List[tuple]]:
        """{contêiner: [(dataEmissao, CE, pesoBruto, tara, iso)]} dos CEs de importação (não exportação)."""
        por_conteiner = defaultdict(list)
        for conteiner, ce, peso, tara, iso, emissao in self._por_lotes(
                'SELECT i.codigoConteiner, c.numeroCEmercante, i.pesoBruto, i.tara, i.isoCode, c.dataEmissao '
                'FROM itensresumo i JOIN conhecimentosresumo c ON c.numeroCEmercante = i.numeroCEmercante '
                "WHERE i.codigoConteiner IN :conteineres AND (c.tipoTrafego IS NULL OR c.tipoTrafego <> '7')",
                'conteineres', {p['conteiner'] for p in self.carregadas}):
            data = _data(emissao)
            if data:
                por_conteiner[_texto(conteiner)].append((data, _texto(ce), peso, tara, _texto(iso)))
        return por_conteiner

    @staticmethod
    def declarado_da_pesagem(pesagem: dict, declarados: Dict[str, List[tuple]]) -> Optional[tuple]:
        """O item do CE emitido mais recentemente antes da pesagem (até DIAS_CE dias)."""
        candidatos = [d for d in declarados.get(pesagem['conteiner'], [])
                      if pesagem['data'] - timedelta(days=DIAS_CE) <= d[0] <= pesagem['data']]
        return max(candidatos, key=lambda d: d[0]) if candidatos else None

    def mede(self, pesagem: dict, taras: Dict[int, dict], declarados) -> Tuple[str, Optional[float], str]:
        """(classificação, diferença % ou None, linha de exemplo) de uma pesagem carregada."""
        opcoes = taras[pesagem['id']]
        tara = opcoes['par'] or opcoes['vazia']
        item = self.declarado_da_pesagem(pesagem, declarados)
        if not tara or not item or not pesagem['peso']:
            return pc.SEM_DADO + (' (sem tara)' if not tara else ' (sem CE)'), None, ''
        _, ce, peso_declarado, tara_item, iso = item
        tara_conteiner, origem_tara = pc.tara_do_conteiner(tara_item, iso)
        medido = pesagem['peso'] - tara['peso'] - (tara_conteiner or 0)
        declarado = _float(str(peso_declarado).replace(',', '.')) if peso_declarado is not None else None
        divisao = self.divisoes.get(pesagem['recinto'])
        categoria, detalhe = pc.classifica(declarado, medido, divisao, iso)
        if pc.fora_da_divisao(pesagem['peso'], divisao):
            categoria += ' — pesagem fora da divisão'
        exemplo = (f'{pesagem["conteiner"]} CE {ce} em {pesagem["data"]:%d/%m %H:%M}: bruto {pesagem["peso"]:.0f} − '
                   f'conjunto {tara["peso"]:.0f} ({"par" if tara is opcoes["par"] else "vazia"}) − contêiner '
                   f'{tara_conteiner or 0:.0f} ({origem_tara}) = {medido:.0f} x declarado {declarado or 0:.0f} — '
                   f'{detalhe}; captura automática {"sim" if pesagem["captura"] else "não"}')
        return categoria, (100 * (medido - declarado) / declarado if declarado else None), exemplo

    def secao_medido(self, taras: Dict[int, dict]):
        self.titulo('6. MEDIDO x DECLARADO — bruto − tara do conjunto − tara do contêiner x Item.pesoBruto '
                    '(classificação PRÉVIA, tolerâncias provisórias)')
        if not self.carregadas:
            self.escreve('Sem amostra.')
            return
        declarados = self.declarados()
        contagem, exemplos, diferencas = Counter(), defaultdict(list), []
        for pesagem in self.carregadas:
            categoria, diferenca, exemplo = self.mede(pesagem, taras, declarados)
            chave = (pesagem['recinto'], categoria)
            contagem[chave] += 1
            if diferenca is not None:
                diferencas.append(diferenca)
            if exemplo and len(exemplos[chave]) < EXEMPLOS:
                exemplos[chave].append(exemplo)
        por_recinto = Counter(p['recinto'] for p in self.carregadas)
        self.tabela(['recinto', 'classificação prévia', 'pesagens'],
                    [(self._nome(r), c, _pct(n, por_recinto[r])) for (r, c), n in sorted(contagem.items())])
        self.escreve()
        self.escreve(f'Diferença (medido − declarado) / declarado, %: {percentis(diferencas)}')
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
                        help=f'recintos da amostra (padrão: os {RECINTOS_MAXIMO} com mais pesagens com contêiner)')
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
            diag = DiagnosticoPesagens(conexao, saida, inicio, agora, args.amostra, args.recintos, agora)
            diag.escreve(f'Diagnóstico das pesagens — {agora:%Y-%m-%d %H:%M}; pesagens de {inicio:%Y-%m-%d} a '
                         f'{agora:%Y-%m-%d}; amostra de {args.amostra} por recinto')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
