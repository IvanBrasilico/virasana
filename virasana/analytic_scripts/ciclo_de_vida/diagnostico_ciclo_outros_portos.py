# coding: utf-8
"""
DIAGNÓSTICO DO CICLO DE VIDA FORA DE SANTOS — fase D1 do plano aprovado em 20/10/2026 (CICLO_DE_VIDA.md, "Fora de
Santos e pesagens"). Somente leitura (SQL e, com o Mongo, as imagens).

O ciclo de vida v2 só abre ciclo nos operadores da unidade 0817800 (decisão de 01/10/2026); fora dela, o card do
NAVI-I fica sem imagem. Este diagnóstico mede o que haveria para ampliar o escopo e o que o fallback da imagem
(`navii/imagem_fallback.py`) já recupera:

  0. UNIDADES: operadores e recintos por unidade (`ovr_recintos`);
  1. EVENTOS por recinto fora da 0817800, com a qualidade: desembarques (D) e embarques (E), acessos efetivados com
     e sem contêiner, inspeções com e sem contêiner e vazias, pesagens com e sem contêiner (Santos como referência);
  2. "E" QUE PARECE DESEMBARQUE (amostra de embarques em operadores fora de Santos): com entrada no gate antes
     (embarque normal), com desembarque antes (passagem/remoção), sem nenhum dos dois e com inspeção/saída depois
     (parece um desembarque informado como embarque — o caso do 7921302), sem outros eventos; e o tráfego do CE
     declarado nesses eventos;
  3. NAVI-I — cards de CEs de importação com destino FORA de Santos (amostra de CEs criados há 15 a 45 dias, pela
     faixa de IDs): com ciclo, ciclo com imagem e, com o Mongo, quantos ganham imagem pelo fallback (pela inspeção ou
     só pela imagem) — por porto de destino;
  4. DEFASAGEM imagem (Mongo) − inspeção (API) por recinto fora de Santos (a tabela `imagens_ciclo.DEFASAGEM_HORAS`
     só mediu Santos);
  5. EXEMPLOS: linha do tempo de alguns contêineres sem imagem e dos que ganharam pelo fallback.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    DIAG=virasana/analytic_scripts/ciclo_de_vida/diagnostico_ciclo_outros_portos.py
    virasana-venv/bin/python $DIAG --saida outros_portos.log
    virasana-venv/bin/python $DIAG --dias 14 --amostra-cards 1500 --sem-mongo --saida outros_portos.log
"""
import argparse
import random
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco  # noqa: E402
from virasana.navii import imagem_fallback as fb  # noqa: E402
from virasana.navii.mercante_por_id import id_na_data, limita_tempo  # noqa: E402

UNIDADE_SANTOS = '0817800'
PORTO_SANTOS = 'BRSSZ'
DIAS_VIZINHANCA = 30            # eventos do contêiner em volta do embarque
EMBARQUES_POR_RECINTO = 300
CES_DE, CES_ATE = 45, 15        # CEs criados entre 45 e 15 dias atrás (já chegaram)
BLOCOS_CES = 60                 # blocos de IDs espalhados na faixa
INSPECOES_POR_RECINTO = 100
EXEMPLOS = 4
LIMITE_CONSULTA = 300
COM_CONTEINER = "numeroConteiner IS NOT NULL AND numeroConteiner <> ''"
SEM_CONTEINER = "(numeroConteiner IS NULL OR numeroConteiner = '')"
PERIODO = 'dataHoraOcorrencia >= :inicio AND dataHoraOcorrencia < :fim'

PARECE_DESEMBARQUE = 'parece desembarque (sem entrada nem D antes; inspeção/saída depois)'
COM_ENTRADA = 'embarque com entrada no gate antes'
COM_DESEMBARQUE = 'com desembarque antes (passagem/remoção)'
SEM_EVENTOS = 'sem outros eventos no recinto'


def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _unidade(valor) -> str:
    texto = _texto(valor)
    return texto.zfill(7) if texto.isdigit() else texto


def _pct(parte: int, total: int) -> str:
    return f'{parte} ({100 * parte / total:.0f}%)' if total else str(parte)


def classifica_embarque(embarque: datetime, recinto: str, eventos: List[tuple]) -> str:
    """Um embarque (E) no recinto, pelos eventos do contêiner no MESMO recinto em DIAS_VIZINHANCA dias:
    eventos = [(data, tipo, recinto)] com tipo em ('entrada', 'saída', 'desembarque', 'inspeção')."""
    antes = [t for d, t, r in eventos if r == recinto and embarque - timedelta(days=DIAS_VIZINHANCA) <= d < embarque]
    depois = [t for d, t, r in eventos if r == recinto and embarque < d <= embarque + timedelta(days=DIAS_VIZINHANCA)]
    if fb.ENTRADA in antes:
        return COM_ENTRADA
    if fb.DESEMBARQUE in antes:
        return COM_DESEMBARQUE
    if fb.SAIDA in depois or 'inspeção' in depois:
        return PARECE_DESEMBARQUE
    return SEM_EVENTOS


class DiagnosticoOutrosPortos(Diagnostico):

    def __init__(self, conexao, saida, inicio: datetime, fim: datetime, mongo_db=None, amostra_cards: int = 1500,
                 agora: Optional[datetime] = None, semente: int = 42):
        super().__init__(conexao, saida, inicio, fim, agora=agora)
        self.mongo_db, self.amostra_cards = mongo_db, amostra_cards
        self.sorteio = random.Random(semente)
        self.recintos: Dict[str, dict] = {}
        self.com_inspecao: Counter = Counter()      # recinto -> inspeções com contêiner no período (seção 1)
        self.exemplos: Dict[str, List[str]] = defaultdict(list)

    def _nome(self, recinto: str) -> str:
        dados = self.recintos.get(recinto)
        return f'{recinto} {dados["nome"]}' if dados else f'{recinto} (sem cadastro)'

    def _de_santos(self, recinto: str) -> bool:
        return (self.recintos.get(recinto) or {}).get('unidade') == UNIDADE_SANTOS

    # ---------------------------------------------------------------- 0
    def secao_unidades(self):
        self.titulo('0. UNIDADES — operadores e recintos (ovr_recintos)')
        for codigo, nome, operador, unidade in self.consulta(
                'SELECT cod_siscomex, nome, e_operador, cod_unidade FROM ovr_recintos '
                "WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''"):
            self.recintos[_texto(codigo)] = {'nome': _texto(nome), 'operador': bool(operador),
                                             'unidade': _unidade(unidade)}
        por_unidade = defaultdict(lambda: [0, 0])
        for dados in self.recintos.values():
            por_unidade[dados['unidade']][0 if dados['operador'] else 1] += 1
        self.tabela(['unidade', 'operadores', 'outros recintos', 'escopo do ciclo de vida'],
                    [(u or '(vazia)', op, outros, 'sim' if u == UNIDADE_SANTOS else 'não')
                     for u, (op, outros) in sorted(por_unidade.items())])

    # ---------------------------------------------------------------- 1
    def _contagens(self, sql: str) -> Dict[str, tuple]:
        return {_texto(r): tuple(int(v or 0) for v in valores) for r, *valores in self.consulta(sql)}

    def secao_eventos(self):
        self.titulo('1. EVENTOS POR RECINTO FORA DA 0817800 — e a qualidade (Santos como referência)')
        desembarques = self._contagens(
            "SELECT codigoRecinto, SUM(CASE WHEN embarqueDesembarque = 'D' THEN 1 ELSE 0 END), "  # nosec B608
            f"SUM(CASE WHEN embarqueDesembarque = 'E' THEN 1 ELSE 0 END) FROM apirecintos_embarquedesembarque "
            f'WHERE {PERIODO} GROUP BY codigoRecinto')
        acessos = self._contagens(
            f'SELECT codigoRecinto, COUNT(*), SUM(CASE WHEN {SEM_CONTEINER} THEN 1 ELSE 0 END) '  # nosec B608
            f"FROM apirecintos_acessosveiculo WHERE {PERIODO} AND operacao = 'C' GROUP BY codigoRecinto")
        inspecoes = self._contagens(
            f'SELECT codigoRecinto, COUNT(*), SUM(CASE WHEN {SEM_CONTEINER} THEN 1 ELSE 0 END), '  # nosec B608
            f'SUM(CASE WHEN vazio = 1 THEN 1 ELSE 0 END) FROM apirecintos_inspecoesnaoinvasivas WHERE {PERIODO} '
            'GROUP BY codigoRecinto')
        pesagens = self._contagens(
            f'SELECT codigoRecinto, COUNT(*), SUM(CASE WHEN {SEM_CONTEINER} THEN 1 ELSE 0 END) '  # nosec B608
            f'FROM apirecintos_pesagensveiculo WHERE {PERIODO} GROUP BY codigoRecinto')
        recintos = set(desembarques) | set(acessos) | set(inspecoes) | set(pesagens)
        self.com_inspecao = Counter({r: total - sem for r, (total, sem, _) in inspecoes.items() if total > sem})
        linhas, santos = [], [0] * 9
        for recinto in sorted(recintos, key=lambda r: -(desembarques.get(r, (0, 0))[0] + acessos.get(r, (0, 0))[0])):
            d, e = desembarques.get(recinto, (0, 0))
            a, a_sem = acessos.get(recinto, (0, 0))
            i, i_sem, i_vazio = inspecoes.get(recinto, (0, 0, 0))
            p, p_sem = pesagens.get(recinto, (0, 0))
            valores = [d, e, a, a_sem, i, i_sem, i_vazio, p, p_sem]
            if self._de_santos(recinto):
                santos = [x + y for x, y in zip(santos, valores)]
                continue
            dados = self.recintos.get(recinto) or {}
            linhas.append([self._nome(recinto), dados.get('unidade', '?'), 'sim' if dados.get('operador') else 'não',
                           d, e, a, _pct(a_sem, a), i, _pct(i_sem, i), _pct(i_vazio, i), p, _pct(p_sem, p)])
        d, e, a, a_sem, i, i_sem, i_vazio, p, p_sem = santos
        linhas.append(['SANTOS (0817800, todos)', UNIDADE_SANTOS, '-', d, e, a, _pct(a_sem, a), i, _pct(i_sem, i),
                       _pct(i_vazio, i), p, _pct(p_sem, p)])
        self.tabela(['recinto', 'unidade', 'operador', 'D', 'E', 'acessos', 'acessos sem contêiner', 'inspeções',
                     'inspeções sem contêiner', 'inspeções vazias', 'pesagens', 'pesagens sem contêiner'], linhas)

    # ---------------------------------------------------------------- 2
    def _eventos_dos_conteineres(self, conteineres: Set[str], de: datetime, ate: datetime) -> Dict[str, List[tuple]]:
        eventos = defaultdict(list)
        params = {'de': de, 'ate': ate}
        for cont, data, recinto, tipo in self._por_lotes(
                'SELECT numeroConteiner, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque '
                'FROM apirecintos_embarquedesembarque WHERE numeroConteiner IN :conts '
                'AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia <= :ate', 'conts', conteineres, params):
            eventos[_texto(cont)].append((_dt(data), fb.EMBARQUE if _texto(tipo) == 'E' else fb.DESEMBARQUE,
                                          _texto(recinto)))
        for cont, data, recinto, direcao in self._por_lotes(
                'SELECT numeroConteiner, dataHoraOcorrencia, codigoRecinto, direcao FROM apirecintos_acessosveiculo '
                "WHERE numeroConteiner IN :conts AND operacao = 'C' AND dataHoraOcorrencia >= :de "
                'AND dataHoraOcorrencia <= :ate', 'conts', conteineres, params):
            eventos[_texto(cont)].append((_dt(data), fb.SAIDA if _texto(direcao) == 'S' else fb.ENTRADA,
                                          _texto(recinto)))
        for cont, data, recinto in self._por_lotes(
                'SELECT numeroConteiner, dataHoraOcorrencia, codigoRecinto FROM apirecintos_inspecoesnaoinvasivas '
                'WHERE numeroConteiner IN :conts AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia <= :ate',
                'conts', conteineres, params):
            eventos[_texto(cont)].append((_dt(data), 'inspeção', _texto(recinto)))
        return eventos

    def secao_embarques(self):
        self.titulo(f'2. "E" QUE PARECE DESEMBARQUE — amostra de até {EMBARQUES_POR_RECINTO} embarques por operador '
                    'fora da 0817800')
        operadores = sorted(r for r, d in self.recintos.items() if d['operador'] and d['unidade'] != UNIDADE_SANTOS)
        embarques = []
        for recinto in operadores:
            linhas = self.consulta(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, numeroConhecimento '  # nosec B608 - SQL fixo
                f'FROM apirecintos_embarquedesembarque WHERE {PERIODO} AND codigoRecinto = :recinto '
                f"AND embarqueDesembarque = 'E' AND {COM_CONTEINER}",
                {'recinto': recinto})
            embarques += [(recinto,) + tuple(linha) for linha in
                          self.sorteio.sample(linhas, min(EMBARQUES_POR_RECINTO, len(linhas)))]
        if not embarques:
            self.escreve('Nenhum embarque com contêiner em operador fora de Santos no período.')
            return
        folga = timedelta(days=DIAS_VIZINHANCA)
        eventos = self._eventos_dos_conteineres({_texto(e[2]) for e in embarques},
                                                min(_dt(e[3]) for e in embarques) - folga,
                                                max(_dt(e[3]) for e in embarques) + folga)
        trafegos = {_texto(ce): (_texto(t), _texto(p)) for ce, t, p in self._por_lotes(
            'SELECT numeroCEmercante, tipoTrafego, portoDestFinal FROM conhecimentosresumo '
            'WHERE numeroCEmercante IN :ces', 'ces', {_texto(e[4]) for e in embarques if _texto(e[4])})}
        contagem, trafego_parece, destino_parece = Counter(), Counter(), Counter()
        for recinto, _, cont, data, ce in embarques:
            classe = classifica_embarque(_dt(data), recinto, eventos[_texto(cont)])
            contagem[(recinto, classe)] += 1
            if classe == PARECE_DESEMBARQUE:
                trafego, destino = trafegos.get(_texto(ce), ('', ''))
                trafego_parece[{'7': 'exportação', '': 'CE não informado/fora do Mercante'}.get(
                    trafego, f'tráfego {trafego}')] += 1
                destino_parece[(recinto, destino or '?')] += 1
                if len(self.exemplos[f'E que parece desembarque — {recinto}']) < EXEMPLOS:
                    self.exemplos[f'E que parece desembarque — {recinto}'].append(
                        self._linha_do_tempo(_texto(cont), _dt(data), eventos[_texto(cont)]))
        por_recinto = Counter(e[0] for e in embarques)
        self.tabela(['operador', 'classificação do embarque', 'embarques'],
                    [(self._nome(r), c, _pct(n, por_recinto[r])) for (r, c), n in sorted(contagem.items())])
        self.escreve()
        self.tabela(['CE declarado nos que parecem desembarque', 'embarques'], trafego_parece.most_common())
        self.escreve()
        self.tabela(['operador', 'destino final do CE declarado', 'embarques'],
                    [(self._nome(r), d, n) for (r, d), n in destino_parece.most_common(20)])

    def _linha_do_tempo(self, conteiner: str, centro: datetime, eventos: List[tuple],
                        imagens: Optional[List[tuple]] = None) -> str:
        perto = sorted((d, t, r) for d, t, r in eventos if abs(d - centro) <= timedelta(days=DIAS_VIZINHANCA))
        partes = [f'{d:%d/%m %H:%M} {t} {r}' for d, t, r in perto]
        partes += [f'{d:%d/%m %H:%M} imagem (Mongo)' for d, _ in sorted(imagens or [])]
        return f'{conteiner}: ' + '; '.join(partes)

    # ---------------------------------------------------------------- 3
    def cards_fora_de_santos(self) -> List[dict]:
        """Cards (CE de importação com destino fora de Santos + contêiner) de CEs criados entre CES_DE e CES_ATE dias
        atrás, em blocos de IDs espalhados na faixa."""
        de = id_na_data(self.conexao, self.agora - timedelta(days=CES_DE))
        ate = id_na_data(self.conexao, self.agora - timedelta(days=CES_ATE))
        if de is None or ate is None or ate <= de:
            return []
        passo = max(1, (ate - de) // BLOCOS_CES)
        tamanho = max(1, min(passo, self.amostra_cards // BLOCOS_CES * 3))
        cards = []
        for inicio in range(de, ate, passo):
            for ce, manifesto, destino, cont in self.consulta(
                    'SELECT c.numeroCEmercante, c.manifestoCE, c.portoDestFinal, i.codigoConteiner '
                    'FROM conhecimentosresumo c JOIN itensresumo i ON i.numeroCEmercante = c.numeroCEmercante '
                    "WHERE c.ID BETWEEN :de AND :ate AND (c.tipoTrafego IS NULL OR c.tipoTrafego <> '7') "
                    'AND c.portoDestFinal <> :santos AND i.codigoConteiner IS NOT NULL '
                    "AND i.codigoConteiner <> ''", {'de': inicio, 'ate': inicio + tamanho - 1, 'santos': PORTO_SANTOS}):
                cards.append({'numeroCEmercante': _texto(ce), 'manifesto': _texto(manifesto),
                              'portoDestFinal': _texto(destino) or '?', 'numeroConteiner': _texto(cont)})
        return self.sorteio.sample(cards, min(self.amostra_cards, len(cards)))

    def _previsoes(self, cards: List[dict]):
        """dataPrevistaAtracacao de cada card: a escala do manifesto no porto de destino; sem ela, a mais tardia."""
        escalas = defaultdict(list)
        for manifesto, porto, data in self._por_lotes(
                'SELECT em.manifesto, e.portoDaEscala, e.dataPrevistaAtracacao FROM escalamanifestoresumo em '
                'JOIN escalasresumo e ON e.numeroDaEscala = em.escala WHERE em.manifesto IN :manifestos',
                'manifestos', {c['manifesto'] for c in cards}):
            if _texto(data):
                escalas[_texto(manifesto)].append((_texto(porto), _texto(data)[:10]))
        for card in cards:
            lista = escalas.get(card['manifesto'], [])
            no_destino = [d for p, d in lista if p == card['portoDestFinal']]
            todas = [d for _, d in lista]
            card['dataPrevistaAtracacao'] = max(no_destino) if no_destino else (max(todas) if todas else None)

    def _ciclos(self, cards: List[dict]):
        """Ciclo (pelo contêiner e pelo CE ou pela família) e imagens do ciclo de cada card."""
        ciclos = defaultdict(list)
        for id_, cont, ce, desembarque in self._por_lotes(
                'SELECT ID, numeroConteiner, numeroCEmercante, data_desembarque FROM '
                'ciclodevida_ciclo_de_vida_importacao WHERE numeroConteiner IN :conts', 'conts',
                {c['numeroConteiner'] for c in cards}):
            ciclos[_texto(cont)].append((id_, _texto(ce), _dt(desembarque)))
        familia = defaultdict(set)
        ids = [id_ for lista in ciclos.values() for id_, _, _ in lista]
        for ciclo, ce in self._por_lotes('SELECT ciclo_de_vida_id, numeroCEmercante FROM ciclodevida_ciclo_ce '
                                         'WHERE ciclo_de_vida_id IN :ids', 'ids', ids):
            familia[ciclo].add(_texto(ce))
        imagens = defaultdict(list)
        for ciclo, imagem in self._por_lotes(
                'SELECT ciclo_de_vida_id, imagem_id FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos '
                'WHERE ciclo_de_vida_id IN :ids AND imagem_id IS NOT NULL', 'ids', ids):
            imagens[ciclo].append(_texto(imagem))
        for card in cards:
            do_ce = [(id_, d) for id_, ce, d in ciclos.get(card['numeroConteiner'], [])
                     if ce == card['numeroCEmercante'] or card['numeroCEmercante'] in familia[id_]]
            card['ciclo'] = bool(do_ce)
            card['imagens_ids'] = [i for id_, _ in do_ce for i in imagens[id_]]
            if do_ce:
                card['linhaDoTempo'] = {'desembarque': {'data': max(d for _, d in do_ce if d) if any(
                    d for _, d in do_ce) else None}}

    def secao_navii(self):
        self.titulo(f'3. NAVI-I — CEs de importação com destino FORA de Santos (criados há {CES_ATE} a {CES_DE} dias; '
                    f'amostra de até {self.amostra_cards} cards)')
        cards = self.cards_fora_de_santos()
        if not cards:
            self.escreve('Sem cards na amostra.')
            return
        self._previsoes(cards)
        self._ciclos(cards)
        tinham = {id(card) for card in cards if card['imagens_ids']}
        if self.mongo_db is not None:
            fb.completa_cards(self.conexao, self.mongo_db, cards)
        por_destino = defaultdict(Counter)
        for card in cards:
            contagem = por_destino[card['portoDestFinal']]
            contagem['cards'] += 1
            contagem['com ciclo'] += card['ciclo']
            contagem['ciclo com imagem'] += id(card) in tinham
            origem = (card.get('imagemFallback') or {}).get('origem')
            contagem['fallback pela inspeção'] += origem == fb.POR_INSPECAO
            contagem['fallback só imagem'] += origem == fb.SO_IMAGEM
            contagem['sem previsão'] += not card.get('dataPrevistaAtracacao')
            contagem['continua sem imagem'] += not card.get('imagens_ids')
            chave = 'fallback' if origem else ('sem imagem' if not card.get('imagens_ids') else None)
            grupo = self.exemplos[f'NAVI-I {chave} — {card["portoDestFinal"]}'] if chave else None
            if grupo is not None and len(grupo) < EXEMPLOS:
                detalhe = f' — {card["imagemFallback"]["detalhe"]}' if origem else ''
                grupo.append(f'CE {card["numeroCEmercante"]} {card["numeroConteiner"]} previsão '
                             f'{card.get("dataPrevistaAtracacao")}{detalhe}')
        colunas = ['cards', 'com ciclo', 'ciclo com imagem', 'fallback pela inspeção', 'fallback só imagem',
                   'continua sem imagem', 'sem previsão']
        total = Counter()
        linhas = []
        for destino, contagem in sorted(por_destino.items(), key=lambda kv: -kv[1]['cards']):
            total.update(contagem)
            linhas.append([destino] + [contagem['cards']] + [_pct(contagem[c], contagem['cards']) for c in colunas[1:]])
        linhas.append(['TODOS'] + [total['cards']] + [_pct(total[c], total['cards']) for c in colunas[1:]])
        self.tabela(['destino final'] + colunas, linhas[:20] + ([linhas[-1]] if len(linhas) > 20 else []))
        if self.mongo_db is None:
            self.escreve('(sem o Mongo: o fallback não foi medido)')

    # ---------------------------------------------------------------- 4
    def secao_defasagem(self):
        self.titulo('4. DEFASAGEM imagem (Mongo) − inspeção (API), em horas, por recinto fora de Santos')
        if self.mongo_db is None:
            self.escreve('(desligado: --sem-mongo)')
            return
        linhas = []
        for recinto, _ in [(r, n) for r, n in self.com_inspecao.most_common() if not self._de_santos(r)][:20]:
            inspecoes = self.consulta(
                'SELECT numeroConteiner, dataHoraOcorrencia FROM apirecintos_inspecoesnaoinvasivas '
                f'WHERE {PERIODO} AND codigoRecinto = :recinto AND {COM_CONTEINER} '  # nosec B608
                'AND (vazio IS NULL OR vazio = 0)', {'recinto': recinto})
            if not inspecoes:
                continue
            contagem = Counter()
            for cont, data in self.sorteio.sample(inspecoes, min(INSPECOES_POR_RECINTO, len(inspecoes))):
                data = _dt(data)
                imagens = fb.imagens_dos_conteineres(self.mongo_db, [_texto(cont)], data - timedelta(hours=6),
                                                     data + timedelta(hours=6)).get(_texto(cont), [])
                if not imagens:
                    contagem['sem imagem em ±6 h'] += 1
                    continue
                horas = min(((d - data).total_seconds() / 3600 for d, _ in imagens), key=abs)
                contagem[f'{round(horas):+d} h'] += 1
            total = sum(contagem.values())
            linhas.append([self._nome(recinto), total, ', '.join(f'{k}: {_pct(n, total)}'
                                                                 for k, n in contagem.most_common())])
        self.tabela(['recinto', 'inspeções', 'imagem mais próxima'], linhas)

    # ---------------------------------------------------------------- 5
    def secao_exemplos(self):
        self.titulo(f'5. EXEMPLOS (até {EXEMPLOS} por grupo)')
        for grupo, linhas in sorted(self.exemplos.items()):
            self.escreve()
            self.escreve(f'--- {grupo}')
            for linha in linhas:
                self.escreve(f'   {linha}')


def roda(diag: DiagnosticoOutrosPortos):
    diag.secao_unidades()
    diag.secao_eventos()
    diag.secao_embarques()
    diag.secao_navii()
    diag.secao_defasagem()
    diag.secao_exemplos()
    diag.resumo(6)


def main():  # pragma: no cover - depende do banco real
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    parser.add_argument('--dias', type=int, default=14, help='eventos dos últimos N dias (padrão 14)')
    parser.add_argument('--amostra-cards', type=int, default=1500, help='cards fora de Santos (padrão 1500)')
    parser.add_argument('--sem-mongo', action='store_true', help='sem as imagens (seções 3 sem o fallback e 4)')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    args = parser.parse_args()
    mongo_db = None
    if not args.sem_mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    engine = create_engine(uri_do_banco(args), pool_pre_ping=True)
    limita_tempo(engine, args.limite)
    agora = datetime.now()
    inicio = agora - timedelta(days=args.dias)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoOutrosPortos(conexao, saida, inicio, agora, mongo_db, args.amostra_cards, agora)
            diag.escreve(f'Diagnóstico do ciclo de vida fora de Santos — {agora:%Y-%m-%d %H:%M}; eventos de '
                         f'{inicio:%Y-%m-%d} a {agora:%Y-%m-%d}')
            roda(diag)
    finally:
        if args.saida:
            saida.close()
    if args.saida:
        print(f'Diagnóstico gravado em {args.saida}')


if __name__ == '__main__':  # pragma: no cover
    main()
