# coding: utf-8
"""
MODO SOMBRA da heurística v2 do Ciclo de Vida (etapa 3 do plano — CICLO_DE_VIDA.md).

Roda o núcleo novo (`heuristica_ciclo.processa_conteiner`) sobre os contêineres desembarcados em
Santos num período e COMPARA com os ciclos atuais (gravados pelo `manager_ciclo_de_vida.py`).
**Somente leitura: não grava nada no banco.** Não traz placa, CPF nem nome de motorista.

Escopo (decisão do usuário): operadores de Santos = `ovr_recintos.e_operador` e `cod_unidade =
'0817800'` (Alfândega de Santos). "Operadores" para a linha do tempo = todos os `e_operador` mais os
recintos que registram embarque/desembarque nos eventos carregados (são terminais portuários).

Relatório:
  0. parâmetros e operadores do escopo;
  1. volume: contêineres, ciclos novos x atuais, desembarques sem ciclo atual (ex.: Ecoporto);
  2. situação nova x atual e motivos;
  3. etapas (saída, entrada e saída do seco): iguais, novas, removidas, diferentes;
  4. escaneamentos: ganhos/perdidos e confiança/motivos das ligações novas;
  5. CEs: atual x principal novo (igual, mesma família, novo, perdido, diferente) e confiança;
  6. saídas tardias (> 72 h, além da janela antiga) e ciclos incompletos (fronteiras);
  7. alertas mais frequentes;
  8. peso x tara no desembarque (serve para saber se veio cheio?);
  9. imagens no Mongo (opcional, --mongo): defasagem depois de corrigir o fuso (+3 h);
 10. amostras das divergências.

CE Mercante: os dígitos 3–4 do número são o ano de emissão (ex.: 1526... = 2026) — usados só para
não carregar CEs de anos antigos do mesmo contêiner (o relatório mostra quantos foram descartados).

Uso (na raiz do repositório virasana):
    python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py
    python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py --dias 30 --conteineres 3000
    python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py --mongo --csv sombra.csv
"""
import argparse
import csv
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import (  # noqa: E402
    CandidatoCE, CEsResolvidos, familia, normaliza_ce,
)
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import processa_conteiner  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402

UNIDADE_SANTOS = '0817800'
PORTO_SANTOS = 'BRSSZ'
HORAS_JANELA_ANTIGA = 72
FUSO_MONGO_HORAS = 3        # Mongo em UTC, API em hora local (medido: +3 h em 291 de 300)
MARGEM_EVENTOS_DIAS = 2

COLUNAS_CSV = ['conteiner', 'desembarque_id', 'data_desembarque', 'operador', 'ciclo_atual_id', 'situacao_atual',
               'situacao_nova', 'fluxo_novo', 'motivo_novo', 'saida', 'entrada_seco', 'saida_seco',
               'escaneamentos', 'escaneamentos_ganhos', 'escaneamentos_perdidos', 'ce_atual', 'ce_novo',
               'ces_familia', 'ce_comparacao', 'ce_confianca', 'alertas']


# ---------------------------------------------------------------------- comparação (pura)

def compara_id(id_atual, ligacao: Optional[ldt.Ligacao]) -> str:
    """Etapa atual x nova: igual, nova, removida, diferente ou nenhuma."""
    id_novo = ligacao.evento.id if ligacao else None
    if id_atual == id_novo:
        return 'igual' if id_novo else 'nenhuma'
    if id_atual is None:
        return 'nova'
    return 'removida' if id_novo is None else 'diferente'


def situacao_atual(atual: Optional[dict]) -> str:
    if atual is None:
        return 'sem ciclo atual'
    if atual['saida_seco_id']:
        etapa = 'saiu do seco'
    elif atual['entrada_seco_id']:
        etapa = 'no seco'
    elif atual['saida_id']:
        etapa = 'saiu do operador'
    else:
        etapa = 'sem saída'
    return ('concluído' if atual['concluido'] else 'pendente') + f' ({etapa})'


def compara_ce(ce_atual: Optional[str], ces: CEsResolvidos, familia_nova: Set[str]) -> str:
    if not ce_atual:
        return 'novo' if ces.principal else 'nenhum'
    if not ces.principal:
        return 'perdido'
    if ce_atual == ces.principal:
        return 'igual'
    return 'mesma família' if ce_atual in familia_nova else 'diferente'


def compara(conteiner: str, ciclo: ldt.CicloCalculado, ces: CEsResolvidos, candidatos: List[CandidatoCE],
            atual: Optional[dict]) -> dict:
    """Linha de comparação de um ciclo (também é a linha do CSV)."""
    dados = atual or {}
    atuais = set(dados.get('escaneamentos', ()))
    novos = {lig.evento.id for lig in ciclo.escaneamentos if lig.confianca != ldt.BAIXA}
    return {
        'conteiner': conteiner, 'desembarque_id': ciclo.desembarque.id, 'data_desembarque': ciclo.desembarque.data,
        'operador': ciclo.desembarque.recinto, 'ciclo_atual_id': dados.get('id'),
        'situacao_atual': situacao_atual(atual), 'situacao_nova': ciclo.situacao, 'fluxo_novo': ciclo.fluxo,
        'motivo_novo': ciclo.motivo,
        'saida': compara_id(dados.get('saida_id'), ciclo.saida),
        'entrada_seco': compara_id(dados.get('entrada_seco_id'), ciclo.entrada_seco),
        'saida_seco': compara_id(dados.get('saida_seco_id'), ciclo.saida_seco),
        'escaneamentos': _compara_conjuntos(atuais, novos),
        'escaneamentos_ganhos': len(novos - atuais), 'escaneamentos_perdidos': len(atuais - novos),
        'ce_atual': dados.get('ce'), 'ce_novo': ces.principal, 'ces_familia': ' '.join(sorted(ces.ces)),
        'ce_comparacao': compara_ce(dados.get('ce'), ces, familia(ces, candidatos)),
        'ce_confianca': ces.confianca or '-',
        'alertas': '; '.join(ciclo.alertas),
    }


def _compara_conjuntos(atuais: set, novos: set) -> str:
    if atuais == novos:
        return 'iguais' if novos else 'nenhum'
    if atuais < novos:
        return 'ganhou'
    if novos < atuais:
        return 'perdeu'
    return 'mudou'


# ---------------------------------------------------------------------- carregamento (só leitura)

def _bool(valor) -> Optional[bool]:
    return None if valor is None else bool(valor)


def _texto(valor) -> Optional[str]:
    return str(valor).strip() or None if valor is not None else None


def _unidade(valor) -> str:
    """Código da unidade com 7 dígitos (o banco pode guardar como número: 817800)."""
    return str(valor or '').strip().zfill(7)


def _ano_do_ce(ce: str) -> Optional[int]:
    return 2000 + int(ce[2:4]) if ce and len(ce) == 15 and ce[2:4].isdigit() else None


class Carregador:
    """Lê do banco, em lote e por contêiner (índices de numeroConteiner), o que o núcleo precisa."""

    def __init__(self, diag: Diagnostico, inicio: datetime, fim: datetime):
        self.diag, self.inicio, self.fim = diag, inicio, fim
        self.desde = inicio - timedelta(days=MARGEM_EVENTOS_DIAS)
        self.estatisticas = Counter()

    def operadores(self):
        """(escopo Santos, todos os operadores, {código: nome})."""
        linhas = self.diag.consulta('SELECT cod_siscomex, nome, e_operador, cod_unidade FROM ovr_recintos '
                                    "WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''")
        escopo = {cod for cod, _, e_op, unidade in linhas if e_op and _unidade(unidade) == UNIDADE_SANTOS}
        todos = {cod for cod, _, e_op, _ in linhas if e_op}
        return escopo, todos, {cod: nome for cod, nome, _, _ in linhas}

    def conteineres(self, escopo: Set[str], limite: int) -> List[str]:
        if not escopo:
            return []
        return [linha[0] for linha in self.diag.consulta(
            'SELECT numeroConteiner, MAX(id) FROM apirecintos_embarquedesembarque '
            "WHERE embarqueDesembarque = 'D' AND codigoRecinto IN :escopo AND dataHoraOcorrencia >= :inicio "
            "AND dataHoraOcorrencia < :fim AND numeroConteiner IS NOT NULL AND numeroConteiner <> '' "
            'GROUP BY numeroConteiner ORDER BY 2 DESC LIMIT :limite',
            {'escopo': sorted(escopo), 'limite': limite}, listas=('escopo',))]

    def eventos(self, conteineres: List[str]):
        """{contêiner: [Evento]}, {desembarque_id: (peso, tara, peso manifesto)} e recintos com embarque."""
        eventos, pesos, portuarios = defaultdict(list), {}, set()
        params = {'desde': self.desde}
        for id_, cont, data, recinto, tipo, ce, peso, tara, manifesto in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque, '
                'numeroConhecimento, pesoBrutoBalanca, taraConteiner, pesoBrutoManifesto '
                'FROM apirecintos_embarquedesembarque WHERE numeroConteiner IN :conteineres '
                'AND dataHoraOcorrencia >= :desde', 'conteineres', conteineres, params):
            tipo_evento = ldt.EMBARQUE if tipo == 'E' else ldt.DESEMBARQUE
            eventos[cont].append(ldt.Evento(tipo_evento, id_, _dt(data), recinto, None, normaliza_ce(ce)))
            pesos[id_] = (peso, tara, manifesto)
            portuarios.add(recinto)
        for id_, cont, data, recinto, direcao, vazio, ce in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, direcao, vazioConteiner, '
                'numeroConhecimento FROM apirecintos_acessosveiculo WHERE numeroConteiner IN :conteineres '
                "AND operacao = 'C' AND dataHoraOcorrencia >= :desde", 'conteineres', conteineres, params):
            tipo_evento = ldt.ENTRADA if direcao == 'E' else ldt.SAIDA
            eventos[cont].append(ldt.Evento(tipo_evento, id_, _dt(data), recinto, _bool(vazio), normaliza_ce(ce)))
        for id_, cont, data, recinto, vazio, ce in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, vazio, numeroConhecimento '
                'FROM apirecintos_inspecoesnaoinvasivas WHERE numeroConteiner IN :conteineres '
                'AND dataHoraOcorrencia >= :desde', 'conteineres', conteineres, params):
            eventos[cont].append(ldt.Evento(ldt.INSPECAO, id_, _dt(data), recinto, _bool(vazio), normaliza_ce(ce)))
        return eventos, pesos, portuarios

    def candidatos(self, conteineres: List[str]) -> Dict[str, List[CandidatoCE]]:
        """CEs do Mercante cujo Item tem o contêiner (só de anos recentes), com master, tráfego e escalas."""
        por_conteiner = self._ces_dos_itens(conteineres)
        ces = {ce for lista in por_conteiner.values() for ce in lista}
        info = {normaliza_ce(ce): (normaliza_ce(master), _texto(trafego), _texto(manifesto))
                for ce, master, trafego, manifesto in self.diag._por_lotes(
                    'SELECT numeroCEmercante, numeroCEMaster, tipoTrafego, manifestoCE FROM conhecimentosresumo '
                    'WHERE numeroCEmercante IN :ces', 'ces', ces)}
        escalas = self._escalas({m for _, _, m in info.values() if m})
        resultado = defaultdict(list)
        for cont, lista in por_conteiner.items():
            for ce in sorted(c for c in lista if c in info):
                master, trafego, manifesto = info[ce]
                resultado[cont].append(CandidatoCE(ce, master, trafego, tuple(escalas.get(manifesto, ()))))
        return resultado

    def _ces_dos_itens(self, conteineres: List[str]) -> Dict[str, Set[str]]:
        anos = set(range(self.inicio.year - 1, self.fim.year + 1))
        por_conteiner = defaultdict(set)
        for ce, cont in self.diag._por_lotes(
                'SELECT numeroCEmercante, codigoConteiner FROM itensresumo WHERE codigoConteiner IN :conteineres',
                'conteineres', conteineres):
            ce = normaliza_ce(ce)
            self.estatisticas['itens com o contêiner (todos os anos)'] += 1
            if _ano_do_ce(ce) in anos:
                por_conteiner[_texto(cont)].add(ce)
        self.estatisticas['CEs dos anos do período'] = len({ce for lista in por_conteiner.values() for ce in lista})
        return por_conteiner

    def _escalas(self, manifestos) -> Dict[str, List[date]]:
        """Datas previstas de atracação por manifesto: as de Santos; sem escala em Santos, todas."""
        santos, todas = defaultdict(list), defaultdict(list)
        for manifesto, data, porto in self.diag._por_lotes(
                'SELECT em.manifesto, e.dataPrevistaAtracacao, e.portoDaEscala FROM escalamanifestoresumo em '
                'JOIN escalasresumo e ON e.numeroDaEscala = em.escala WHERE em.manifesto IN :manifestos',
                'manifestos', manifestos):
            try:
                dia = datetime.strptime(str(data)[:10], '%Y-%m-%d').date()
            except ValueError:
                continue
            todas[manifesto].append(dia)
            if porto == PORTO_SANTOS:
                santos[manifesto].append(dia)
        return {m: santos.get(m) or todas[m] for m in todas}

    def ciclos_atuais(self, desembarques: Set[int]) -> Dict[int, dict]:
        atuais = {}
        for id_, desembarque_id, concluido, saida, entrada, saida_seco, ce in self.diag._por_lotes(
                'SELECT ID, desembarque_id, concluido, saida_id, entrada_seco_id, saida_seco_id, numeroCEmercante '
                'FROM ciclodevida_ciclo_de_vida_importacao WHERE desembarque_id IN :ids', 'ids', desembarques):
            atuais[desembarque_id] = {'id': id_, 'concluido': bool(concluido), 'saida_id': saida,
                                      'entrada_seco_id': entrada, 'saida_seco_id': saida_seco,
                                      'ce': (ce or '').strip() or None, 'escaneamentos': []}
        por_id = {a['id']: a for a in atuais.values()}
        for ciclo_id, escaneamento_id in self.diag._por_lotes(
                'SELECT ciclo_de_vida_id, escaneamento_id FROM ciclodevida_ciclo_de_vida_importacao_escaneamentos '
                'WHERE ciclo_de_vida_id IN :ids', 'ids', set(por_id)):
            por_id[ciclo_id]['escaneamentos'].append(escaneamento_id)
        return atuais


# ---------------------------------------------------------------------- relatório

class Sombra(Diagnostico):
    """Executa a sombra e escreve o relatório (reaproveita a saída do diagnóstico do ciclo de vida)."""

    def __init__(self, conexao, saida, inicio, fim, conteineres=3000, amostras=5, agora=None):
        super().__init__(conexao, saida, inicio, fim, agora=agora, amostras=amostras)
        self.limite_conteineres = conteineres
        self.linhas: List[dict] = []
        self.ciclos: Dict[int, ldt.CicloCalculado] = {}
        self.pesos: Dict[int, tuple] = {}
        self.nomes: Dict[str, str] = {}

    def executa(self):
        carregador = Carregador(self, self.periodo['inicio'], self.periodo['fim'])
        escopo, operadores, self.nomes = carregador.operadores()
        self.titulo('0. PARÂMETROS E OPERADORES DO ESCOPO')
        self.escreve(f'Período dos desembarques: {self.periodo["inicio"]:%Y-%m-%d} a {self.periodo["fim"]:%Y-%m-%d}; '
                     f'agora = {self.agora:%Y-%m-%d %H:%M}; até {self.limite_conteineres} contêineres; '
                     f'parâmetros: {ldt.Parametros()}')
        self.tabela(['operador do escopo (unidade 0817800)', 'nome'], [(c, self.nomes.get(c)) for c in sorted(escopo)])
        conteineres = carregador.conteineres(escopo, self.limite_conteineres)
        eventos, self.pesos, portuarios = carregador.eventos(conteineres)
        candidatos = carregador.candidatos(conteineres)
        operadores |= portuarios
        self.escreve(f'Operadores para a linha do tempo: {len(operadores)} (e_operador + recintos com embarque/'
                     f'desembarque nos eventos). CEs do Mercante: {dict(carregador.estatisticas)}')
        resultados = []
        for conteiner in conteineres:
            for ciclo, ces in processa_conteiner(eventos[conteiner], candidatos.get(conteiner, []), escopo,
                                                 operadores, self.agora):
                if self.periodo['inicio'] <= ciclo.desembarque.data < self.periodo['fim']:
                    resultados.append((conteiner, ciclo, ces))
        atuais = carregador.ciclos_atuais({c.desembarque.id for _, c, _ in resultados})
        for conteiner, ciclo, ces in resultados:
            self.ciclos[ciclo.desembarque.id] = ciclo
            self.linhas.append(compara(conteiner, ciclo, ces, candidatos.get(conteiner, []),
                                       atuais.get(ciclo.desembarque.id)))
        self.escreve(f'Contêineres: {len(conteineres)}; ciclos novos no período: {len(self.linhas)}; '
                     f'com ciclo atual: {sum(1 for lin in self.linhas if lin["ciclo_atual_id"])}')

    # ---------------------------------------------------------------- seções
    def _conta(self, *campos, filtro=None) -> list:
        contagem = Counter(tuple(lin[c] for c in campos) for lin in self.linhas if not filtro or filtro(lin))
        return [(*chave, n) for chave, n in contagem.most_common()]

    def secao_volume(self):
        self.titulo('1. VOLUME — ciclos novos sem ciclo atual, por operador')
        self.tabela(['operador', 'nome', 'ciclos novos', 'sem ciclo atual'], [
            (op, self.nomes.get(op), n, sum(1 for lin in self.linhas
                                            if lin['operador'] == op and not lin['ciclo_atual_id']))
            for op, n in Counter(lin['operador'] for lin in self.linhas).most_common()])

    def secao_situacao(self):
        self.titulo('2. SITUAÇÃO ATUAL x NOVA')
        self.tabela(['situação atual', 'situação nova', 'fluxo novo', 'ciclos'],
                    self._conta('situacao_atual', 'situacao_nova', 'fluxo_novo'))
        self.escreve()
        self.tabela(['situação nova', 'motivo', 'ciclos'], self._conta('situacao_nova', 'motivo_novo'))

    def secao_etapas(self):
        self.titulo('3. ETAPAS — atual x nova')
        for etapa in ('saida', 'entrada_seco', 'saida_seco'):
            self.escreve()
            self.tabela([etapa, 'ciclos'], self._conta(etapa))

    def secao_escaneamentos(self):
        self.titulo('4. ESCANEAMENTOS')
        self.tabela(['escaneamentos (atual x novo, sem os de confiança baixa)', 'ciclos'], self._conta('escaneamentos'))
        self.escreve(f'Ligações ganhas: {sum(lin["escaneamentos_ganhos"] for lin in self.linhas)}; '
                     f'perdidas: {sum(lin["escaneamentos_perdidos"] for lin in self.linhas)}')
        confianca, motivos = Counter(), Counter()
        for ciclo in self.ciclos.values():
            for lig in ciclo.escaneamentos:
                confianca[lig.confianca] += 1
                motivos[(lig.confianca, ' / '.join(lig.motivos) or '-')] += 1
        self.escreve()
        self.tabela(['confiança da ligação', 'inspeções'], confianca.most_common())
        self.tabela(['confiança', 'motivos', 'inspeções'], [(*k, n) for k, n in motivos.most_common(20)])

    def secao_ces(self):
        self.titulo('5. CEs — atual x principal novo (filhote principal; família master/filhote)')
        self.tabela(['CE atual x novo', 'confiança do CE novo', 'ciclos'], self._conta('ce_comparacao', 'ce_confianca'))

    def secao_saidas(self):
        self.titulo('6. SAÍDAS TARDIAS E FRONTEIRAS')
        tardias = Counter()
        for ciclo in self.ciclos.values():
            if ciclo.saida and ldt._horas(ciclo.saida.evento.data, ciclo.desembarque.data) > HORAS_JANELA_ANTIGA:
                tardias[ciclo.saida.confianca] += 1
        self.tabela([f'saída do operador > {HORAS_JANELA_ANTIGA} h após o desembarque (confiança)', 'ciclos'],
                    tardias.most_common())
        self.escreve()
        self.tabela(['ciclos incompletos (fronteira)', 'ciclos'],
                    self._conta('motivo_novo', filtro=lambda lin: lin['situacao_nova'] == ldt.INCOMPLETO))

    def secao_alertas(self):
        self.titulo('7. ALERTAS MAIS FREQUENTES')
        contagem = Counter(alerta.split(' — ')[0] for ciclo in self.ciclos.values() for alerta in ciclo.alertas)
        self.tabela(['alerta', 'ciclos'], contagem.most_common(20))

    def secao_pesos(self):
        self.titulo('8. PESO x TARA NO DESEMBARQUE (dá para saber se veio cheio?)')
        faixas = Counter()
        for ciclo in self.ciclos.values():
            peso, tara, manifesto = self.pesos.get(ciclo.desembarque.id, (None, None, None))
            faixas[(_faixa_peso(peso, tara), 'com peso de manifesto' if manifesto else 'sem peso de manifesto')] += 1
        self.tabela(['peso bruto - tara', 'manifesto', 'desembarques'], [(*k, n) for k, n in faixas.most_common()])

    def secao_mongo(self, db, limite=300):
        self.titulo(f'9. IMAGENS NO MONGO — defasagem após corrigir o fuso (+{FUSO_MONGO_HORAS} h), até {limite}')
        if db is None:
            self.escreve('(desligado: use --mongo)')
            return
        inspecoes = [(c.desembarque, lig.evento) for c in self.ciclos.values() for lig in c.escaneamentos][:limite]
        conteiner_do = {lin['desembarque_id']: lin['conteiner'] for lin in self.linhas}
        contagem = Counter()
        for desembarque, inspecao in inspecoes:
            esperado = inspecao.data + timedelta(hours=FUSO_MONGO_HORAS)
            documentos = db['fs.files'].find(
                {'metadata.numeroinformado': conteiner_do[desembarque.id],
                 'metadata.dataescaneamento': {'$gte': esperado - timedelta(hours=6),
                                               '$lte': esperado + timedelta(hours=6)}},
                {'metadata.dataescaneamento': 1})
            deltas = [abs((d['metadata']['dataescaneamento'].replace(tzinfo=None) - esperado).total_seconds()) / 60
                      for d in documentos]
            contagem[_faixa_minutos(min(deltas) if deltas else None)] += 1
        self.tabela(['imagem mais próxima de inspeção + 3 h', 'inspeções'], contagem.most_common())

    def secao_amostras_divergencias(self):
        self.titulo(f'10. AMOSTRAS DAS DIVERGÊNCIAS ({self.amostras} por tipo)')
        tipos = [
            ('pendente hoje, concluído na nova', lambda lin: lin['situacao_atual'].startswith('pendente')
             and lin['situacao_nova'] != ldt.ABERTO),
            ('concluído hoje, aberto na nova', lambda lin: lin['situacao_atual'].startswith('concluído')
             and lin['situacao_nova'] == ldt.ABERTO),
            ('entrada no seco removida/diferente', lambda lin: lin['entrada_seco'] in ('removida', 'diferente')),
            ('CE diferente (fora da família)', lambda lin: lin['ce_comparacao'] == 'diferente'),
            ('escaneamento perdido', lambda lin: lin['escaneamentos_perdidos'] > 0),
        ]
        for rotulo, filtro in tipos:
            escolhidas = [lin for lin in self.linhas if filtro(lin)][:self.amostras]
            self.escreve()
            self.escreve(f'--- {rotulo}: {len(escolhidas)} amostra(s)')
            for linha in escolhidas:
                self._mostra(linha)

    def _mostra(self, linha: dict):
        ciclo = self.ciclos[linha['desembarque_id']]
        self.escreve(f"{linha['conteiner']} desembarque {ciclo.desembarque.data} em {ciclo.desembarque.recinto}; "
                     f"ciclo atual {linha['ciclo_atual_id'] or '-'}: {linha['situacao_atual']}; "
                     f"nova: {ciclo.situacao}/{ciclo.fluxo} — {ciclo.motivo}; CE {linha['ce_atual'] or '-'} -> "
                     f"{linha['ce_novo'] or '-'} ({linha['ce_comparacao']})")
        for nome, lig in (('saída', ciclo.saida), ('entrada seco', ciclo.entrada_seco),
                          ('saída seco', ciclo.saida_seco), ('embarque', ciclo.embarque)):
            if lig:
                self.escreve(f'   {nome}: {lig.evento.data} {lig.evento.recinto} [{lig.confianca}] '
                             f'{"; ".join(lig.motivos)}')
        for lig in ciclo.escaneamentos:
            self.escreve(f'   inspeção: {lig.evento.data} {lig.evento.recinto} [{lig.confianca}] '
                         f'{"; ".join(lig.motivos)}')
        if ciclo.alertas:
            self.escreve(f'   alertas: {"; ".join(ciclo.alertas)}')

    def grava_csv(self, caminho: str):
        with open(caminho, 'w', newline='', encoding='utf-8-sig') as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=COLUNAS_CSV, delimiter=';')
            escritor.writeheader()
            escritor.writerows(self.linhas)


def _faixa_peso(peso, tara) -> str:
    if peso is None or tara is None:
        return 'sem peso ou tara'
    liquido = float(peso) - float(tara)
    if liquido <= 0:
        return '<= 0 (vazio?)'
    if liquido < 500:
        return '0 a 500 kg'
    return '500 a 2.000 kg' if liquido < 2000 else '> 2.000 kg'


def _faixa_minutos(minutos: Optional[float]) -> str:
    if minutos is None:
        return 'nenhuma imagem em ±6 h'
    for teto, rotulo in ((5, 'até 5 min'), (30, '5 a 30 min'), (120, '30 min a 2 h')):
        if minutos <= teto:
            return rotulo
    return '2 a 6 h'


SECOES = ('secao_volume', 'secao_situacao', 'secao_etapas', 'secao_escaneamentos', 'secao_ces', 'secao_saidas',
          'secao_alertas', 'secao_pesos')


def roda(sombra: Sombra, mongo_db=None) -> List[str]:
    sombra.executa()
    falhas = []
    for nome in SECOES + ('secao_mongo', 'secao_amostras_divergencias'):
        inicio = time.perf_counter()
        try:
            getattr(sombra, nome)(mongo_db) if nome == 'secao_mongo' else getattr(sombra, nome)()
        except Exception as err:  # noqa: uma seção com problema não impede as outras
            sombra.escreve(f'!! ERRO na seção {nome}: {type(err).__name__}: {err}')
            falhas.append(nome)
        sombra.escreve(f'   [seção em {time.perf_counter() - inicio:.1f} s]')
        sombra.saida.flush()
    sombra.resumo(11)
    return falhas


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--dias', type=int, default=30, help='período: desembarques dos últimos N dias (padrão 30)')
    parser.add_argument('--conteineres', type=int, default=3000, help='contêineres avaliados (os mais recentes)')
    parser.add_argument('--amostras', type=int, default=5, help='amostras por tipo de divergência')
    parser.add_argument('--mongo', action='store_true', help='inclui a seção das imagens (MONGODB_URI do ajna_commons)')
    parser.add_argument('--csv', help='grava uma linha por ciclo com a comparação (separador ;)')
    parser.add_argument('--saida', default=datetime.now().strftime('ciclo_de_vida_sombra_%Y%m%d_%H%M%S.log'))
    args = parser.parse_args()
    uri = args.uri
    if not uri:
        from ajna_commons.flask.conf import SQL_URI as uri
    mongo_db = None
    if args.mongo:
        from pymongo import MongoClient
        from ajna_commons.flask.conf import DATABASE, MONGODB_URI
        mongo_db = MongoClient(host=MONGODB_URI)[DATABASE]
    fim = datetime.now()
    inicio = fim - timedelta(days=args.dias)
    engine = create_engine(uri)
    with engine.connect() as conexao, open(args.saida, 'w', encoding='utf-8') as saida:
        sombra = Sombra(conexao, saida, inicio, fim, args.conteineres, args.amostras)
        sombra.escreve(f'Modo sombra da heurística v2 do Ciclo de Vida — {datetime.now():%Y-%m-%d %H:%M:%S} '
                       f'(somente leitura)')
        sombra.escreve(f'Banco: {engine.url.render_as_string(hide_password=True)}')
        falhas = roda(sombra, mongo_db)
        if args.csv:
            sombra.grava_csv(args.csv)
    print(f'Relatório gravado em {args.saida}' + (f'; CSV em {args.csv}' if args.csv else '')
          + (f' (seções com erro: {", ".join(falhas)})' if falhas else ''))


if __name__ == '__main__':  # pragma: no cover
    main()
