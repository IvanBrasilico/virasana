# coding: utf-8
"""
MODO SOMBRA da heurística v2 do Ciclo de Vida (etapa 3 do plano — CICLO_DE_VIDA.md).

Roda o núcleo novo (`heuristica_ciclo.processa_conteiner`) sobre os contêineres desembarcados em
Santos num período e COMPARA com os ciclos atuais (gravados pelo `manager_ciclo_de_vida.py`).
**Somente leitura: não grava nada no banco.** Não traz placa, CPF nem nome de motorista.

Escopo (decisão do usuário): operadores de Santos = `ovr_recintos.e_operador` e `cod_unidade =
'0817800'` (Alfândega de Santos). "Operadores" para a linha do tempo = todos os `e_operador` mais os
recintos que registram embarque/desembarque nos eventos carregados (são terminais portuários).

Amostra: contêineres ESPALHADOS pelo período (ordem pseudoaleatória fixa pelo id, reprodutível) — a
primeira rodada (01/10) pegou só os mais recentes, que o loop antigo ainda não tinha criado (atraso).
O período termina, por padrão, 7 dias antes de hoje (--ignorar-ultimos-dias) pelo mesmo motivo.

Relatório:
  0. parâmetros e operadores do escopo;
  1. volume: desembarques por dia no escopo x ciclos atuais, atraso do loop antigo, ciclos novos com e
     sem ciclo atual por operador e por idade (ex.: Ecoporto);
  2. situação nova x atual e motivos;
  3. etapas (saída, entrada e saída do seco), só ciclos com ciclo atual: iguais, novas, removidas...;
     onde estava a entrada no seco do ciclo atual quando a nova a remove/troca;
  4. escaneamentos (só com ciclo atual): ganhos/perdidos; confiança/motivos das ligações novas;
  5. CEs: atual x principal novo (igual, mesma família, novo, perdido, diferente) e confiança; por que
     o CE atual foi perdido/trocado; eventos com CE declarado de OUTRA família: relação e tráfego;
  6. saídas tardias (> 72 h, além da janela antiga) e ciclos incompletos (fronteiras);
  7. alertas mais frequentes;
  8. contêiner vazio: manifesto do Mercante (conteinervazioresumo) x peso x tara x CE;
  9. imagens no Mongo (opcional, --mongo): defasagem imagem - inspeção POR RECINTO (janela ±8 h);
 10. amostras das divergências.

Família do CE: os CEs com o contêiner nos itens, o master e os IRMÃOS (filhotes do mesmo master, mesmo
sem o contêiner nos itens) — a 2ª rodada mostrou inspeções no cais declarando um CE "de outra família".

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
    JANELA_ESCALA_DIAS, TRAFEGO_EXPORTACAO, CandidatoCE, CEsResolvidos, familia, normaliza_ce,
)
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ces_ciclo_de_vida import relacao  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico, _dt  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.heuristica_ciclo import processa_conteiner  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt  # noqa: E402

UNIDADE_SANTOS = '0817800'
PORTO_SANTOS = 'BRSSZ'
HORAS_JANELA_ANTIGA = 72
JANELA_MONGO_HORAS = 8      # a 1ª sombra mostrou que o +3 h não vale para todos: medir por recinto
RESIDUO_MONGO_MINUTOS = 10
MARGEM_EVENTOS_DIAS = 2
DIAS_ATRASO_LOOP = 7        # o loop antigo cria os ciclos com dias de atraso (medido: ~5)
SIM, NAO = 'sim', 'não'
TRAFEGOS = {'3': 'cabotagem', '5': 'importação', '7': 'exportação', '9': 'passagem'}
OUTRA_FAMILIA = 'CE declarado de OUTRA família'

COLUNAS_CSV = ['conteiner', 'desembarque_id', 'data_desembarque', 'operador', 'ciclo_atual_id', 'situacao_atual',
               'situacao_nova', 'fluxo_novo', 'motivo_novo', 'saida', 'entrada_seco', 'saida_seco',
               'escaneamentos', 'escaneamentos_ganhos', 'escaneamentos_perdidos', 'ce_atual', 'ce_novo',
               'ces_familia', 'ce_comparacao', 'ce_confianca', 'alertas', 'idade_dias', 'vazio_mercante',
               'peso_balanca', 'peso_manifesto', 'ce_atual_motivo', 'recinto_seco_atual', 'recinto_seco_novo']


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
        self.info: Dict[str, dict] = {}   # CE -> master, tráfego, manifesto (Mercante)

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
            # ordem pseudoaleatória reprodutível (hash multiplicativo do id): espalha a amostra pelo período
            'SELECT numeroConteiner, (MAX(id) * 2654435761) % 4294967291 AS ordem '
            "FROM apirecintos_embarquedesembarque WHERE embarqueDesembarque = 'D' AND codigoRecinto IN :escopo "
            'AND dataHoraOcorrencia >= :inicio AND dataHoraOcorrencia < :fim AND numeroConteiner IS NOT NULL '
            "AND numeroConteiner <> '' GROUP BY numeroConteiner ORDER BY ordem LIMIT :limite",
            {'escopo': sorted(escopo), 'limite': limite}, listas=('escopo',))]

    def desembarques_por_dia(self, escopo: Set[str]) -> List[tuple]:
        """(dia, desembarques no escopo, com ciclo atual) — todo o período, não só a amostra."""
        if not escopo:
            return []
        return self.diag.consulta(
            'SELECT DATE(d.dataHoraOcorrencia) AS dia, COUNT(*), COUNT(c.ID) FROM apirecintos_embarquedesembarque d '
            'LEFT JOIN ciclodevida_ciclo_de_vida_importacao c ON c.desembarque_id = d.id '
            "WHERE d.embarqueDesembarque = 'D' AND d.codigoRecinto IN :escopo AND d.dataHoraOcorrencia >= :inicio "
            'AND d.dataHoraOcorrencia < :fim GROUP BY DATE(d.dataHoraOcorrencia) ORDER BY dia',
            {'escopo': sorted(escopo)}, listas=('escopo',))

    def ultimo_desembarque_com_ciclo(self) -> Optional[datetime]:
        linhas = self.diag.consulta(
            'SELECT d.dataHoraOcorrencia FROM apirecintos_embarquedesembarque d WHERE d.id = '
            '(SELECT MAX(desembarque_id) FROM ciclodevida_ciclo_de_vida_importacao)')
        return _dt(linhas[0][0]) if linhas else None

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
        """CEs do Mercante cujo Item tem o contêiner (só de anos recentes), com master, tráfego e escalas,
        mais os irmãos (filhotes do mesmo master) sem o contêiner nos itens (`tem_conteiner=False`)."""
        por_conteiner = self._ces_dos_itens(conteineres)
        ces = {ce for lista in por_conteiner.values() for ce in lista}
        self.info_de(ces)
        escalas = self._escalas({self.info[ce]['manifesto'] for ce in ces if ce in self.info} - {None})
        irmaos = self._irmaos({self.info[ce]['master'] for ce in ces if ce in self.info} | ces)
        resultado = {}
        for cont, lista in por_conteiner.items():
            proprios = [CandidatoCE(ce, self.info[ce]['master'], self.info[ce]['trafego'],
                                    tuple(escalas.get(self.info[ce]['manifesto'], ())))
                        for ce in sorted(lista) if ce in self.info]
            resultado[cont] = proprios + irmaos_de(proprios, irmaos)
        self.estatisticas['irmãos sem o contêiner (família)'] = sum(
            1 for lista in resultado.values() for c in lista if not c.tem_conteiner)
        return resultado

    def info_de(self, ces):
        """Carrega master, tráfego e manifesto dos CEs que ainda não estão em `self.info`."""
        for ce, master, trafego, manifesto in self.diag._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster, tipoTrafego, manifestoCE FROM conhecimentosresumo '
                'WHERE numeroCEmercante IN :ces', 'ces', {ce for ce in ces if ce and ce not in self.info}):
            self.info[normaliza_ce(ce)] = {'master': normaliza_ce(master), 'trafego': _texto(trafego),
                                           'manifesto': _texto(manifesto)}

    def _irmaos(self, masters) -> Dict[str, List[tuple]]:
        """{master: [(filhote, tráfego)]} — todos os filhotes dos masters (índice numeroCEMaster)."""
        por_master = defaultdict(list)
        for ce, master, trafego in self.diag._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster, tipoTrafego FROM conhecimentosresumo '
                'WHERE numeroCEMaster IN :masters', 'masters', {m for m in masters if m}):
            por_master[normaliza_ce(master)].append((normaliza_ce(ce), _texto(trafego)))
        return por_master

    def itens_de(self, ces) -> Set[tuple]:
        """{(CE, contêiner)} dos itens dos CEs (sem filtro de ano)."""
        return {(normaliza_ce(ce), _texto(cont)) for ce, cont in self.diag._por_lotes(
            'SELECT numeroCEmercante, codigoConteiner FROM itensresumo WHERE numeroCEmercante IN :ces', 'ces', ces)}

    def escalas_dos_ces(self, ces) -> Dict[str, List[date]]:
        """{CE: datas de escala do manifesto} (CEs já em `self.info`)."""
        escalas = self._escalas({self.info[ce]['manifesto'] for ce in ces if ce in self.info} - {None})
        return {ce: escalas.get(self.info[ce]['manifesto'], []) for ce in ces if ce in self.info}

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

    def vazios(self, conteineres: List[str]) -> Dict[str, List[date]]:
        """Datas de escala dos manifestos em que o contêiner consta como VAZIO (conteinervazioresumo)."""
        manifestos = defaultdict(set)
        for manifesto, cont in self.diag._por_lotes(
                'SELECT manifesto, idConteinerVazio FROM conteinervazioresumo WHERE idConteinerVazio IN :conteineres',
                'conteineres', conteineres):
            manifestos[_texto(cont)].add(_texto(manifesto))
        escalas = self._escalas({m for lista in manifestos.values() for m in lista if m})
        return {cont: [dia for m in lista for dia in escalas.get(m, ())] for cont, lista in manifestos.items()}

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
        self.por_dia: List[tuple] = []
        self.ultimo_com_ciclo: Optional[datetime] = None
        self.atuais: Dict[int, dict] = {}
        self.acessos: Dict[int, ldt.Evento] = {}
        self.inspecoes: Dict[int, ldt.Evento] = {}
        self.conflitos = Counter()

    def executa(self):
        carregador = Carregador(self, self.periodo['inicio'], self.periodo['fim'])
        escopo, operadores, self.nomes = carregador.operadores()
        self.titulo('0. PARÂMETROS E OPERADORES DO ESCOPO')
        self.escreve(f'Período dos desembarques: {self.periodo["inicio"]:%Y-%m-%d} a {self.periodo["fim"]:%Y-%m-%d}; '
                     f'agora = {self.agora:%Y-%m-%d %H:%M}; até {self.limite_conteineres} contêineres; '
                     f'parâmetros: {ldt.Parametros()}')
        self.tabela(['operador do escopo (unidade 0817800)', 'nome'], [(c, self.nomes.get(c)) for c in sorted(escopo)])
        self.por_dia = carregador.desembarques_por_dia(escopo)
        self.ultimo_com_ciclo = carregador.ultimo_desembarque_com_ciclo()
        conteineres = carregador.conteineres(escopo, self.limite_conteineres)
        eventos, self.pesos, portuarios = carregador.eventos(conteineres)
        todos = [e for lista in eventos.values() for e in lista]
        self.acessos = {e.id: e for e in todos if e.tipo in (ldt.ENTRADA, ldt.SAIDA)}
        self.inspecoes = {e.id: e for e in todos if e.tipo == ldt.INSPECAO}
        candidatos = carregador.candidatos(conteineres)
        vazios = carregador.vazios(conteineres)
        operadores |= portuarios
        self.escreve(f'Operadores para a linha do tempo: {len(operadores)} (e_operador + recintos com embarque/'
                     f'desembarque nos eventos). CEs do Mercante: {dict(carregador.estatisticas)}')
        resultados = [(conteiner, ciclo, ces) for conteiner in conteineres
                      for ciclo, ces in processa_conteiner(eventos[conteiner], candidatos.get(conteiner, []), escopo,
                                                           operadores, self.agora)
                      if self.periodo['inicio'] <= ciclo.desembarque.data < self.periodo['fim']]
        self.atuais = carregador.ciclos_atuais({c.desembarque.id for _, c, _ in resultados})
        for conteiner, ciclo, ces in resultados:
            self.ciclos[ciclo.desembarque.id] = ciclo
            self.linhas.append(self._linha(conteiner, ciclo, ces, candidatos.get(conteiner, []),
                                           vazios.get(conteiner, ())))
        self._explica_ces(carregador)
        self.escreve(f'Contêineres: {len(conteineres)}; ciclos novos no período: {len(self.linhas)}; '
                     f'com ciclo atual: {sum(1 for lin in self.linhas if lin["ciclo_atual_id"])}')

    def _linha(self, conteiner, ciclo, ces, candidatos, vazios) -> dict:
        atual = self.atuais.get(ciclo.desembarque.id)
        linha = compara(conteiner, ciclo, ces, candidatos, atual)
        peso, tara, manifesto = self.pesos.get(ciclo.desembarque.id, (None, None, None))
        entrada_atual = self.acessos.get((atual or {}).get('entrada_seco_id'))
        linha.update({'idade_dias': (self.agora - ciclo.desembarque.data).days,
                      'vazio_mercante': vazio_no_manifesto(vazios, ciclo.desembarque.data),
                      'peso_balanca': _faixa_peso(peso, tara), 'peso_manifesto': _faixa_peso(manifesto, tara),
                      'ce_atual_motivo': '',
                      'recinto_seco_atual': entrada_atual.recinto if entrada_atual else
                      ('fora dos eventos carregados' if (atual or {}).get('entrada_seco_id') else '-'),
                      'recinto_seco_novo': ciclo.entrada_seco.evento.recinto if ciclo.entrada_seco else '-'})
        return linha

    def _explica_ces(self, carregador: 'Carregador'):
        """Por que o CE atual foi perdido/trocado; relação dos CEs declarados de OUTRA família."""
        divergentes = [lin for lin in self.linhas if lin['ce_comparacao'] in ('perdido', 'diferente')]
        conflitos = [(lig.evento.tipo, lig.evento.ce_declarado, ciclo) for ciclo in self.ciclos.values()
                     for lig in ciclo.ligacoes() if OUTRA_FAMILIA in lig.motivos]
        carregador.info_de({lin['ce_atual'] for lin in divergentes} | {ce for _, ce, _ in conflitos})
        ces_atuais = {lin['ce_atual'] for lin in divergentes}
        itens, escalas = carregador.itens_de(ces_atuais), carregador.escalas_dos_ces(ces_atuais)
        anos = set(range(self.periodo['inicio'].year - 1, self.periodo['fim'].year + 1))
        for linha in divergentes:
            linha['ce_atual_motivo'] = motivo_ce_atual(linha['ce_atual'], linha['conteiner'], carregador.info, itens,
                                                       escalas, linha['data_desembarque'], anos)
        principal = {lin['desembarque_id']: lin['ce_novo'] for lin in self.linhas}
        for tipo, declarado, ciclo in conflitos:
            dados = carregador.info.get(declarado) or {}
            self.conflitos[(tipo, relacao(principal[ciclo.desembarque.id], declarado, carregador.info),
                            TRAFEGOS.get(dados.get('trafego'), dados.get('trafego') or '-'))] += 1

    # ---------------------------------------------------------------- seções
    def _conta(self, *campos, filtro=None, extra=None) -> list:
        """Contagem das linhas por `campos` (`extra`: campos calculados {nome: função da linha})."""
        def valor(lin, campo):
            return extra[campo](lin) if extra and campo in extra else lin[campo]
        contagem = Counter(tuple(valor(lin, c) for c in campos) for lin in self.linhas if not filtro or filtro(lin))
        return [(*chave, n) for chave, n in contagem.most_common()]

    def secao_volume(self):
        self.titulo('1. VOLUME E ATRASO DO LOOP ANTIGO')
        atraso = (f'{(self.agora - self.ultimo_com_ciclo).total_seconds() / 86400:.1f} dias'
                  if self.ultimo_com_ciclo else '-')
        self.escreve(f'Último desembarque com ciclo atual: {self.ultimo_com_ciclo or "-"} (atraso: {atraso}); o loop '
                     f'antigo cria no máximo 2.000 ciclos por volta.')
        self.escreve()
        self.tabela(['dia (todo o período, escopo)', 'desembarques', 'com ciclo atual'], self.por_dia)
        self.escreve()
        self.escreve('Amostra:')
        self.tabela(['operador', 'nome', 'ciclos novos', 'com ciclo atual', 'sem ciclo atual'], [
            (op, self.nomes.get(op), n, n - sem, sem) for op, n, sem in self._por_operador()])
        self.escreve()
        self.tabela(['sem ciclo atual: idade do desembarque', 'vazio no Mercante', 'ciclos'], self._conta(
            'idade', 'vazio_mercante', filtro=lambda lin: not lin['ciclo_atual_id'],
            extra={'idade': lambda lin: _faixa_idade(lin['idade_dias'])}))

    def _por_operador(self):
        for op, n in Counter(lin['operador'] for lin in self.linhas).most_common():
            yield op, n, sum(1 for lin in self.linhas if lin['operador'] == op and not lin['ciclo_atual_id'])

    def secao_situacao(self):
        self.titulo('2. SITUAÇÃO ATUAL x NOVA')
        self.tabela(['situação atual', 'situação nova', 'fluxo novo', 'ciclos'],
                    self._conta('situacao_atual', 'situacao_nova', 'fluxo_novo'))
        self.escreve()
        self.tabela(['situação nova', 'motivo', 'ciclos'], self._conta('situacao_nova', 'motivo_novo'))

    def secao_etapas(self):
        self.titulo('3. ETAPAS — atual x nova (só ciclos que têm ciclo atual)')
        for etapa in ('saida', 'entrada_seco', 'saida_seco'):
            self.escreve()
            self.tabela([etapa, 'ciclos'], self._conta(etapa, filtro=_com_atual))
        self.escreve()
        self.escreve('Entrada no seco removida/diferente: onde estava a do ciclo atual (o loop antigo trata como seco '
                     'todo recinto fora da sua lista de 3 operadores, inclusive o Ecoporto):')
        self.tabela(['entrada_seco', 'recinto do ciclo atual', 'nome', 'recinto novo', 'ciclos'], [
            (comp, atual, self.nomes.get(atual, ''), novo, n) for comp, atual, novo, n in self._conta(
                'entrada_seco', 'recinto_seco_atual', 'recinto_seco_novo',
                filtro=lambda lin: lin['entrada_seco'] in ('removida', 'diferente'))[:15]])

    def secao_escaneamentos(self):
        self.titulo('4. ESCANEAMENTOS')
        com_atual = [lin for lin in self.linhas if _com_atual(lin)]
        self.tabela(['escaneamentos (atual x novo, sem os de confiança baixa; só com ciclo atual)', 'ciclos'],
                    self._conta('escaneamentos', filtro=_com_atual))
        self.escreve(f'Ligações ganhas: {sum(lin["escaneamentos_ganhos"] for lin in com_atual)}; '
                     f'perdidas: {sum(lin["escaneamentos_perdidos"] for lin in com_atual)}')
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
        self.tabela(['tem ciclo atual', 'CE atual x novo', 'confiança do CE novo', 'ciclos'], self._conta(
            'tem_atual', 'ce_comparacao', 'ce_confianca', extra={'tem_atual': lambda lin: _sim(_com_atual(lin))}))
        self.escreve()
        self.tabela(['CE atual perdido/trocado', 'por quê (CE atual no Mercante)', 'ciclos'], self._conta(
            'ce_comparacao', 'ce_atual_motivo', filtro=lambda lin: lin['ce_comparacao'] in ('perdido', 'diferente')))
        self.escreve()
        self.escreve('Eventos com CE declarado de OUTRA família (confiança baixa): relação com o CE principal do '
                     'ciclo e tráfego do CE declarado:')
        self.tabela(['evento', 'relação com o CE do ciclo', 'tráfego do declarado', 'eventos'],
                    [(*chave, n) for chave, n in self.conflitos.most_common()])

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
        self.titulo('8. CONTÊINER VAZIO — manifesto do Mercante x peso x tara x CE')
        self.escreve(f'"vazio no Mercante": o contêiner consta como vazio (conteinervazioresumo) num manifesto com '
                     f'escala a até {JANELA_ESCALA_DIAS} dias do desembarque.')
        tem_ce = {'ce': lambda lin: _sim(bool(lin['ce_novo']))}
        self.tabela(['vazio no Mercante', 'CE resolvido', 'ciclos'], self._conta('vazio_mercante', 'ce', extra=tem_ce))
        self.escreve()
        self.tabela(['vazio no Mercante', 'balança: peso - tara', 'ciclos'],
                    self._conta('vazio_mercante', 'peso_balanca'))
        self.escreve()
        self.tabela(['vazio no Mercante', 'manifesto: peso - tara', 'ciclos'],
                    self._conta('vazio_mercante', 'peso_manifesto'))
        self.escreve()
        self.tabela(['vazio no Mercante', 'situação nova', 'fluxo novo', 'ciclos'],
                    self._conta('vazio_mercante', 'situacao_nova', 'fluxo_novo'))

    def secao_mongo(self, db, limite=600):
        self.titulo(f'9. IMAGENS NO MONGO — defasagem imagem - inspeção por recinto (janela ±{JANELA_MONGO_HORAS} h, '
                    f'até {limite} inspeções)')
        if db is None:
            self.escreve('(desligado: use --mongo)')
            return
        conteiner_do = {lin['desembarque_id']: lin['conteiner'] for lin in self.linhas}
        inspecoes = [(conteiner_do[c.desembarque.id], lig.evento) for c in self.ciclos.values()
                     for lig in c.escaneamentos][:limite]
        contagem, exatas = Counter(), Counter()
        for conteiner, inspecao in inspecoes:
            chave, exata = defasagem_mongo(db, conteiner, inspecao.data)
            contagem[(inspecao.recinto, chave)] += 1
            exatas[(inspecao.recinto, chave)] += exata
        self.escreve(f'Imagem mais próxima da inspeção (metadata.numeroinformado + dataescaneamento), em horas '
                     f'inteiras; "resíduo <= {RESIDUO_MONGO_MINUTOS} min" = diferença quase exata nessas horas.')
        self.tabela(['recinto', 'nome', 'imagem - inspeção', 'inspeções', f'resíduo <= {RESIDUO_MONGO_MINUTOS} min'], [
            (recinto, self.nomes.get(recinto), chave, n, exatas[(recinto, chave)])
            for (recinto, chave), n in sorted(contagem.items(), key=lambda kv: (kv[0][0], -kv[1]))])

    def secao_amostras_divergencias(self):
        self.titulo(f'10. AMOSTRAS DAS DIVERGÊNCIAS ({self.amostras} por tipo)')
        tipos = [
            ('pendente hoje, concluído na nova', lambda lin: lin['situacao_atual'].startswith('pendente')
             and lin['situacao_nova'] != ldt.ABERTO),
            ('concluído hoje, aberto na nova', lambda lin: lin['situacao_atual'].startswith('concluído')
             and lin['situacao_nova'] == ldt.ABERTO),
            ('entrada no seco removida/diferente', lambda lin: lin['entrada_seco'] in ('removida', 'diferente')),
            ('CE diferente (fora da família)', lambda lin: lin['ce_comparacao'] == 'diferente'),
            ('CE atual perdido', lambda lin: lin['ce_comparacao'] == 'perdido'),
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
                     f"{linha['ce_novo'] or '-'} ({linha['ce_comparacao']}"
                     + (f": {linha['ce_atual_motivo']})" if linha['ce_atual_motivo'] else ')'))
        self._mostra_atual(linha)
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

    def _mostra_atual(self, linha: dict):
        """Etapas do ciclo atual que a nova não tem (entrada no seco, escaneamentos perdidos)."""
        atual = self.atuais.get(linha['desembarque_id'])
        if not atual:
            return
        if linha['entrada_seco'] in ('removida', 'diferente'):
            entrada = self.acessos.get(atual['entrada_seco_id'])
            self.escreve(f'   [atual] entrada seco: {entrada.data} {entrada.recinto}' if entrada else
                         f'   [atual] entrada seco: id {atual["entrada_seco_id"]} (fora dos eventos carregados)')
        novos = {lig.evento.id for lig in self.ciclos[linha['desembarque_id']].escaneamentos
                 if lig.confianca != ldt.BAIXA}
        for esc_id in sorted(set(atual['escaneamentos']) - novos):
            inspecao = self.inspecoes.get(esc_id)
            self.escreve(f'   [atual] inspeção perdida: {inspecao.data} {inspecao.recinto} CE {inspecao.ce_declarado}'
                         if inspecao else f'   [atual] inspeção perdida: id {esc_id} (fora dos eventos carregados)')

    def grava_csv(self, caminho: str):
        with open(caminho, 'w', newline='', encoding='utf-8-sig') as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=COLUNAS_CSV, delimiter=';')
            escritor.writeheader()
            escritor.writerows(self.linhas)


def vazio_no_manifesto(datas_escala, desembarque: datetime, janela_dias: int = JANELA_ESCALA_DIAS) -> str:
    """'sim' se o contêiner consta vazio num manifesto com escala perto do desembarque."""
    perto = any(abs((dia - desembarque.date()).days) <= janela_dias for dia in datas_escala)
    return _sim(perto)


def irmaos_de(proprios: List[CandidatoCE], irmaos_por_master: Dict[str, List[tuple]]) -> List[CandidatoCE]:
    """Filhotes dos masters da família que não têm o contêiner nos itens (só corroboram)."""
    conhecidos = {c.ce for c in proprios}
    masters = ({c.master for c in proprios if c.master} | conhecidos) & set(irmaos_por_master)
    return [CandidatoCE(ce, master, trafego, (), tem_conteiner=False)
            for master in sorted(masters) for ce, trafego in irmaos_por_master[master] if ce not in conhecidos]


def motivo_ce_atual(ce: str, conteiner: str, info: Dict[str, dict], itens: Set[tuple],
                    escalas: Dict[str, List[date]], desembarque: datetime, anos: Set[int]) -> str:
    """Por que o CE do ciclo atual não foi o escolhido pela heurística nova."""
    dados = info.get(ce)
    if not dados:
        return 'CE atual fora do Mercante'
    if dados['trafego'] == TRAFEGO_EXPORTACAO:
        return 'CE atual é de exportação'
    if (ce, conteiner) not in itens:
        return 'contêiner não está nos itens do CE atual'
    if _ano_do_ce(ce) not in anos:
        return 'CE atual de outro ano'
    datas = escalas.get(ce)
    if not datas:
        return 'manifesto do CE atual sem escala'
    dias = min(abs((dia - desembarque.date()).days) for dia in datas)
    if dias <= JANELA_ESCALA_DIAS:
        return 'outra família escolhida (as duas com escala próxima)'
    return 'escala do CE atual a ' + ('16 a 30 dias' if dias <= 30 else '31 a 90 dias' if dias <= 90 else '> 90 dias')


def defasagem_mongo(db, conteiner: str, data_inspecao: datetime):
    """('+3 h' / '+0 h' / 'nenhuma imagem', resíduo pequeno?) da imagem mais próxima da inspeção."""
    janela = timedelta(hours=JANELA_MONGO_HORAS)
    documentos = db['fs.files'].find(
        {'metadata.numeroinformado': conteiner,
         'metadata.dataescaneamento': {'$gte': data_inspecao - janela, '$lte': data_inspecao + janela}},
        {'metadata.dataescaneamento': 1})
    minutos = [(d['metadata']['dataescaneamento'].replace(tzinfo=None) - data_inspecao).total_seconds() / 60
               for d in documentos]
    if not minutos:
        return f'nenhuma imagem em ±{JANELA_MONGO_HORAS} h', False
    mais_proxima = min(minutos, key=abs)
    horas = round(mais_proxima / 60)
    return f'{horas:+d} h', abs(mais_proxima - 60 * horas) <= RESIDUO_MONGO_MINUTOS


def _com_atual(linha: dict) -> bool:
    return bool(linha['ciclo_atual_id'])


def _sim(valor: bool) -> str:
    return SIM if valor else NAO


def _faixa_idade(dias: int) -> str:
    for teto, rotulo in ((DIAS_ATRASO_LOOP, f'< {DIAS_ATRASO_LOOP} dias (atraso do loop)'), (30, '7 a 30 dias'),
                         (93, '30 a 93 dias')):
        if dias < teto:
            return rotulo
    return '>= 93 dias'


def _faixa_peso(peso, tara) -> str:
    """Peso menos tara, em faixas; separa peso ausente/zerado (não medido) de peso <= tara."""
    if peso is None:
        return 'sem peso'
    if float(peso) == 0:
        return 'peso zero'
    if not tara:
        return 'sem tara'
    liquido = float(peso) - float(tara)
    if liquido <= 0:
        return 'peso <= tara'
    if liquido < 500:
        return '0 a 500 kg'
    return '500 a 2.000 kg' if liquido < 2000 else '> 2.000 kg'


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
    parser.add_argument('--dias', type=int, default=30, help='período: N dias de desembarques (padrão 30)')
    parser.add_argument('--ignorar-ultimos-dias', type=int, default=DIAS_ATRASO_LOOP,
                        help=f'o período termina N dias antes de hoje (atraso do loop; padrão {DIAS_ATRASO_LOOP})')
    parser.add_argument('--conteineres', type=int, default=3000,
                        help='contêineres avaliados (amostra espalhada pelo período, reprodutível)')
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
    fim = datetime.now() - timedelta(days=args.ignorar_ultimos_dias)
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
