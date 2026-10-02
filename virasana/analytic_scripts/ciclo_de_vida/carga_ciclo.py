# coding: utf-8
"""
Carga, em lote e por contêiner, do que o núcleo do Ciclo de Vida v2 precisa: eventos da API Recintos
(desembarque/embarque, acessos efetivados, inspeções e pesagens), CEs do Mercante (itens, master,
tráfego, escalas, irmãos), contêiner vazio no manifesto e escopo de operadores. Somente leitura.

Usada pela sombra (`sombra_ciclo_de_vida.py`, que mede) e pelo serviço (`servico_ciclo_de_vida.py`,
que grava). O SQL é fixo neste módulo; os valores vão sempre como parâmetro e as listas, em lotes
(índices de numeroConteiner, numeroCEmercante, numeroCEMaster, manifesto).
"""
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set

from sqlalchemy import bindparam, text

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import JANELA_ESCALA_DIAS, CandidatoCE, normaliza_ce

UNIDADE_SANTOS = '0817800'
PORTO_SANTOS = 'BRSSZ'
MARGEM_EVENTOS_DIAS = 2
TAMANHO_LOTE = 500


# ---------------------------------------------------------------------- funções puras

def _dt(valor) -> Optional[datetime]:
    """Data/hora de uma linha de consulta: o MariaDB devolve datetime; o SQLite, texto."""
    if valor is None or isinstance(valor, datetime):
        return valor
    return datetime.fromisoformat(str(valor)[:19])


def _bool(valor) -> Optional[bool]:
    return None if valor is None else bool(valor)


def _texto(valor) -> Optional[str]:
    return str(valor).strip() or None if valor is not None else None


def _unidade(valor) -> str:
    """Código da unidade com 7 dígitos (o banco pode guardar como número: 817800)."""
    return str(valor or '').strip().zfill(7)


def _ano_do_ce(ce: str) -> Optional[int]:
    return 2000 + int(ce[2:4]) if ce and len(ce) == 15 and ce[2:4].isdigit() else None


def irmaos_de(proprios: List[CandidatoCE], irmaos_por_master: Dict[str, List[tuple]]) -> List[CandidatoCE]:
    """Filhotes dos masters da família que não têm o contêiner nos itens (só corroboram)."""
    conhecidos = {c.ce for c in proprios}
    masters = ({c.master for c in proprios if c.master} | conhecidos) & set(irmaos_por_master)
    return [CandidatoCE(ce, master, trafego, (), tem_conteiner=False)
            for master in sorted(masters) for ce, trafego in irmaos_por_master[master] if ce not in conhecidos]


def manifesto_vazio(escalas_por_manifesto: Dict[str, List[date]], desembarque: datetime,
                    janela_dias: int = JANELA_ESCALA_DIAS) -> Optional[str]:
    """Manifesto em que o contêiner consta VAZIO com a escala mais perto do desembarque (até a janela)."""
    distancias = {m: min(abs((dia - desembarque.date()).days) for dia in dias)
                  for m, dias in (escalas_por_manifesto or {}).items() if dias}
    perto = [m for m, d in distancias.items() if d <= janela_dias]
    return min(perto, key=lambda m: (distancias[m], m)) if perto else None


# ---------------------------------------------------------------------- execução de consultas

class ConsultaSQL:
    """Executa as consultas da carga numa conexão; erro de banco sobe (quem chama decide)."""

    def __init__(self, conexao):
        self.conexao = conexao
        self.tempos: List[tuple] = []

    def consulta(self, sql, params=None, listas=()) -> list:
        comando = text(sql)
        if listas:
            comando = comando.bindparams(*[bindparam(nome, expanding=True) for nome in listas])
        inicio = time.perf_counter()
        linhas = [tuple(linha) for linha in self.conexao.execute(comando, params or {})]
        self.tempos.append((' '.join(sql.split())[:70], time.perf_counter() - inicio))
        return linhas

    def _por_lotes(self, sql, nome, valores, params=None) -> list:
        linhas = []
        valores = sorted(v for v in set(valores) if v)
        for i in range(0, len(valores), TAMANHO_LOTE):
            linhas += self.consulta(sql, {**(params or {}), nome: valores[i:i + TAMANHO_LOTE]}, listas=(nome,))
        return linhas


# ---------------------------------------------------------------------- carga

class CargaCiclo:
    """Lê do banco, em lote e por contêiner, o que o núcleo precisa.

    `diag`: objeto com `consulta` e `_por_lotes` (`ConsultaSQL` ou o `Diagnostico` da sombra);
    `inicio`/`fim`: período dos desembarques — os eventos vêm desde `inicio` − 2 dias e os CEs, dos
    anos do período (dígitos 3–4 do número do CE).
    """

    def __init__(self, diag, inicio: datetime, fim: datetime):
        self.diag, self.inicio, self.fim = diag, inicio, fim
        self.desde = inicio - timedelta(days=MARGEM_EVENTOS_DIAS)
        self.estatisticas = Counter()
        self.info: Dict[str, dict] = {}   # CE -> master, tráfego, manifesto, porto de destino (Mercante)
        self.desembarques: Dict[int, dict] = {}   # id -> peso, tara, peso de manifesto, tipo, escala
        self.tipos: Dict[str, Counter] = defaultdict(Counter)   # contêiner -> tipos informados nos eventos

    def operadores(self):
        """(escopo Santos, todos os operadores, {código: nome})."""
        linhas = self.diag.consulta('SELECT cod_siscomex, nome, e_operador, cod_unidade FROM ovr_recintos '
                                    "WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''")
        escopo = {cod for cod, _, e_op, unidade in linhas if e_op and _unidade(unidade) == UNIDADE_SANTOS}
        todos = {cod for cod, _, e_op, _ in linhas if e_op}
        return escopo, todos, {cod: nome for cod, nome, _, _ in linhas}

    def eventos(self, conteineres: Iterable[str]):
        """({contêiner: [Evento]}, recintos com embarque/desembarque); guarda em `self.desembarques` os dados de
        cada desembarque/embarque e em `self.tipos` os tipos de contêiner informados."""
        eventos, portuarios = defaultdict(list), set()
        params = {'desde': self.desde}
        for id_, cont, data, recinto, tipo, ce, peso, tara, manifesto, tipo_cont, escala in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque, '
                'numeroConhecimento, pesoBrutoBalanca, taraConteiner, pesoBrutoManifesto, tipoConteiner, escala '
                'FROM apirecintos_embarquedesembarque WHERE numeroConteiner IN :conteineres '
                'AND dataHoraOcorrencia >= :desde', 'conteineres', conteineres, params):
            tipo_evento = ldt.EMBARQUE if tipo == 'E' else ldt.DESEMBARQUE
            eventos[cont].append(ldt.Evento(tipo_evento, id_, _dt(data), recinto, None, normaliza_ce(ce)))
            self.desembarques[id_] = {'peso': peso, 'tara': tara, 'manifesto': manifesto,
                                      'tipo': _texto(tipo_cont), 'escala': _texto(escala)}
            self._tipo(cont, tipo_cont)
            portuarios.add(recinto)
        for id_, cont, data, recinto, direcao, vazio, ce, tipo_cont in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, direcao, vazioConteiner, '
                'numeroConhecimento, tipoConteiner FROM apirecintos_acessosveiculo '
                "WHERE numeroConteiner IN :conteineres AND operacao = 'C' AND dataHoraOcorrencia >= :desde",
                'conteineres', conteineres, params):
            tipo_evento = ldt.ENTRADA if direcao == 'E' else ldt.SAIDA
            eventos[cont].append(ldt.Evento(tipo_evento, id_, _dt(data), recinto, _bool(vazio), normaliza_ce(ce)))
            self._tipo(cont, tipo_cont)
        for id_, cont, data, recinto, vazio, ce, tipo_cont in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, vazio, numeroConhecimento, '
                'tipoConteiner FROM apirecintos_inspecoesnaoinvasivas WHERE numeroConteiner IN :conteineres '
                'AND dataHoraOcorrencia >= :desde', 'conteineres', conteineres, params):
            eventos[cont].append(ldt.Evento(ldt.INSPECAO, id_, _dt(data), recinto, _bool(vazio), normaliza_ce(ce)))
            self._tipo(cont, tipo_cont)
        for id_, cont, data, recinto in self.diag._por_lotes(
                'SELECT id, numeroConteiner, dataHoraOcorrencia, codigoRecinto FROM apirecintos_pesagensveiculo '
                'WHERE numeroConteiner IN :conteineres AND dataHoraOcorrencia >= :desde', 'conteineres',
                conteineres, params):
            eventos[cont].append(ldt.Evento(ldt.PESAGEM, id_, _dt(data), recinto))
            self.estatisticas['pesagens'] += 1
        return eventos, portuarios

    def _tipo(self, conteiner: str, tipo):
        if _texto(tipo):
            self.tipos[conteiner][_texto(tipo)] += 1

    def tipo_do(self, conteiner: str, desembarque_id: int) -> Optional[str]:
        """Tipo informado no desembarque; sem ele, o mais informado nos outros eventos do contêiner."""
        tipo = (self.desembarques.get(desembarque_id) or {}).get('tipo')
        if tipo:
            return tipo
        return self.tipos[conteiner].most_common(1)[0][0] if self.tipos.get(conteiner) else None

    def manifestos_das_escalas(self, escalas) -> Dict[str, Set[str]]:
        """{escala: manifestos} (escalamanifestoresumo) — para conferir a escala declarada no desembarque."""
        resultado = defaultdict(set)
        for escala, manifesto in self.diag._por_lotes(
                'SELECT escala, manifesto FROM escalamanifestoresumo WHERE escala IN :escalas', 'escalas', escalas):
            resultado[_texto(escala)].add(_texto(manifesto))
        return resultado

    def candidatos(self, conteineres: Iterable[str]) -> Dict[str, List[CandidatoCE]]:
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
        """Carrega master, tráfego, manifesto e porto de destino dos CEs que ainda não estão em `self.info`."""
        for ce, master, trafego, manifesto, destino in self.diag._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster, tipoTrafego, manifestoCE, portoDestFinal '
                'FROM conhecimentosresumo WHERE numeroCEmercante IN :ces', 'ces',
                {ce for ce in ces if ce and ce not in self.info}):
            self.info[normaliza_ce(ce)] = {'master': normaliza_ce(master), 'trafego': _texto(trafego),
                                           'manifesto': _texto(manifesto), 'porto_destino': _texto(destino)}

    def conhecidos(self, eventos: Iterable[ldt.Evento]) -> Set[str]:
        """CEs declarados nos eventos que existem no Mercante (os outros não desconfiam da ligação)."""
        declarados = {e.ce_declarado for e in eventos if e.ce_declarado}
        self.info_de(declarados)
        self.estatisticas['CEs declarados nos eventos'] = len(declarados)
        self.estatisticas['declarados fora do Mercante'] = len(declarados - set(self.info))
        return set(self.info)

    def _irmaos(self, masters) -> Dict[str, List[tuple]]:
        """{master: [(filhote, tráfego)]} — todos os filhotes dos masters (índice numeroCEMaster)."""
        por_master = defaultdict(list)
        for ce, master, trafego in self.diag._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster, tipoTrafego FROM conhecimentosresumo '
                'WHERE numeroCEMaster IN :masters', 'masters', {m for m in masters if m}):
            por_master[normaliza_ce(master)].append((normaliza_ce(ce), _texto(trafego)))
        return por_master

    def _ces_dos_itens(self, conteineres: Iterable[str]) -> Dict[str, Set[str]]:
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

    def vazios(self, conteineres: Iterable[str]) -> Dict[str, Dict[str, List[date]]]:
        """{contêiner: {manifesto: datas de escala}} dos manifestos em que o contêiner consta VAZIO."""
        manifestos = defaultdict(set)
        for manifesto, cont in self.diag._por_lotes(
                'SELECT manifesto, idConteinerVazio FROM conteinervazioresumo WHERE idConteinerVazio IN :conteineres',
                'conteineres', conteineres):
            manifestos[_texto(cont)].add(_texto(manifesto))
        escalas = self._escalas({m for lista in manifestos.values() for m in lista if m})
        return {cont: {m: escalas.get(m, []) for m in lista if m} for cont, lista in manifestos.items()}

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
