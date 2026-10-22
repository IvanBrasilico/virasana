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
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import bindparam, text

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt
from virasana.analytic_scripts.ciclo_de_vida import pesagens_ciclo as pc
from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import (
    JANELA_ESCALA_DIAS, CandidatoCE, CEsResolvidos, datas_no_porto, normaliza_ce,
)

UNIDADE_SANTOS = '0817800'
PORTO_SANTOS = 'BRSSZ'
# Ampliação do escopo (usuário, 20/10/2026): fora de Santos a base só recebe os OPERADORES PORTUÁRIOS e quase não há
# recinto seco — o ciclo "simplificado" (só a estada no operador) cobre quase tudo. Lista EXPLÍCITA, medida antes na
# sombra (`--operadores`; CICLO_DE_VIDA.md, "Fora de Santos e pesagens"). Só entra quem também é `e_operador` no
# cadastro. Fora: Rio ICTSI 7921302 (acesso sem contêiner) e Itaguaí 7961304 (sem acesso nem pesagem).
OPERADORES_FORA_DE_SANTOS = frozenset({
    '7921303',      # MultiRio
    '4931303',      # Tecon Suape
    '3111401',      # Porto de Pecém
})
# Operadores que informam embarque e desembarque TROCADOS (D3, 20/10/2026: no Rio ICTSI 7921302 nenhum dos 4.465 "D"
# em 14 dias tem evento depois no recinto e os "E" têm inspeção/pesagem depois; a escala do "D" confere com o manifesto
# do CE em 17% — MultiRio, 96%). A carga destroca. VAZIO até a ativação: a sombra mede com `--d-e-trocados`. Quando o
# operador corrigir a origem, o histórico continua trocado — aí será preciso uma data de corte.
OPERADORES_D_E_TROCADOS = frozenset()
MARGEM_EVENTOS_DIAS = 2
TAMANHO_LOTE = 500
COLUNAS_PESAGEM = ('id, numeroConteiner, dataHoraOcorrencia, codigoRecinto, placa, placaSemirreboque, '
                   'pesoBrutoBalanca, taraConjunto, capturaAutoPeso')


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


def tipo_movimento(tipo, recinto, trocados=OPERADORES_D_E_TROCADOS) -> str:
    """ldt.EMBARQUE ou ldt.DESEMBARQUE do `embarqueDesembarque` informado — destrocado nos operadores `trocados`."""
    return ldt.EMBARQUE if (_texto(tipo) == 'E') != (recinto in trocados) else ldt.DESEMBARQUE


def filtro_desembarques(escopo, trocados=OPERADORES_D_E_TROCADOS, prefixo: str = '') -> Tuple[str, dict, tuple]:
    """(SQL, parâmetros, listas) dos DESEMBARQUES nos operadores do `escopo`: 'D' — e 'E' nos de D/E trocados. Sem
    operador trocado no escopo, o filtro de sempre. `prefixo`: o da tabela na consulta (ex.: 'd.')."""
    normais, invertidos = sorted(set(escopo) - set(trocados)), sorted(set(escopo) & set(trocados))
    tipo, recinto = f'{prefixo}embarqueDesembarque', f'{prefixo}codigoRecinto'
    if not invertidos:
        return f"{tipo} = 'D' AND {recinto} IN :escopo", {'escopo': normais}, ('escopo',)
    return (f"(({tipo} = 'D' AND {recinto} IN :escopo) OR ({tipo} = 'E' AND {recinto} IN :trocados))",
            {'escopo': normais, 'trocados': invertidos}, ('escopo', 'trocados'))


def _numero(valor) -> Optional[float]:
    try:
        return float(valor) if valor is not None else None
    except (TypeError, ValueError):
        return None


def pesagem_de(linha) -> dict:
    """Uma linha de COLUNAS_PESAGEM como o dict das regras de `pesagens_ciclo`."""
    id_, conteiner, data, recinto, placa, semirreboque, peso, tara_conjunto, captura = linha
    return {'id': id_, 'conteiner': _texto(conteiner) or '', 'data': _dt(data), 'recinto': _texto(recinto),
            'placa': (_texto(placa) or '').upper(), 'semirreboque': (_texto(semirreboque) or '').upper(),
            'peso': _numero(peso), 'tara_conjunto': _numero(tara_conjunto),
            'captura': None if captura is None else bool(captura)}


def _dia(valor) -> Optional[date]:
    """Data 'AAAA-MM-DD' (texto do Mercante) ou None."""
    try:
        return datetime.strptime(str(valor)[:10], '%Y-%m-%d').date()
    except ValueError:
        return None


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
    `trocados`: operadores com embarque/desembarque trocados (a sombra troca por instância).
    """
    trocados = OPERADORES_D_E_TROCADOS

    def __init__(self, diag, inicio: datetime, fim: datetime):
        self.diag, self.inicio, self.fim = diag, inicio, fim
        self.desde = inicio - timedelta(days=MARGEM_EVENTOS_DIAS)
        self.estatisticas = Counter()
        self.info: Dict[str, dict] = {}   # CE -> master, tráfego, manifesto, porto de destino (Mercante)
        self.desembarques: Dict[int, dict] = {}   # id -> peso, tara, peso de manifesto, tipo, escala
        self.tipos: Dict[str, Counter] = defaultdict(Counter)   # contêiner -> tipos informados nos eventos
        self.pesagens: Dict[str, List[dict]] = defaultdict(list)   # contêiner -> pesagens (pesagem_de)
        self.itens: Dict[tuple, tuple] = {}   # (CE, contêiner) -> (pesoBruto, tara, isoCode) do Mercante

    def operadores(self):
        """(escopo — operadores de Santos e os de OPERADORES_FORA_DE_SANTOS —, todos os operadores, {código: nome})."""
        linhas = self.diag.consulta('SELECT cod_siscomex, nome, e_operador, cod_unidade FROM ovr_recintos '
                                    "WHERE cod_siscomex IS NOT NULL AND cod_siscomex <> ''")
        escopo = {cod for cod, _, e_op, unidade in linhas
                  if e_op and (_unidade(unidade) == UNIDADE_SANTOS or cod in OPERADORES_FORA_DE_SANTOS)}
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
            tipo_evento = tipo_movimento(tipo, recinto, self.trocados)
            eventos[cont].append(ldt.Evento(tipo_evento, id_, _dt(data), recinto, None, normaliza_ce(ce)))
            self.desembarques[id_] = {'peso': peso, 'tara': tara, 'manifesto': manifesto,
                                      'tipo': _texto(tipo_cont), 'escala': _texto(escala), 'recinto': recinto}
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
        for linha in self.diag._por_lotes(
                f'SELECT {COLUNAS_PESAGEM} FROM apirecintos_pesagensveiculo '  # nosec B608 - colunas fixas
                'WHERE numeroConteiner IN :conteineres AND dataHoraOcorrencia >= :desde', 'conteineres',
                conteineres, params):
            pesagem = pesagem_de(linha)
            eventos[linha[1]].append(ldt.Evento(ldt.PESAGEM, pesagem['id'], pesagem['data'], linha[3]))
            self.pesagens[pesagem['conteiner']].append(pesagem)
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

    def portos_dos_desembarques(self) -> Dict[int, Optional[str]]:
        """{id do desembarque/embarque carregado: porto} — o da escala declarada no evento (escalasresumo); sem ela,
        o porto mais visto nas escalas dos outros eventos do mesmo recinto no lote. Medição de 22/10/2026
        (`diagnostico_porto_desembarque.py`, 145 mil eventos, 16 recintos): o porto da escala declarada é 100%
        constante por recinto; o `ovr_recintos.cod_carga` está vazio em todos."""
        porto_da_escala = {_texto(escala): _texto(porto) for escala, porto in self.diag._por_lotes(
            'SELECT numeroDaEscala, portoDaEscala FROM escalasresumo WHERE numeroDaEscala IN :escalas', 'escalas',
            {d['escala'] for d in self.desembarques.values() if d['escala']})}
        por_recinto: Dict[str, Counter] = defaultdict(Counter)
        for dados in self.desembarques.values():
            if porto_da_escala.get(dados['escala']):
                por_recinto[dados['recinto']][porto_da_escala[dados['escala']]] += 1
        do_recinto = {recinto: contagem.most_common(1)[0][0] for recinto, contagem in por_recinto.items()}
        return {id_: porto_da_escala.get(dados['escala']) or do_recinto.get(dados['recinto'])
                for id_, dados in self.desembarques.items()}

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
        escalas = self.escalas_com_porto({self.info[ce]['manifesto'] for ce in ces if ce in self.info} - {None})
        irmaos = self._irmaos({self.info[ce]['master'] for ce in ces if ce in self.info} | ces)
        resultado = {}
        for cont, lista in por_conteiner.items():
            proprios = [self._candidato(ce, escalas.get(self.info[ce]['manifesto'], []))
                        for ce in sorted(lista) if ce in self.info]
            resultado[cont] = proprios + irmaos_de(proprios, irmaos)
        self.estatisticas['irmãos sem o contêiner (família)'] = sum(
            1 for lista in resultado.values() for c in lista if not c.tem_conteiner)
        return resultado

    def _candidato(self, ce: str, escalas: List[Tuple[date, str]]) -> CandidatoCE:
        info = self.info[ce]
        return CandidatoCE(ce, info['master'], info['trafego'], tuple(datas_no_porto(escalas)), escalas=tuple(escalas),
                           origem=info['origem'], destino=info['porto_destino'])

    def info_de(self, ces):
        """Carrega master, tráfego, manifesto e portos de origem e de destino dos CEs que ainda não estão em
        `self.info`."""
        for ce, master, trafego, manifesto, destino, origem in self.diag._por_lotes(
                'SELECT numeroCEmercante, numeroCEMaster, tipoTrafego, manifestoCE, portoDestFinal, portoOrigemCarga '
                'FROM conhecimentosresumo WHERE numeroCEmercante IN :ces', 'ces',
                {ce for ce in ces if ce and ce not in self.info}):
            self.info[normaliza_ce(ce)] = {'master': normaliza_ce(master), 'trafego': _texto(trafego),
                                           'manifesto': _texto(manifesto), 'porto_destino': _texto(destino),
                                           'origem': _texto(origem)}

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
        for ce, cont, peso, tara, iso in self.diag._por_lotes(
                'SELECT numeroCEmercante, codigoConteiner, pesoBruto, tara, isoCode FROM itensresumo '
                'WHERE codigoConteiner IN :conteineres', 'conteineres', conteineres):
            ce = normaliza_ce(ce)
            self.itens[(ce, _texto(cont))] = (peso, tara, _texto(iso))
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
        return {m: datas_no_porto(escalas) for m, escalas in self.escalas_com_porto(manifestos).items()}

    def escalas_com_porto(self, manifestos) -> Dict[str, List[Tuple[date, str]]]:
        """{manifesto: [(atracação prevista, porto)]} das escalas do manifesto. O manifesto SEM escala ligada no
        Mercante (caso CAAU5545018, 22/10/2026: medição, 0,2% dos desembarques, e em todos o navio e o início da
        operação conferiam com a escala declarada) fica com o início da operação no porto de descarregamento."""
        escalas = defaultdict(list)
        for manifesto, data, porto in self.diag._por_lotes(
                'SELECT em.manifesto, e.dataPrevistaAtracacao, e.portoDaEscala FROM escalamanifestoresumo em '
                'JOIN escalasresumo e ON e.numeroDaEscala = em.escala WHERE em.manifesto IN :manifestos',
                'manifestos', manifestos):
            dia = _dia(data)
            if dia:
                escalas[_texto(manifesto)].append((dia, _texto(porto)))
        for manifesto, data, porto in self.diag._por_lotes(
                'SELECT numero, dataInicioOperacao, portoDescarregamento FROM manifestosresumo '
                'WHERE numero IN :manifestos', 'manifestos', {m for m in manifestos if m and _texto(m) not in escalas}):
            dia = _dia(data)
            if dia:
                escalas[_texto(manifesto)].append((dia, _texto(porto)))
        return dict(escalas)

    # ------------------------------------------------------------------ pesagens do ciclo (P1)
    def candidatas_das_pesagens(self, alvos: Iterable[dict], horas: float = pc.HORAS_PAR) -> Dict[str, List[dict]]:
        """{placa: pesagens da placa até `horas` das pesagens `alvos`} — para o par na visita (a tara do conjunto não
        traz o contêiner). Uma consulta por dia das pesagens: janela curta pelo índice da placa."""
        por_dia = defaultdict(list)
        for alvo in alvos:
            if alvo['placa']:
                por_dia[alvo['data'].date()].append(alvo)
        por_placa: Dict[str, Dict[int, dict]] = defaultdict(dict)
        for lista in por_dia.values():
            params = {'de': min(a['data'] for a in lista) - timedelta(hours=horas),
                      'ate': max(a['data'] for a in lista) + timedelta(hours=horas)}
            for linha in self.diag._por_lotes(
                    f'SELECT {COLUNAS_PESAGEM} FROM apirecintos_pesagensveiculo '  # nosec B608 - colunas fixas
                    'WHERE placa IN :placas AND dataHoraOcorrencia >= :de AND dataHoraOcorrencia <= :ate',
                    'placas', {a['placa'] for a in lista}, params):
                pesagem = pesagem_de(linha)
                por_placa[pesagem['placa']][pesagem['id']] = pesagem
        return {placa: list(pesagens.values()) for placa, pesagens in por_placa.items()}

    def declarado(self, conteiner: str, ces: CEsResolvidos, desembarque_id: int) -> pc.Declarado:
        """Peso declarado do contêiner na família de CEs do ciclo (item do master ou soma dos filhotes), tara do
        contêiner (item do Mercante; senão a do desembarque; senão a típica do tamanho) e tipo ISO."""
        itens = [(ce, (self.info.get(ce) or {}).get('master'), *self.itens.get((ce, conteiner), (None, None, None)))
                 for ce in sorted(ces.ces)]
        peso, origem = pc.peso_da_familia([(ce, master, peso) for ce, master, peso, _, _ in itens])
        tipo = next((iso for *_, iso in itens if iso), None) or self.tipo_do(conteiner, desembarque_id)
        tara = next((tara for *_, tara, iso in itens if pc.tara_do_conteiner(tara, iso)[1] == 'informada'), None)
        tara_conteiner, origem_tara = pc.tara_do_conteiner(
            tara or (self.desembarques.get(desembarque_id) or {}).get('tara'), tipo)
        return pc.Declarado(ces.principal, peso, origem, tara_conteiner, origem_tara, tipo)

    def divisoes_das_balancas(self, desde: datetime) -> Dict[str, int]:
        """{recinto: divisão da balança rodoviária (kg)} pelas pesagens desde `desde` (`pc.divisao_pelas_contagens`)."""
        somas = ', '.join(f'SUM(CASE WHEN pesoBrutoBalanca % {d} = 0 THEN 1 ELSE 0 END)' for d in pc.DIVISOES)
        divisoes = {}
        for recinto, total, *multiplos in self.diag.consulta(
                f'SELECT codigoRecinto, COUNT(*), {somas} FROM apirecintos_pesagensveiculo '  # nosec B608 - fixo
                "WHERE dataHoraOcorrencia >= :desde AND pesoBrutoBalanca > 0 AND placa IS NOT NULL AND placa <> '' "
                'GROUP BY codigoRecinto', {'desde': desde}):
            divisao = pc.divisao_pelas_contagens({d: int(n or 0) for d, n in zip(pc.DIVISOES, multiplos)}, int(total))
            if divisao:
                divisoes[_texto(recinto)] = divisao
        return divisoes
