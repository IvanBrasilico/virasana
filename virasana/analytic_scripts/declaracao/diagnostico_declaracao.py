# coding: utf-8
"""
DIAGNÓSTICO DOS SINAIS DE DECLARAÇÃO INVENTADA (itens 16, 17 e 18 do `BACKLOG.md`) — passo 0 do plano aprovado pelo
usuário em 23/10/2026. Somente leitura. Regras em `regras_declaracao.py`.

2ª versão (23/10/2026, depois da 1ª rodada em produção): sinal novo 16e (cubagem copiada, pedido do usuário); 18 por
direção; 17a só em empresa de prefixo dominante; 16a/16b só como qualidade do dado; fora os pares raros e a cidade no
BL. E o CONTROLE DA MESMA ÉPOCA: na 1ª rodada todo acerto nas fichas e RVFs veio de CEs antigos (2021, sobretudo),
comparados com uma população de 30 dias — diferença de época, não de fraude, explicava o 17a. Agora cada CE de
referência ganha vizinhos pelo ID (criados na mesma hora, de outra família), e a validação mostra "vezes o controle".

As referências são evidência FRACA (usuário, 23/10/2026): as Fichas são poucas e foram escolhidas por outros critérios
(a análise de risco delas não usava estas técnicas) — sinal sem força nelas não está descartado, e força nelas não
prova fraude. O relatório dá os números; a decisão junta coerência do sinal, taxa de marcação, pesagem, revisão de
amostras e, se for o caso, um piloto no NAVI-I.

  0. ESTRUTURA: os índices de que a leitura depende (itens e NCMs por CE, filhotes pelo master) — sem eles, para;
  1. POPULAÇÃO E TEMPO: CEs (sem exportação) CRIADOS na janela — pela janela de ID do `conhecimentosresumo`, nunca
     pela create_date (= última alteração) —, com as famílias completadas; contêineres; tempo e estimativas;
  16. CONTÊINER × CUBAGEM × CARGA: preenchimento e formato; cubagens mais frequentes por tipo; ocupação e peso ÷ carga
     máxima por tipo; qualidade do dado (16a, 16b); cubagem copiada (16e);
  17. NÚMERO DO BL: formato da empresa de navegação e do número por tipo de BL; prefixo comum por empresa (17a);
  18. PESO × VOLUME × NCM: contêineres de um NCM só; nível do NCM com típico; dispersão; desvios; 18a/18b por direção;
  V. VALIDAÇÃO: cada sinal na população, nas referências (fichas positivas do risco histórico; RVFs — todas, por
     infração, com peso/volume da RVF fora de 0,8–1,2 do declarado; pesagens do ciclo de vida, por diagnóstico) e no
     controle da mesma época de cada referência;
  A. AMOSTRAS de CEs marcados (todos no --csv);
  R. REVISÃO (3ª versão, 24/10/2026): um CE por posição do NCM marcado no 18a+ ou no 18b+, para o usuário olhar no
     NAVI-I (imagem, descrição) — no --revisao, com colunas para a avaliação; e, na validação, as combinações do 18
     (18+ = peso ou densidade acima, candidato ao piloto; os dois; 18b+ com cubagem em m³ inteiro ou com fração);
  C. as consultas mais lentas.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons; da estação de trabalho também —
a conexão que cai pela rede é refeita e a consulta repetida):
    D=virasana/analytic_scripts/declaracao/diagnostico_declaracao.py
    virasana-venv/bin/python $D --saida declaracao3.log --csv declaracao3.csv --revisao revisao.csv
"""
import argparse
import csv
import re
import sys
import time
from collections import Counter, defaultdict, namedtuple
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import create_engine

sys.path.append('.')
sys.path.append('../ajna_docs/commons')

from virasana.analytic_scripts.ciclo_de_vida.ces_do_ciclo import normaliza_ce  # noqa: E402
from virasana.analytic_scripts.ciclo_de_vida.diagnostico_ciclo_de_vida import Diagnostico  # noqa: E402
from virasana.analytic_scripts.declaracao import regras_declaracao as rd  # noqa: E402
from virasana.navii.mercante_por_id import id_na_data, limita_tempo  # noqa: E402
from virasana.risco_historico import fichas_positivas  # noqa: E402

SECOES = ('16', '17', '18')
DIAS_PADRAO = 30
BLOCO_IDS = 20000                 # IDs do conhecimentosresumo por bloco (~1,5 dia de CEs)
LIMITE_CONSULTA = 300             # s por consulta no MariaDB
TRAFEGO_EXPORTACAO = '7'
TIPOS_MASTER = ('11', '15')       # MBL e MHBL: têm filhotes
VOLTAS_FAMILIA = 3                # MBL > MHBL > HBL
DESLOCAMENTOS_CONTROLE = (-150, 100, 250)       # vizinhos de cada referência pelo ID: a mesma época, outra família
GRUPOS_COM_CONTROLE = ('fichas positivas', 'RVF')   # as referências antigas (pesagem: já são da janela)
INDICES = (('conhecimentosresumo', 'numeroCEmercante'), ('conhecimentosresumo', 'numeroCEMaster'),
           ('itensresumo', 'numeroCEmercante'), ('ncmitemresumo', 'numeroCEMercante'))
TIPOS_NO_RELATORIO, EMPRESAS_NO_RELATORIO = 8, 20
INFRACOES_NA_VALIDACAO = 12
DIAGNOSTICOS_PESAGEM = ('divergencia', 'digitacao', 'moderada', 'coerente')
DIVERGENTE = (0.8, 1.2)           # RVF ÷ declarado fora disto: referência "peso/volume da RVF diverge"
FAIXAS_OCUPACAO = ((0.0, 'até 25%'), (0.25, '25 a 50%'), (0.5, '50 a 75%'), (0.75, '75 a 90%'), (0.9, '90 a 100%'),
                   (1.0, '100 a 110%'), (rd.OCUPACAO_IMPOSSIVEL, 'acima de 110%'))
FAIXAS_CARGA = ((0.0, 'até 50%'), (0.5, '50 a 80%'), (0.8, '80 a 100%'), (1.0, '100 a 110%'),
                (rd.CARGA_ACIMA, 'acima de 110%'))
FAIXAS_RAZAO = ((0.0, 'até 0,5'), (0.5, '0,5 a 0,8'), (0.8, '0,8 a 1,2'), (1.2, '1,2 a 2'), (2.0, 'acima de 2'))
FAIXAS_DISPERSAO = ((1.0, 'até ×1,3'), (1.3, '×1,3 a ×2'), (2.0, '×2 a ×4'), (4.0, 'acima de ×4'))
FAIXAS_DESVIOS = ((0.0, 'até 2'), (2.0, '2 a 3'), (rd.DESVIOS, '3 a 4'), (4.0, '4 a 6'), (6.0, 'acima de 6'))
FAIXAS_CONTEINERES = ((2, '2'), (3, '3'), (4, '4 a 5'), (6, '6 a 10'), (11, 'mais de 10'))
COLUNAS_CSV = ('sinal', 'descricao', 'ce', 'conteiner', 'na_janela', 'referencias', 'controle_de', 'motivo')
REVISAO_N = 30                    # CEs na lista de revisão (um por posição do NCM)
REVISAO_DESVIO_MAXIMO = 15.0      # acima disto é quase sempre digitação: fica fora da lista de revisão
ACIMA = ('18a+', '18b+')
COLUNAS_REVISAO = ('ce', 'conteiner', 'ncm', 'tipo', 'peso_kg', 'cubagem_m3', 'sinais', 'desvios_peso',
                   'desvios_densidade', 'motivo', 'avaliacao', 'observacao')

CAMPOS_CE = ('ID, numeroCEmercante, numeroCEMaster, tipoBLConhecimento, tipoTrafego, codigoEmpresaNavegacao, '
             'portoOrigemCarga, numConhecimento, cubagem')
# SQL: só a lista fixa de campos acima entra no texto
SQL_CES_JANELA = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE ID >= :de AND ID < :ate'  # nosec B608
SQL_CES_NUMERO = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE numeroCEmercante IN :ces'  # nosec B608
SQL_CES_MASTER = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE numeroCEMaster IN :masters'  # nosec B608
SQL_CES_ID = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE ID IN :ids'  # nosec B608
SQL_ITENS = ('SELECT numeroCEmercante, codigoConteiner, isoCode, pesoBruto, cubagemM3, codigoTipoEmbalagem, '
             'indicadorUsoParcial FROM itensresumo WHERE numeroCEmercante IN :ces')
SQL_NCMS = ('SELECT numeroCEMercante, codigoConteiner, identificacaoNCM, codigoTipoEmbalagem, qtdeVolumes '
            'FROM ncmitemresumo WHERE numeroCEMercante IN :ces')
SQL_PESO_DO_CE = 'SELECT numeroCEmercante, pesoBruto, cubagemM3 FROM itensresumo WHERE numeroCEmercante IN :ces'
SQL_RVFS = ('SELECT id, numeroCEmercante, peso, volume FROM ovr_verificacoesfisicas '
            "WHERE numeroCEmercante IS NOT NULL AND numeroCEmercante <> ''")
SQL_INFRACOES = 'SELECT e.rvf_id, i.nome FROM ovr_infracoesencontradas e JOIN ovr_infracoes i ON i.id = e.infracao_id'
SQL_PESAGENS = ('SELECT numeroCEmercante, diagnostico FROM ciclodevida_pesagem '
                'WHERE principal = 1 AND numeroCEmercante IS NOT NULL')
CAMPOS_ITEM = ('ce', 'conteiner', 'tipo', 'peso', 'cubagem', 'embalagem', 'uso_parcial')
CAMPOS_NCM = ('ce', 'conteiner', 'ncm', 'embalagem', 'volumes')

LinhaCE = namedtuple('LinhaCE', 'id master tipo_bl empresa numero cubagem')


def _t(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def formato_empresa(valor) -> str:
    """O que vem em `codigoEmpresaNavegacao`: CNPJ, outro código numérico, letras ou misto."""
    texto = re.sub(r'\s', '', _t(valor))
    if not texto:
        return '(vazio)'
    if texto.isdigit():
        return 'CNPJ (14 dígitos)' if len(texto) == 14 else f'{len(texto)} dígitos'
    return 'só letras' if texto.isalpha() else 'letras e dígitos'


def situacao_tipo(codigo) -> str:
    if not _t(codigo):
        return '(vazio)'
    chave = rd.chave_tipo(codigo)
    if not chave:
        return 'não reconhecido'
    return f'reconhecido, {"com" if rd.capacidade(chave) else "sem"} capacidade de referência'


def _quantil(ordenados: Sequence[float], q: float) -> float:
    return ordenados[min(len(ordenados) - 1, int(q * len(ordenados)))]


def base_do_sinal(sinal: str) -> str:
    """O conjunto de avaliáveis do sinal: '16e' tem o seu; os outros, o da seção ('16', '17', '18')."""
    return sinal if sinal == rd.CUBAGEM_REPETIDA else sinal[:2]


def _por_ce(linhas: List[dict]) -> Dict[str, List[dict]]:
    agrupadas = defaultdict(list)
    for linha in linhas:
        agrupadas[linha['ce']].append(linha)
    return agrupadas


class _Masters:
    """`get(CE)` -> o master do CE carregado (o `master_de` das regras), sem copiar o dicionário a cada bloco."""

    def __init__(self, ces: Dict[str, LinhaCE]):
        self.ces = ces

    def get(self, ce, padrao=None):
        linha = self.ces.get(ce)
        return linha.master if linha else padrao


class DiagnosticoDeclaracao(Diagnostico):

    def __init__(self, conexao, saida, desde: datetime, ate: datetime, secoes: Sequence[str] = SECOES,
                 amostras: int = 5, bloco: int = BLOCO_IDS, progresso: Optional[Callable[[str], None]] = None,
                 deslocamentos: Sequence[int] = DESLOCAMENTOS_CONTROLE, engine=None, revisao: int = REVISAO_N):
        super().__init__(conexao, saida, desde, ate, agora=ate, amostras=amostras)
        self.desde, self.ate, self.bloco, self.deslocamentos = desde, ate, bloco, tuple(deslocamentos)
        self.engine, self.revisao_n = engine, revisao
        self.secoes = [secao for secao in SECOES if secao in secoes]
        self.progresso = progresso or (lambda texto: print(texto, file=sys.stderr))
        self.ces: Dict[str, LinhaCE] = {}
        self.masters = _Masters(self.ces)
        self.populacao: Set[str] = set()
        self.janela: Tuple[int, int] = (0, 0)
        self.lidos_na_janela: Counter = Counter()
        self.masters_lidos: Set[str] = set()
        self.com_itens: Set[str] = set()
        self.familias_feitas: Set[str] = set()
        self.conteineres: List[rd.Conteiner] = []
        self.da_populacao = 0                     # os primeiros N contêineres são da população; os outros, não
        self.fase = 'população'
        self.contagens: Dict[str, Counter] = defaultdict(Counter)
        self.etapas: Counter = Counter()
        self.bls_populacao: List[rd.BL] = []
        self.mapa: Dict[str, Counter] = {}
        self.tipicos: Dict[str, Dict[str, rd.Tipico]] = {}
        self.referencias: Dict[str, Set[str]] = {}
        self.vizinhos: Dict[str, Set[str]] = defaultdict(set)    # CE de referência -> CEs de controle
        self.infracoes: Counter = Counter()
        self.rvfs: List[Tuple[str, Optional[float], Optional[float]]] = []
        self.razoes_rvf: Dict[str, Counter] = defaultdict(Counter)
        self.marcas: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)
        self.avaliaveis: Dict[str, Set[str]] = defaultdict(set)
        self.info_18: Dict[str, dict] = {}       # CE marcado no 18a+ / 18b+ -> o contêiner e os desvios (revisão)

    # ---------------------------------------------------------------- leitura
    def consulta(self, sql, params=None, listas=()) -> list:
        """Como a do Diagnostico; se a conexão caiu (rodada da estação de trabalho, pela rede), refaz e repete."""
        linhas = super().consulta(sql, params, listas)
        if not linhas and self.engine is not None and getattr(self.conexao, 'invalidated', False):
            self.escreve('   (conexão perdida: refeita e a consulta repetida)')
            self.conexao = self.engine.connect()
            linhas = super().consulta(sql, params, listas)
        return linhas

    def _cronometra(self, etapa: str, funcao, *args):
        inicio = time.perf_counter()
        try:
            return funcao(*args)
        finally:
            self.etapas[f'{self.fase}: {etapa}'] += time.perf_counter() - inicio

    def _guarda_ces(self, linhas) -> List[str]:
        """Guarda os CEs ainda não lidos (sem exportação); devolve os novos."""
        novos = []
        for id_, numero_ce, master, tipo_bl, trafego, empresa, _origem, numero_bl, cubagem in linhas:
            ce = normaliza_ce(numero_ce)
            if not ce or ce in self.ces or _t(trafego) == TRAFEGO_EXPORTACAO:
                continue
            self.ces[ce] = LinhaCE(int(id_), normaliza_ce(master), _t(tipo_bl), _t(empresa), _t(numero_bl),
                                   _t(cubagem))
            novos.append(ce)
        return novos

    def _volta_das_familias(self, ces: List[str]) -> List[str]:
        """Uma volta: os masters que faltam e os filhotes dos masters ainda não lidos; devolve os CEs novos."""
        masters = {self.ces[ce].master for ce in ces if self.ces[ce].master}
        masters |= {ce for ce in ces if self.ces[ce].tipo_bl in TIPOS_MASTER}
        novos = self._guarda_ces(self._por_lotes(SQL_CES_NUMERO, 'ces', [m for m in masters if m not in self.ces]))
        a_ler = masters - self.masters_lidos
        self.masters_lidos |= a_ler
        return novos + self._guarda_ces(self._por_lotes(SQL_CES_MASTER, 'masters', a_ler))

    def _completa_familias(self, ces: List[str]) -> List[str]:
        todos, novos = list(ces), list(ces)
        for _ in range(VOLTAS_FAMILIA):
            novos = self._volta_das_familias(novos)
            todos += novos
            if not novos:
                break
        return todos

    def _le_itens(self, ces: List[str]) -> Tuple[List[dict], List[dict]]:
        ces = [ce for ce in ces if ce not in self.com_itens]
        self.com_itens.update(ces)
        itens = [dict(zip(CAMPOS_ITEM, (normaliza_ce(linha[0]),) + tuple(linha[1:])))
                 for linha in self._por_lotes(SQL_ITENS, 'ces', ces)]
        ncms = [dict(zip(CAMPOS_NCM, (normaliza_ce(linha[0]),) + tuple(linha[1:])))
                for linha in self._por_lotes(SQL_NCMS, 'ces', ces)]
        return itens, ncms

    def _cubagens_repetidas(self, itens: List[dict], ncms: List[dict]):
        """16e, CE a CE (os itens do próprio CE; só se lê uma vez)."""
        ncms_do_ce = _por_ce(ncms)
        for ce, do_ce in _por_ce(itens).items():
            avaliavel, repeticao = rd.cubagem_repetida(do_ce, ncms_do_ce.get(ce, []))
            if avaliavel:
                self.avaliaveis[rd.CUBAGEM_REPETIDA].add(ce)
            if repeticao:
                self.marcas[rd.CUBAGEM_REPETIDA][ce] = ('', repeticao.motivo)
                if self.fase == 'população':
                    self.contagens['16e cubagem repetida (m³)'][rd.br(repeticao.cubagem, 1)] += 1
                    self.contagens['16e contêineres com a mesma cubagem'][
                        rd.faixa(repeticao.conteineres, FAIXAS_CONTEINERES)] += 1

    def _processa(self, novos: List[str]):
        """Completa as famílias dos CEs novos, lê itens e NCMs, aplica o 16e e monta os contêineres das famílias
        novas."""
        todos = self._cronometra('famílias (masters e filhotes)', self._completa_familias, novos)
        itens, ncms = self._cronometra('itens e NCMs', self._le_itens, todos)
        if self.fase == 'população':
            self._conta_itens(itens, ncms)
        if '16' in self.secoes:
            self._cubagens_repetidas(itens, ncms)
        feitos = [c for c in rd.conteineres(itens, ncms, self.masters) if c.familia not in self.familias_feitas]
        self.familias_feitas.update(c.familia for c in feitos)
        self.conteineres += feitos

    def _conta_itens(self, itens: List[dict], ncms: List[dict]):
        conta = self.contagens
        for item in itens:
            conta['peso do item (formato)'][rd.formato(item['peso'])] += 1
            conta['cubagem do item (formato)'][rd.formato(item['cubagem'])] += 1
            conta['tipo do contêiner (item)'][situacao_tipo(item['tipo'])] += 1
            conta['uso parcial (valor informado)'][_t(item['uso_parcial']) or '(vazio)'] += 1
        for linha in ncms:
            codigo = rd.ncm(linha['ncm'])
            conta['NCM (dígitos)'][f'{len(codigo)} dígitos' if codigo else '(sem NCM)'] += 1
            conta['volumes do NCM (formato)'][rd.formato(linha['volumes'])] += 1
            conta['NCM com contêiner'][
                'sim' if _t(linha['conteiner']) else 'não (vai para o único contêiner do CE)'] += 1

    def _janela(self) -> Tuple[int, int]:
        return id_na_data(self.conexao, self.desde) or 0, id_na_data(self.conexao, self.ate) or 0

    def _conta_janela(self, linhas):
        for linha in linhas:
            ce, exportacao = normaliza_ce(linha[1]), _t(linha[4]) == TRAFEGO_EXPORTACAO
            self.lidos_na_janela['exportação (fora)' if exportacao else 'sem exportação (população)'] += 1
            if ce and not exportacao:
                self.populacao.add(ce)

    def carrega_populacao(self):
        self.janela = self._cronometra('janela por ID', self._janela)
        de, ate = self.janela
        for inicio in range(de, ate, self.bloco):
            fim = min(inicio + self.bloco, ate)
            linhas = self._cronometra('CEs da janela', self.consulta, SQL_CES_JANELA, {'de': inicio, 'ate': fim})
            self._conta_janela(linhas)
            self._processa(self._guarda_ces(linhas))
            self.progresso(f'IDs até {fim} de {ate}: {len(self.populacao)} CEs, {len(self.conteineres)} contêineres')
        self.da_populacao = len(self.conteineres)

    # ---------------------------------------------------------------- referências e controle
    def _positivas(self) -> Set[str]:
        try:
            fichas = fichas_positivas.busca(self.conexao)
        except Exception as erro:  # noqa: o diagnóstico segue sem esta referência
            self.escreve(f'!! fichas positivas: {erro}')
            return set()
        return {ce for ce in (normaliza_ce(ficha.get('numeroCEmercante')) for ficha in fichas) if ce}

    def _rvfs(self):
        linhas = self.consulta(SQL_RVFS)
        ce_da_rvf = {rvf_id: normaliza_ce(ce) for rvf_id, ce, _, _ in linhas}
        self.rvfs = [(normaliza_ce(ce), rd.numero(peso), rd.numero(volume)) for _, ce, peso, volume in linhas
                     if normaliza_ce(ce)]
        self.referencias['RVF: todas'] = {ce for ce, _, _ in self.rvfs}
        por_infracao = defaultdict(set)
        for rvf_id, nome in self.consulta(SQL_INFRACOES):
            por_infracao[_t(nome) or '(sem nome)'].add(rvf_id)
        self.infracoes = Counter({nome: len(rvfs) for nome, rvfs in por_infracao.items()})
        for nome, _ in self.infracoes.most_common(INFRACOES_NA_VALIDACAO):
            self.referencias[f'RVF com a infração "{nome}"'] = {
                ce_da_rvf[rvf] for rvf in por_infracao[nome] if ce_da_rvf.get(rvf)}

    def _divergentes_da_rvf(self, nome: str, posicao: int, declarados: Dict[str, List[float]]) -> Set[str]:
        """CEs com a medida da RVF (peso = 1, volume = 2) fora de DIVERGENTE × o declarado nos itens do CE."""
        fora = set()
        for linha in self.rvfs:
            medido, declarado = linha[posicao], declarados.get(linha[0], [0.0, 0.0])[posicao - 1]
            if not medido or not declarado:
                self.razoes_rvf[nome]['sem medida na RVF' if not medido else 'sem declarado'] += 1
                continue
            razao = medido / declarado
            self.razoes_rvf[nome][rd.faixa(razao, FAIXAS_RAZAO)] += 1
            if not DIVERGENTE[0] <= razao <= DIVERGENTE[1]:
                fora.add(linha[0])
        return fora

    def _compara_rvf(self):
        declarados = defaultdict(lambda: [0.0, 0.0])
        for ce, peso, cubagem in self._por_lotes(SQL_PESO_DO_CE, 'ces', {ce for ce, _, _ in self.rvfs}):
            declarado = declarados[normaliza_ce(ce)]
            declarado[0] += rd.numero(peso) or 0.0
            declarado[1] += rd.numero(cubagem) or 0.0
        for nome, posicao in (('peso', 1), ('volume', 2)):
            self.referencias[f'RVF com {nome} fora de 0,8–1,2 do declarado'] = self._divergentes_da_rvf(
                nome, posicao, declarados)

    def _pesagens(self):
        por_diagnostico = defaultdict(set)
        for ce, diagnostico in self.consulta(SQL_PESAGENS):
            if normaliza_ce(ce):
                por_diagnostico[_t(diagnostico)].add(normaliza_ce(ce))
        for codigo in DIAGNOSTICOS_PESAGEM:
            if por_diagnostico.get(codigo):
                self.referencias[f'pesagem: {codigo}'] = por_diagnostico[codigo]

    def _carrega_por_lotes(self, sql: str, nome: str, valores: List, etapa: str) -> list:
        """Lê os CEs em blocos (CEs, famílias, itens) e devolve as linhas lidas."""
        lidas = []
        for inicio in range(0, len(valores), self.bloco):
            linhas = self._cronometra(etapa, self._por_lotes, sql, nome, valores[inicio:inicio + self.bloco])
            self._processa(self._guarda_ces(linhas))
            lidas += linhas
        return lidas

    def carrega_referencias(self):
        self.fase = 'referências'
        self.referencias['fichas positivas'] = self._cronometra('fichas positivas', self._positivas)
        self._cronometra('RVFs', self._rvfs)
        self._cronometra('RVF × declarado', self._compara_rvf)
        self._cronometra('pesagens', self._pesagens)
        fora = sorted({ce for ces in self.referencias.values() for ce in ces if ce not in self.ces})
        self._carrega_por_lotes(SQL_CES_NUMERO, 'ces', fora, 'CEs')
        self.progresso(f'referências: {len(fora)} CEs fora da janela')

    def carrega_controles(self):
        """Para cada CE de referência antiga (fichas, RVFs), os vizinhos pelo ID (DESLOCAMENTOS_CONTROLE): CEs
        criados na mesma época, que não sejam referência nem da mesma família."""
        self.fase = 'controles'
        if not self.deslocamentos:
            return
        com_controle = {ce for nome, ces in self.referencias.items() if nome.startswith(GRUPOS_COM_CONTROLE)
                        for ce in ces if ce in self.ces}
        todas = {ce for ces in self.referencias.values() for ce in ces}
        alvos = defaultdict(list)                          # ID vizinho -> CEs de referência
        for ce in com_controle:
            for deslocamento in self.deslocamentos:
                alvos[self.ces[ce].id + deslocamento].append(ce)
        for linha in self._carrega_por_lotes(SQL_CES_ID, 'ids', sorted(i for i in alvos if i > 0), 'CEs vizinhos'):
            vizinho = normaliza_ce(linha[1])
            if vizinho not in self.ces or vizinho in todas:
                continue
            for ce in alvos[int(linha[0])]:
                if rd.raiz(vizinho, self.masters) != rd.raiz(ce, self.masters):
                    self.vizinhos[ce].add(vizinho)
        self.progresso(f'controles: {len(set().union(*self.vizinhos.values())) if self.vizinhos else 0} CEs vizinhos')

    # ---------------------------------------------------------------- aprendizado e marcas
    def _fcl_da_populacao(self) -> List[rd.Conteiner]:
        return [c for c in self.conteineres[:self.da_populacao] if not c.uso_parcial]

    def _bl(self, ce: str) -> rd.BL:
        linha = self.ces[ce]
        return rd.BL(ce, linha.tipo_bl, linha.empresa, linha.numero)

    def aprende(self):
        """Tabelas aprendidas da POPULAÇÃO: mapa de prefixos e típicos dos NCMs (limiares lidos na chamada: o teste os
        reduz para uma base pequena)."""
        self.bls_populacao = [self._bl(ce) for ce in sorted(self.populacao) if ce in self.ces]
        self.mapa = rd.mapa_prefixos(self.bls_populacao)
        if '18' in self.secoes:
            amostras = defaultdict(lambda: defaultdict(list))
            for c in self._fcl_da_populacao():
                for medida, chave, valor in rd.medidas_ncm(c):
                    amostras[medida][chave].append(valor)
            self.tipicos = {medida: rd.tipicos(por_chave, rd.AMOSTRAS_MINIMAS)
                            for medida, por_chave in amostras.items()}

    def _marca(self, sinal: str, ces, conteiner: str, motivo: str):
        for ce in ces:
            self.marcas[sinal].setdefault(ce, (conteiner, motivo))

    def _marca_conteiner(self, c: rd.Conteiner):
        if c.uso_parcial:
            return
        sinais = []
        if '16' in self.secoes and c.chave:
            self.avaliaveis['16'].update(c.ces)
            sinais += rd.sinais_capacidade(c)
        if '18' in self.secoes and c.ncm_unico:
            self.avaliaveis['18'].update(c.ces)
            sinais_18 = rd.sinais_ncm(c, self.tipicos)
            self._guarda_18(c, sinais_18)
            sinais += sinais_18
        for sinal, motivo in sinais:
            self._marca(sinal, c.ces, c.conteiner, motivo)

    def _guarda_18(self, c: rd.Conteiner, sinais: List[Tuple[str, str]]):
        """O contêiner e os desvios dos CEs marcados acima do típico (18a+ / 18b+): a lista de revisão e a divisão do
        18b+ pela cubagem."""
        if not any(sinal in ACIMA for sinal, _ in sinais):
            return
        achados = rd.desvios_ncm(c, self.tipicos)
        info = {'conteiner': c.conteiner, 'ncm': c.ncm_unico, 'tipo': c.tipo, 'peso': c.peso, 'cubagem': c.cubagem,
                'redonda': rd.cubagem_redonda(c.cubagem),
                'z_peso': achados.get(rd.PESO_NCM, (None, None, None))[2],
                'z_densidade': achados.get(rd.DENSIDADE_NCM, (None, None, None))[2]}
        for ce in c.ces:
            self.info_18.setdefault(ce, info)

    def calcula_marcas(self):
        for c in self.conteineres:
            self._marca_conteiner(c)
        if '17' not in self.secoes:
            return
        for ce in self.ces:
            bl = self._bl(ce)
            if bl.da_companhia:
                self.avaliaveis['17'].add(ce)
                for sinal, motivo in rd.sinais_bl(bl, self.mapa):
                    self._marca(sinal, [ce], '', motivo)

    def _sinais_ativos(self, todos: bool = False) -> List[str]:
        """Os sinais em teste das seções pedidas (com `todos`, também os de qualidade do dado)."""
        return [sinal for sinal in (rd.DESCRICOES if todos else rd.SINAIS) if sinal[:2] in self.secoes]

    # ---------------------------------------------------------------- 0 e 1
    def secao_estrutura(self) -> bool:
        """Confere os índices de que a leitura depende; sem algum, o diagnóstico para (varreria a tabela)."""
        self.titulo('0. ESTRUTURA — índices usados pela leitura')
        if not self.mysql:
            self.escreve('(SHOW INDEX só no MariaDB/MySQL)')
            return True
        faltam = []
        for tabela, coluna in INDICES:
            primeiras = {_t(linha[4]).lower() for linha in self.consulta(f'SHOW INDEX FROM {tabela}')  # nosec B608
                         if int(linha[3]) == 1}
            self.escreve(f'{tabela}.{coluna}: {"índice" if coluna.lower() in primeiras else "SEM ÍNDICE"}')
            if coluna.lower() not in primeiras:
                faltam.append(f'{tabela}.{coluna}')
        if faltam:
            self.escreve(f'!! Sem índice em {", ".join(faltam)} — o diagnóstico para aqui (cada consulta varreria a '
                         'tabela). Peça o índice ao DBA ou mande este log.')
        return not faltam

    def _conta_tabela(self, nome: str, contagem: Optional[Counter] = None, limite: int = 15, faixas=None):
        """Valor, quantidade e %: os mais frequentes ou, com `faixas`, na ordem delas (e o resto depois)."""
        contagem = self.contagens.get(nome, Counter()) if contagem is None else contagem
        total = sum(contagem.values())
        ordem = [rotulo for _, rotulo in faixas or () if rotulo in contagem]
        ordem += [valor for valor, _ in contagem.most_common(limite) if valor not in ordem]
        self.escreve()
        self.tabela([nome, 'quantidade', '%'],
                    [(valor, contagem[valor], rd.pct(contagem[valor] / total)) for valor in ordem[:limite]])

    def _tempos(self, dias: float):
        da_populacao = sum(s for etapa, s in self.etapas.items() if etapa.startswith('população'))
        linhas = [(etapa, rd.br(segundos, 1)) for etapa, segundos in self.etapas.most_common()]
        linhas += [('total da população', rd.br(da_populacao, 1)),
                   ('estimativa da população em 90 dias', rd.br(da_populacao * 90 / dias)),
                   ('estimativa da população em 180 dias', rd.br(da_populacao * 180 / dias))]
        self.escreve()
        self.tabela(['etapa', 's'], linhas)

    def secao_populacao(self):
        self.titulo('1. POPULAÇÃO, FAMÍLIAS E TEMPO')
        de, ate = self.janela
        dias = max((self.ate - self.desde).total_seconds() / 86400, 1e-6)
        self.escreve(f'Janela: CEs criados de {self.desde:%d/%m/%Y %H:%M} a {self.ate:%d/%m/%Y %H:%M} '
                     f'({rd.br(dias, 1)} dias), IDs {de} a {ate - 1} — pela janela de ID (a create_date é a última '
                     'alteração).')
        self.tabela(['CEs lidos na janela', 'quantidade'], sorted(self.lidos_na_janela.items()))
        populacao = self.conteineres[:self.da_populacao]
        self.escreve()
        self.tabela(['população', 'quantidade'], [
            ('CEs (sem exportação)', len(self.populacao)),
            ('CEs carregados (com famílias, referências e controles)', len(self.ces)),
            ('contêineres (família × contêiner)', len(populacao)),
            ('cheios (FCL)', sum(not c.uso_parcial for c in populacao)),
            ('uso parcial (LCL, fora)', sum(c.uso_parcial for c in populacao))])
        self._conta_tabela('tipo de BL (população)', Counter(
            self.ces[ce].tipo_bl or '(vazio)' for ce in self.populacao if ce in self.ces))
        self._tempos(dias)

    # ---------------------------------------------------------------- 16
    def _cubagens_frequentes(self, conteineres: List[rd.Conteiner]):
        por_tipo = defaultdict(Counter)
        for c in conteineres:
            if c.chave and c.cubagem:
                por_tipo[c.chave][round(c.cubagem, 1)] += 1
        linhas = []
        for chave, contagem in sorted(por_tipo.items(), key=lambda par: -sum(par[1].values()))[:TIPOS_NO_RELATORIO]:
            total = sum(contagem.values())
            linhas.append((chave, total, '; '.join(f'{rd.br(valor, 1)} ({rd.pct(n / total, 0)})'
                                                   for valor, n in contagem.most_common(5))))
        self.escreve()
        self.escreve('Cubagens mais frequentes por tipo (muitos contêineres com o mesmo número = valor padrão):')
        self.tabela(['tipo', 'com cubagem', 'mais frequentes (m³)'], linhas)

    def _por_faixa(self, titulo: str, conteineres: List[rd.Conteiner], medida, campo: str, faixas):
        por_tipo = defaultdict(list)
        for c in conteineres:
            valor = medida(getattr(c, campo), c.chave)
            if valor is not None:
                por_tipo[c.chave].append(valor)
        rotulos = [rotulo for _, rotulo in faixas]
        linhas = []
        for chave, valores in sorted(por_tipo.items(), key=lambda par: -len(par[1])):
            contagem, ordenados = Counter(rd.faixa(v, faixas) for v in valores), sorted(valores)
            linhas.append([chave, len(valores)] + [rd.pct(contagem[r] / len(valores)) for r in rotulos]
                          + [rd.pct(_quantil(ordenados, 0.5), 0), rd.pct(_quantil(ordenados, 0.99), 0)])
        self.escreve()
        self.escreve(f'{titulo} — tipos com capacidade de referência:')
        self.tabela(['tipo', 'contêineres'] + rotulos + ['mediana', 'p99'], linhas)

    def _marcados(self, secao: str):
        linhas = []
        for sinal in self._sinais_ativos(todos=True):
            if sinal[:2] == secao:
                avaliaveis = self.avaliaveis[base_do_sinal(sinal)] & self.populacao
                marcados = len(set(self.marcas[sinal]) & avaliaveis)
                linhas.append((sinal, rd.DESCRICOES[sinal], len(avaliaveis), marcados,
                               rd.pct(marcados / len(avaliaveis), 2) if avaliaveis else '-'))
        self.escreve()
        self.escreve('CEs da população:')
        self.tabela(['sinal', 'descrição', 'avaliáveis', 'marcados', '% dos avaliáveis'], linhas)

    def secao_16(self):
        self.titulo('16. CONTÊINER × CUBAGEM × CARGA (16a/16b: qualidade do dado; 16e: cubagem copiada)')
        fcl = self._fcl_da_populacao()
        for nome in ('tipo do contêiner (item)', 'peso do item (formato)', 'cubagem do item (formato)',
                     'uso parcial (valor informado)', 'NCM (dígitos)', 'volumes do NCM (formato)', 'NCM com contêiner'):
            self._conta_tabela(nome)
        self._conta_tabela('origem do peso do contêiner (FCL)', Counter(c.origem_peso for c in fcl), limite=4)
        self._conta_tabela('origem da cubagem do contêiner (FCL)', Counter(c.origem_cubagem for c in fcl), limite=4)
        self._cubagens_frequentes(fcl)
        self._por_faixa('Ocupação (cubagem ÷ capacidade) por tipo', fcl, rd.ocupacao, 'cubagem', FAIXAS_OCUPACAO)
        self._por_faixa('Peso ÷ carga máxima por tipo', fcl, rd.carga, 'peso', FAIXAS_CARGA)
        self._conta_tabela('16e cubagem repetida (m³)', limite=12)
        self._conta_tabela('16e contêineres com a mesma cubagem', faixas=FAIXAS_CONTEINERES)
        self._marcados('16')

    # ---------------------------------------------------------------- 17
    def _formatos_por_tipo(self, bls: List[rd.BL]):
        tipos = sorted({bl.tipo_bl for bl in bls})
        contagem = Counter((rd.formato_bl(bl.numero), bl.tipo_bl) for bl in bls)
        formatos = [f for f, _ in Counter(rd.formato_bl(bl.numero) for bl in bls).most_common()]
        self.escreve()
        self.tabela(['formato do número do BL'] + [f'tipo {tipo or "(vazio)"}' for tipo in tipos],
                    [[f] + [contagem[(f, tipo)] for tipo in tipos] for f in formatos])

    def _mapa_relatorio(self):
        linhas = []
        for empresa, contagem in sorted(self.mapa.items(), key=lambda par: -sum(par[1].values()))[
                :EMPRESAS_NO_RELATORIO]:
            total = sum(contagem.values())
            comum, quantidade = contagem.most_common(1)[0]
            raros = sum(q for prefixo, q in contagem.items() if prefixo and q < rd.PREFIXO_RARO * total)
            linhas.append((empresa, total, comum or '(sem prefixo)', rd.pct(quantidade / total), len(contagem),
                           'sim' if quantidade >= rd.PREFIXO_DOMINANTE * total else 'não', raros))
        self.escreve()
        self.escreve(f'Prefixo do número por empresa de navegação (só BL e MBL; o 17a só vale com prefixo dominante, '
                     f'{rd.pct(rd.PREFIXO_DOMINANTE, 0)}+):')
        self.tabela(['empresa', 'BLs', 'prefixo comum', '%', 'prefixos', 'dominante', 'BLs de prefixo raro'], linhas)

    def secao_17(self):
        self.titulo('17. NÚMERO DO BL × EMPRESA DE NAVEGAÇÃO')
        bls = self.bls_populacao
        self._conta_tabela('empresa de navegação (formato)', Counter(formato_empresa(bl.empresa) for bl in bls))
        self._formatos_por_tipo(bls)
        self._mapa_relatorio()
        self._marcados('17')

    # ---------------------------------------------------------------- 18
    def _mais_especificos(self, c: rd.Conteiner) -> Dict[str, Tuple[str, float]]:
        """{medida: (chave, valor)} do nível mais específico que tem típico."""
        achados = {}
        for medida, chave, valor in rd.medidas_ncm(c):
            if medida not in achados and chave in self.tipicos.get(medida, {}):
                achados[medida] = (chave, valor)
        return achados

    def _niveis_e_desvios(self, unicos: List[rd.Conteiner]):
        niveis, desvios = defaultdict(Counter), defaultdict(Counter)
        for c in unicos:
            achados = self._mais_especificos(c)
            for medida in (rd.PESO_NCM, rd.DENSIDADE_NCM):
                if medida not in achados:
                    niveis[medida]['sem típico (ou sem medida)'] += 1
                    continue
                chave, valor = achados[medida]
                niveis[medida][f'NCM com {len(chave.split("|")[-1])} dígitos'] += 1
                desvios[medida][rd.faixa(abs(rd.desvios(valor, self.tipicos[medida][chave])), FAIXAS_DESVIOS)] += 1
        for medida in (rd.PESO_NCM, rd.DENSIDADE_NCM):
            self._conta_tabela(f'{medida} nível do típico usado', niveis[medida])
            self._conta_tabela(f'{medida} |desvios| do típico', desvios[medida], faixas=FAIXAS_DESVIOS)

    def _dispersao(self):
        linhas = []
        for medida, tabela in sorted(self.tipicos.items()):
            por_nivel = defaultdict(Counter)
            for chave, referencia in tabela.items():
                por_nivel[len(chave.split('|')[-1])][rd.faixa(10 ** referencia.mad, FAIXAS_DISPERSAO)] += 1
            for nivel, contagem in sorted(por_nivel.items(), reverse=True):
                linhas.append([medida, nivel, sum(contagem.values())] + [contagem[r] for _, r in FAIXAS_DISPERSAO])
        self.escreve()
        self.escreve('Dispersão dos típicos (fator de 1 desvio robusto; estreito = sinal útil):')
        self.tabela(['medida', 'dígitos', 'típicos'] + [r for _, r in FAIXAS_DISPERSAO], linhas)

    def secao_18(self):
        self.titulo('18. PESO × VOLUME × NCM (contêineres de um NCM só; por direção)')
        fcl = self._fcl_da_populacao()
        unicos = [c for c in fcl if c.ncm_unico]
        self.tabela(['contêineres FCL', 'quantidade'], [
            ('todos', len(fcl)), ('com um NCM só', len(unicos)),
            ('um NCM só, com peso e tamanho', sum(bool(c.peso and c.tamanho) for c in unicos)),
            ('um NCM só, com densidade', sum(rd.densidade(c) is not None for c in unicos))])
        self._niveis_e_desvios(unicos)
        self._dispersao()
        self._marcados('18')
        self._sobreposicao()

    def _acima(self, ce: str) -> List[str]:
        return [sinal for sinal in ACIMA if ce in self.marcas[sinal]]

    def _derivados(self) -> List[Tuple[str, str, Set[str]]]:
        """(código, descrição, CEs marcados) das combinações do 18 que a validação também mede."""
        if '18' not in self.secoes:
            return []
        peso, densidade = set(self.marcas['18a+']), set(self.marcas['18b+'])
        redonda = {ce for ce in densidade if self.info_18.get(ce, {}).get('redonda')}
        return [('18+', 'peso OU densidade acima do típico do NCM (candidato ao piloto)', peso | densidade),
                ('18a+ e 18b+', 'peso E densidade acima do típico do NCM', peso & densidade),
                ('18b+ redonda', 'densidade acima, cubagem em m³ inteiro (estimada)', redonda),
                ('18b+ com fração', 'densidade acima, cubagem com fração de m³', densidade - redonda)]

    def _sobreposicao(self):
        derivados = {codigo: ces & self.populacao for codigo, _, ces in self._derivados()}
        peso, densidade = set(self.marcas['18a+']) & self.populacao, set(self.marcas['18b+']) & self.populacao
        self.escreve()
        self.tabela(['CEs da população marcados acima do típico', 'quantidade'], [
            ('só peso (18a+)', len(peso - densidade)), ('só densidade (18b+)', len(densidade - peso)),
            ('os dois', len(peso & densidade)), ('algum (18+)', len(peso | densidade)),
            ('18b+ com cubagem em m³ inteiro', len(derivados['18b+ redonda'])),
            ('18b+ com cubagem com fração de m³', len(derivados['18b+ com fração']))])

    # ---------------------------------------------------------------- V, A e CSV
    def _controle(self, ces: Set[str]) -> Set[str]:
        return {vizinho for ce in ces for vizinho in self.vizinhos.get(ce, ())}

    def _taxa(self, sinal: str, ces: Set[str], marcados: Optional[Set[str]] = None) -> Tuple[int, int]:
        """(avaliáveis, marcados) do sinal entre os CEs (`marcados`: os de uma combinação)."""
        avaliaveis = self.avaliaveis[base_do_sinal(sinal)] & ces
        marcados = set(self.marcas[sinal]) if marcados is None else marcados
        return len(avaliaveis), len(marcados & avaliaveis)

    def _valida(self, sinal: str, grupos: List[Tuple[str, Set[str]]], descricao: Optional[str] = None,
                marcados: Optional[Set[str]] = None):
        marcados = set(self.marcas[sinal]) if marcados is None else marcados
        total_base, marcados_base = self._taxa(sinal, self.populacao, marcados)
        linhas = []
        for nome, ces in grupos:
            total, no_grupo = self._taxa(sinal, ces, marcados)
            linha = [nome, total, no_grupo, rd.pct(no_grupo / total, 2) if total else '-',
                     rd.br(rd.vezes(no_grupo, total, marcados_base, total_base), 1)]
            if nome.startswith(GRUPOS_COM_CONTROLE) and self.vizinhos:
                total_c, marcados_c = self._taxa(sinal, self._controle(ces), marcados)
                linha += [total_c, rd.pct(marcados_c / total_c, 2) if total_c else '-',
                          rd.br(rd.vezes(no_grupo, total, marcados_c, total_c), 1)]
            else:
                linha += ['', '', '']
            linhas.append(linha)
        self.escreve()
        self.escreve(f'{sinal} — {descricao or rd.DESCRICOES[sinal]}:')
        self.tabela(['grupo', 'avaliáveis', 'marcados', 'taxa', 'vezes a população', 'controle', 'taxa no controle',
                     'vezes o controle'], linhas)

    def secao_validacao(self):
        self.titulo('V. VALIDAÇÃO — os sinais nas referências e no controle da mesma época')
        self.escreve('Evidência FRACA: as Fichas são poucas e foram escolhidas por outros critérios. "Vezes o '
                     'controle" compara cada referência com CEs vizinhos pelo ID (criados na mesma época); "vezes a '
                     'população", com os CEs da janela (de outra época, para as referências antigas).')
        grupos = [('população (janela)', self.populacao)] + list(self.referencias.items())
        self.escreve()
        self.tabela(['grupo', 'CEs', 'carregados (sem exportação)', 'controles (vizinhos)'],
                    [(nome, len(ces), sum(ce in self.ces for ce in ces),
                      len(self._controle(ces)) if nome.startswith(GRUPOS_COM_CONTROLE) else '')
                     for nome, ces in grupos])
        self._conta_tabela('infração encontrada na RVF (todas)', self.infracoes, limite=60)
        for nome in ('peso', 'volume'):
            self._conta_tabela(f'{nome} da RVF ÷ declarado nos itens do CE', self.razoes_rvf[nome],
                               faixas=FAIXAS_RAZAO)
        for sinal in self._sinais_ativos():
            self._valida(sinal, grupos)
        for codigo, descricao, marcados in self._derivados():
            self._valida(codigo, grupos, descricao, marcados)

    def secao_amostras(self):
        self.titulo(f'A. AMOSTRAS ({self.amostras} CEs da população por sinal; todos no --csv)')
        for sinal in self._sinais_ativos(todos=True):
            marcados = [ce for ce in self.marcas[sinal] if ce in self.populacao]
            self.escreve()
            self.escreve(f'{sinal} — {rd.DESCRICOES[sinal]}: {len(marcados)} CEs')
            for ce in sorted(marcados, key=lambda numero: numero[::-1])[:self.amostras]:   # espalha pelas datas
                conteiner, motivo = self.marcas[sinal][ce]
                self.escreve(f'  CE {ce}' + (f' contêiner {conteiner}' if conteiner else '') + f': {motivo}')

    def candidatos_revisao(self) -> List[Tuple[str, dict]]:
        """Um CE da população por posição do NCM (4 dígitos), marcado no 18a+ ou no 18b+: primeiro os marcados nos
        dois, depois o maior desvio — sem os extremos (REVISAO_DESVIO_MAXIMO: quase sempre digitação)."""
        por_posicao = {}
        for ce, info in self.info_18.items():
            desvio = max(abs(info['z_peso'] or 0), abs(info['z_densidade'] or 0))
            if ce not in self.populacao or desvio > REVISAO_DESVIO_MAXIMO:
                continue
            chave = (len(self._acima(ce)), desvio, ce)
            posicao = info['ncm'][:4]
            if posicao not in por_posicao or chave > por_posicao[posicao]:
                por_posicao[posicao] = chave
        escolhidos = sorted(por_posicao.values(), reverse=True)[:self.revisao_n]
        return [(ce, self.info_18[ce]) for _, _, ce in escolhidos]

    def secao_revisao(self):
        if '18' not in self.secoes:
            return
        self.titulo('R. REVISÃO — um CE por posição do NCM marcado acima do típico (para olhar no NAVI-I; no '
                    '--revisao, com o motivo e colunas para a avaliação)')
        self.tabela(['CE', 'contêiner', 'NCM', 'tipo', 'peso (kg)', 'cubagem (m³)', 'sinais'],
                    [(ce, info['conteiner'], info['ncm'], info['tipo'] or '', rd.br(info['peso']),
                      rd.br(info['cubagem'], 1), ' '.join(self._acima(ce))) for ce, info in self.candidatos_revisao()])

    def grava_revisao(self, caminho: str):
        """A lista de revisão em CSV para o Excel (utf-8 com BOM), com colunas vazias para a avaliação."""
        with open(caminho, 'w', encoding='utf-8-sig', newline='') as arquivo:
            escritor = csv.writer(arquivo, delimiter=';')
            escritor.writerow(COLUNAS_REVISAO)
            for ce, info in self.candidatos_revisao():
                sinais = self._acima(ce)
                escritor.writerow([ce, info['conteiner'], info['ncm'], info['tipo'] or '', rd.br(info['peso']),
                                   rd.br(info['cubagem'], 1), ' '.join(sinais), rd.br(info['z_peso'], 1),
                                   rd.br(info['z_densidade'], 1), ' | '.join(self.marcas[s][ce][1] for s in sinais),
                                   '', ''])

    def grava_csv(self, caminho: str):
        referencias_do_ce, controle_de = defaultdict(list), defaultdict(set)
        for nome, ces in self.referencias.items():
            for ce in ces:
                referencias_do_ce[ce].append(nome)
        for ce, vizinhos in self.vizinhos.items():
            for vizinho in vizinhos:
                controle_de[vizinho].add(ce)
        with open(caminho, 'w', encoding='utf-8', newline='') as arquivo:
            escritor = csv.writer(arquivo, delimiter=';')
            escritor.writerow(COLUNAS_CSV)
            for sinal in self._sinais_ativos(todos=True):
                for ce, (conteiner, motivo) in sorted(self.marcas[sinal].items()):
                    escritor.writerow([sinal, rd.DESCRICOES[sinal], ce, conteiner,
                                       'sim' if ce in self.populacao else 'não',
                                       ' | '.join(referencias_do_ce.get(ce, [])),
                                       ' '.join(sorted(controle_de.get(ce, ()))), motivo])


def roda(diag: DiagnosticoDeclaracao, caminho_csv: Optional[str] = None,
         caminho_revisao: Optional[str] = None) -> bool:
    """O diagnóstico inteiro; False quando para na estrutura (falta índice)."""
    if not diag.secao_estrutura():
        return False
    diag.carrega_populacao()
    diag.aprende()
    diag.carrega_referencias()
    diag.carrega_controles()
    diag.calcula_marcas()
    diag.secao_populacao()
    for secao in diag.secoes:
        getattr(diag, 'secao_' + secao)()
    diag.secao_validacao()
    diag.secao_amostras()
    diag.secao_revisao()
    diag.resumo('C')
    if caminho_csv:
        diag.grava_csv(caminho_csv)
    if caminho_revisao:
        diag.grava_revisao(caminho_revisao)
    return True


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dias', type=int, default=DIAS_PADRAO, help='CEs criados nos N dias até --ate (padrão 30)')
    parser.add_argument('--ate', help='fim da janela (AAAA-MM-DD, exclusivo; padrão: agora)')
    parser.add_argument('--secoes', nargs='+', choices=SECOES, default=list(SECOES),
                        help='itens do backlog a diagnosticar (padrão: 16 17 18)')
    parser.add_argument('--amostras', type=int, default=5, help='CEs marcados por sinal no log (padrão 5)')
    parser.add_argument('--sem-controle', action='store_true',
                        help='não lê os vizinhos das referências (o controle da mesma época)')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    parser.add_argument('--csv', help='grava todos os CEs marcados neste CSV (;)')
    parser.add_argument('--revisao', help='grava a lista de revisão (18a+ / 18b+) neste CSV (;), para o Excel')
    parser.add_argument('--revisao-n', type=int, default=REVISAO_N,
                        help=f'CEs na lista de revisão, um por posição do NCM (padrão {REVISAO_N})')
    parser.add_argument('--uri', help='URI SQLAlchemy do banco (padrão: SQL_URI do ajna_commons)')
    parser.add_argument('--homologacao', action='store_true', help='usa o banco de homologação (SQL_URI_HOM)')
    return parser.parse_args(argv)


def main(argv=None):  # pragma: no cover - depende do banco real
    from virasana.analytic_scripts.ciclo_de_vida.servico_ciclo_de_vida import uri_do_banco
    args = _argumentos(argv)
    ate = datetime.strptime(args.ate, '%Y-%m-%d') if args.ate else datetime.now()
    engine = create_engine(uri_do_banco(args), pool_pre_ping=True)
    limita_tempo(engine, args.limite)
    saida = open(args.saida, 'w', encoding='utf-8') if args.saida else sys.stdout
    try:
        with engine.connect() as conexao:
            diag = DiagnosticoDeclaracao(conexao, saida, ate - timedelta(days=args.dias), ate, args.secoes,
                                         args.amostras, deslocamentos=() if args.sem_controle else
                                         DESLOCAMENTOS_CONTROLE, engine=engine, revisao=args.revisao_n)
            roda(diag, args.csv, args.revisao)
    finally:
        if args.saida:
            saida.close()


if __name__ == '__main__':  # pragma: no cover
    main()
