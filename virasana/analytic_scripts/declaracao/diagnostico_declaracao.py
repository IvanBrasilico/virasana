# coding: utf-8
"""
DIAGNÓSTICO DOS SINAIS DE DECLARAÇÃO INVENTADA (itens 16, 17 e 18 do `BACKLOG.md`) — passo 0 do plano aprovado pelo
usuário em 23/10/2026. Somente leitura. Regras em `regras_declaracao.py`.

  0. ESTRUTURA: os índices de que a leitura depende (itens e NCMs por CE, filhotes pelo master) — sem eles, para;
  1. POPULAÇÃO E TEMPO: CEs (sem exportação) CRIADOS na janela — pela janela de ID do `conhecimentosresumo`, nunca
     pela create_date (= última alteração) —, com as famílias completadas (master e filhotes fora da janela);
     contêineres; tempo de cada etapa e estimativa para 90 e 180 dias;
  16. TIPO × CUBAGEM × CARGA × EMBALAGEM: preenchimento e formato dos campos; cubagens mais frequentes por tipo
     (valor padrão?); ocupação e peso ÷ carga máxima por tipo (com a mediana e o p99 da própria base); pares raros
     carga × tipo e carga × embalagem — a lista é para o usuário dizer quais fazem sentido;
  17. NÚMERO DO BL: formato da empresa de navegação e do número por tipo de BL; prefixo comum por empresa; cidade no
     número × origem, por prefixo;
  18. PESO × VOLUME × NCM: contêineres de um NCM só; nível do NCM com típico; dispersão dos típicos; desvios;
  V. VALIDAÇÃO: cada sinal na população e nas referências — fichas positivas do risco histórico, RVFs (todas, por
     infração encontrada e com peso/volume da RVF fora de 0,8–1,2 do declarado) e pesagens do ciclo de vida (por
     diagnóstico) — com contagens e "vezes a população" (as referências são poucas: valem os números, não só o %);
  A. AMOSTRAS de CEs marcados por sinal, com o motivo (todos no --csv); C. as consultas mais lentas.

Uso (na raiz do repositório virasana, com as variáveis de ambiente do ajna_commons):
    D=virasana/analytic_scripts/declaracao/diagnostico_declaracao.py
    virasana-venv/bin/python $D --saida declaracao.log --csv declaracao.csv
    virasana-venv/bin/python $D --dias 90 --secoes 18 --saida declaracao_18.log    # se a estimativa de tempo couber
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
from virasana.navii import referencias  # noqa: E402
from virasana.navii.mercante_por_id import id_na_data, limita_tempo  # noqa: E402
from virasana.risco_historico import fichas_positivas  # noqa: E402

SECOES = ('16', '17', '18')
DIAS_PADRAO = 30
BLOCO_IDS = 20000                 # IDs do conhecimentosresumo por bloco (~1,5 dia de CEs)
LIMITE_CONSULTA = 300             # s por consulta no MariaDB
TRAFEGO_EXPORTACAO = '7'
TIPOS_MASTER = ('11', '15')       # MBL e MHBL: têm filhotes
VOLTAS_FAMILIA = 3                # MBL > MHBL > HBL
INDICES = (('conhecimentosresumo', 'numeroCEmercante'), ('conhecimentosresumo', 'numeroCEMaster'),
           ('itensresumo', 'numeroCEmercante'), ('ncmitemresumo', 'numeroCEMercante'))
TIPOS_NO_RELATORIO, PARES_NO_RELATORIO, EMPRESAS_NO_RELATORIO, PREFIXOS_NO_RELATORIO = 8, 30, 20, 20
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
COLUNAS_CSV = ('sinal', 'descricao', 'ce', 'conteiner', 'na_janela', 'referencias', 'motivo')

CAMPOS_CE = ('ID, numeroCEmercante, numeroCEMaster, tipoBLConhecimento, tipoTrafego, codigoEmpresaNavegacao, '
             'portoOrigemCarga, numConhecimento, cubagem')
# SQL: só a lista fixa de campos acima entra no texto
SQL_CES_JANELA = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE ID >= :de AND ID < :ate'  # nosec B608
SQL_CES_NUMERO = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE numeroCEmercante IN :ces'  # nosec B608
SQL_CES_MASTER = f'SELECT {CAMPOS_CE} FROM conhecimentosresumo WHERE numeroCEMaster IN :masters'  # nosec B608
SQL_ITENS = ('SELECT numeroCEmercante, codigoConteiner, isoCode, pesoBruto, cubagemM3, codigoTipoEmbalagem, '
             'indicadorUsoParcial FROM itensresumo WHERE numeroCEmercante IN :ces')
SQL_NCMS = ('SELECT numeroCEMercante, codigoConteiner, identificacaoNCM, codigoTipoEmbalagem FROM ncmitemresumo '
            'WHERE numeroCEMercante IN :ces')
SQL_PESO_DO_CE = 'SELECT numeroCEmercante, pesoBruto, cubagemM3 FROM itensresumo WHERE numeroCEmercante IN :ces'
SQL_RVFS = ('SELECT id, numeroCEmercante, peso, volume FROM ovr_verificacoesfisicas '
            "WHERE numeroCEmercante IS NOT NULL AND numeroCEmercante <> ''")
SQL_INFRACOES = 'SELECT e.rvf_id, i.nome FROM ovr_infracoesencontradas e JOIN ovr_infracoes i ON i.id = e.infracao_id'
SQL_PESAGENS = ('SELECT numeroCEmercante, diagnostico FROM ciclodevida_pesagem '
                'WHERE principal = 1 AND numeroCEmercante IS NOT NULL')
CAMPOS_ITEM = ('ce', 'conteiner', 'tipo', 'peso', 'cubagem', 'embalagem', 'uso_parcial')
CAMPOS_NCM = ('ce', 'conteiner', 'ncm', 'embalagem')

LinhaCE = namedtuple('LinhaCE', 'master tipo_bl empresa numero origem cubagem')


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


def _sinais_da(secao: str) -> List[str]:
    return [sinal for sinal in rd.SINAIS if sinal.startswith(secao)]


class _Masters:
    """`get(CE)` -> o master do CE carregado (o `master_de` das regras), sem copiar o dicionário a cada bloco."""

    def __init__(self, ces: Dict[str, LinhaCE]):
        self.ces = ces

    def get(self, ce, padrao=None):
        linha = self.ces.get(ce)
        return linha.master if linha else padrao


class DiagnosticoDeclaracao(Diagnostico):

    def __init__(self, conexao, saida, desde: datetime, ate: datetime, secoes: Sequence[str] = SECOES,
                 amostras: int = 5, bloco: int = BLOCO_IDS, progresso: Optional[Callable[[str], None]] = None):
        super().__init__(conexao, saida, desde, ate, agora=ate, amostras=amostras)
        self.desde, self.ate, self.bloco = desde, ate, bloco
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
        self.da_populacao = 0                     # os primeiros N contêineres são da população; os outros, referências
        self.fase = 'população'
        self.contagens: Dict[str, Counter] = defaultdict(Counter)
        self.etapas: Counter = Counter()
        self.pares: Dict[str, Counter] = {'tipo': Counter(), 'embalagem': Counter()}
        self.raros: Dict[str, frozenset] = {'tipo': frozenset(), 'embalagem': frozenset()}
        self.bls_populacao: List[rd.BL] = []
        self.mapa: Dict[str, Counter] = {}
        self.taxas: Dict[str, Tuple[int, int]] = {}
        self.prefixos_cidade: frozenset = frozenset()
        self.tipicos: Dict[str, Dict[str, rd.Tipico]] = {}
        self.referencias: Dict[str, Set[str]] = {}
        self.infracoes: Counter = Counter()
        self.rvfs: List[Tuple[str, Optional[float], Optional[float]]] = []
        self.razoes_rvf: Dict[str, Counter] = defaultdict(Counter)
        self.marcas: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)
        self.avaliaveis: Dict[str, Set[str]] = defaultdict(set)

    # ---------------------------------------------------------------- leitura
    def _cronometra(self, etapa: str, funcao, *args):
        inicio = time.perf_counter()
        try:
            return funcao(*args)
        finally:
            self.etapas[f'{self.fase}: {etapa}'] += time.perf_counter() - inicio

    def _guarda_ces(self, linhas) -> List[str]:
        """Guarda os CEs ainda não lidos (sem exportação); devolve os novos."""
        novos = []
        for _id, numero_ce, master, tipo_bl, trafego, empresa, origem, numero_bl, cubagem in linhas:
            ce = normaliza_ce(numero_ce)
            if not ce or ce in self.ces or _t(trafego) == TRAFEGO_EXPORTACAO:
                continue
            self.ces[ce] = LinhaCE(normaliza_ce(master), _t(tipo_bl), _t(empresa), _t(numero_bl), _t(origem),
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

    def _processa(self, novos: List[str]):
        """Completa as famílias dos CEs novos, lê itens e NCMs e monta os contêineres das famílias novas."""
        todos = self._cronometra('famílias (masters e filhotes)', self._completa_familias, novos)
        itens, ncms = self._cronometra('itens e NCMs', self._le_itens, todos)
        if self.fase == 'população':
            self._conta_itens(itens, ncms)
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
            conta['embalagem do item'][_t(item['embalagem']) or '(vazia)'] += 1
        for linha in ncms:
            codigo = rd.ncm(linha['ncm'])
            conta['NCM (dígitos)'][f'{len(codigo)} dígitos' if codigo else '(sem NCM)'] += 1
            conta['embalagem do NCM'][_t(linha['embalagem']) or '(vazia)'] += 1
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

    # ---------------------------------------------------------------- referências
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

    def carrega_referencias(self):
        self.fase = 'referências'
        self.referencias['fichas positivas'] = self._cronometra('fichas positivas', self._positivas)
        self._cronometra('RVFs', self._rvfs)
        self._cronometra('RVF × declarado', self._compara_rvf)
        self._cronometra('pesagens', self._pesagens)
        fora = sorted({ce for ces in self.referencias.values() for ce in ces if ce not in self.ces})
        for inicio in range(0, len(fora), self.bloco):
            linhas = self._cronometra('CEs', self._por_lotes, SQL_CES_NUMERO, 'ces', fora[inicio:inicio + self.bloco])
            self._processa(self._guarda_ces(linhas))
        self.progresso(f'referências: {len(fora)} CEs fora da janela')

    # ---------------------------------------------------------------- aprendizado e marcas
    def _fcl_da_populacao(self) -> List[rd.Conteiner]:
        return [c for c in self.conteineres[:self.da_populacao] if not c.uso_parcial]

    def _bl(self, ce: str) -> rd.BL:
        linha = self.ces[ce]
        return rd.BL(ce, linha.tipo_bl, linha.empresa, linha.numero, linha.origem)

    def aprende(self):
        """Tabelas aprendidas da POPULAÇÃO: pares raros, mapa de prefixos, cidades e típicos dos NCMs."""
        fcl = self._fcl_da_populacao()
        for c in fcl:
            self.pares['tipo'].update(rd.pares_tipo_carga(c))
            self.pares['embalagem'].update(rd.pares_embalagem_carga(c))
        # os limiares são lidos aqui, na chamada (o teste os reduz para uma base pequena)
        self.raros = {nome: rd.pares_raros(contagem, rd.FRACAO_PAR_RARO, rd.MINIMO_DO_CAPITULO)
                      for nome, contagem in self.pares.items()}
        self.bls_populacao = [self._bl(ce) for ce in sorted(self.populacao) if ce in self.ces]
        self.mapa = rd.mapa_prefixos(self.bls_populacao)
        self.taxas = rd.taxas_cidade(self.bls_populacao)
        self.prefixos_cidade = rd.prefixos_com_cidade(self.taxas, rd.CIDADE_BATE, rd.AMOSTRAS_MINIMAS)
        if '18' in self.secoes:
            amostras = defaultdict(lambda: defaultdict(list))
            for c in fcl:
                for sinal, chave, valor in rd.medidas_ncm(c):
                    amostras[sinal][chave].append(valor)
            self.tipicos = {sinal: rd.tipicos(por_chave, rd.AMOSTRAS_MINIMAS) for sinal, por_chave in amostras.items()}

    def _marca(self, sinal: str, ces, conteiner: str, motivo: str):
        for ce in ces:
            self.marcas[sinal].setdefault(ce, (conteiner, motivo))

    def _marca_conteiner(self, c: rd.Conteiner):
        if c.uso_parcial:
            return
        sinais = []
        if '16' in self.secoes and c.chave:
            self.avaliaveis['16'].update(c.ces)
            sinais += rd.sinais_capacidade(c) + rd.sinais_pares(c, self.raros['tipo'], self.raros['embalagem'])
        if '18' in self.secoes and c.ncm_unico:
            self.avaliaveis['18'].update(c.ces)
            sinais += rd.sinais_ncm(c, self.tipicos)
        for sinal, motivo in sinais:
            self._marca(sinal, c.ces, c.conteiner, motivo)

    def calcula_marcas(self):
        for c in self.conteineres:
            self._marca_conteiner(c)
        if '17' not in self.secoes:
            return
        for ce in self.ces:
            bl = self._bl(ce)
            if bl.da_companhia:
                self.avaliaveis['17'].add(ce)
                for sinal, motivo in rd.sinais_bl(bl, self.mapa, self.prefixos_cidade):
                    self._marca(sinal, [ce], '', motivo)

    def _sinais_ativos(self) -> List[str]:
        return [sinal for sinal in rd.SINAIS if sinal[:2] in self.secoes]

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
            ('CEs carregados (com famílias e referências)', len(self.ces)),
            ('contêineres (família × contêiner)', len(populacao)),
            ('cheios (FCL)', sum(not c.uso_parcial for c in populacao)),
            ('uso parcial (LCL, fora dos 16 e 18)', sum(c.uso_parcial for c in populacao))])
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

    def _pares_raros(self, nome: str, coluna: str, descreve: Callable[[str], str]):
        contagem, raros, totais = self.pares[nome], self.raros[nome], Counter()
        for (capitulo, _), quantidade in contagem.items():
            totais[capitulo] += quantidade
        ordem = sorted(raros, key=lambda par: (-totais[par[0]], -contagem[par], par))
        linhas = [(f'{capitulo} {(referencias.descricao_posicao_ncm(capitulo) or "")[:40]}', descreve(valor),
                   contagem[par], totais[capitulo], rd.pct(contagem[par] / totais[capitulo], 2))
                  for par in ordem[:PARES_NO_RELATORIO] for capitulo, valor in [par]]
        self.escreve()
        self.escreve(f'Pares raros carga × {coluna}: {len(raros)} (menos de {rd.pct(rd.FRACAO_PAR_RARO)} dos '
                     f'contêineres da carga, nas cargas com {rd.MINIMO_DO_CAPITULO}+ contêineres). Os das cargas mais '
                     'frequentes — quais fazem sentido como sinal?')
        self.tabela(['capítulo', coluna, 'contêineres', 'da carga', '%'], linhas)

    def _marcados(self, secao: str):
        avaliaveis = self.avaliaveis[secao] & self.populacao
        linhas, algum = [], set()
        for sinal in _sinais_da(secao):
            marcados = set(self.marcas[sinal]) & avaliaveis
            algum |= marcados
            linhas.append((sinal, rd.SINAIS[sinal], len(marcados), rd.pct(len(marcados) / len(avaliaveis), 2)
                           if avaliaveis else '-'))
        linhas.append(('', 'algum sinal da seção', len(algum), rd.pct(len(algum) / len(avaliaveis), 2)
                       if avaliaveis else '-'))
        self.escreve()
        self.escreve(f'CEs da população avaliáveis: {len(avaliaveis)}')
        self.tabela(['sinal', 'descrição', 'CEs marcados', '% dos avaliáveis'], linhas)

    def secao_16(self):
        self.titulo('16. TIPO DO CONTÊINER × CUBAGEM × CARGA × EMBALAGEM')
        fcl = self._fcl_da_populacao()
        for nome in ('tipo do contêiner (item)', 'peso do item (formato)', 'cubagem do item (formato)',
                     'uso parcial (valor informado)', 'embalagem do item', 'embalagem do NCM', 'NCM (dígitos)',
                     'NCM com contêiner'):
            self._conta_tabela(nome)
        self._conta_tabela('cubagem do CE (formato)', Counter(
            rd.formato(self.ces[ce].cubagem) for ce in self.populacao if ce in self.ces))
        self._conta_tabela('origem do peso do contêiner (FCL)', Counter(c.origem_peso for c in fcl))
        self._conta_tabela('origem da cubagem do contêiner (FCL)', Counter(c.origem_cubagem for c in fcl))
        self._cubagens_frequentes(fcl)
        self._por_faixa('Ocupação (cubagem ÷ capacidade) por tipo', fcl, rd.ocupacao, 'cubagem', FAIXAS_OCUPACAO)
        self._por_faixa('Peso ÷ carga máxima por tipo', fcl, rd.carga, 'peso', FAIXAS_CARGA)
        self._pares_raros('tipo', 'tipo do contêiner', rd.nome_grupo)
        self._pares_raros('embalagem', 'embalagem', str)
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
                           rd.pct(contagem[''] / total), raros))
        self.escreve()
        self.escreve('Prefixo do número por empresa de navegação (só BL e MBL):')
        self.tabela(['empresa', 'BLs', 'prefixo comum', '%', 'prefixos', 'sem prefixo', 'BLs de prefixo raro'], linhas)

    def _cidades_relatorio(self):
        linhas = [(prefixo, total, bate, rd.pct(bate / total), 'sim' if prefixo in self.prefixos_cidade else '')
                  for prefixo, (bate, total) in sorted(self.taxas.items(), key=lambda par: -par[1][1])[
                      :PREFIXOS_NO_RELATORIO]]
        self.escreve()
        self.escreve(f'Cidade no número (3 letras depois do prefixo) × porto de origem — vira sinal com '
                     f'{rd.pct(rd.CIDADE_BATE, 0)}+ e {rd.AMOSTRAS_MINIMAS}+ BLs:')
        self.tabela(['prefixo', 'BLs com cidade e origem', 'batem', '%', 'vira sinal'], linhas)

    def secao_17(self):
        self.titulo('17. NÚMERO DO BL × EMPRESA DE NAVEGAÇÃO × ORIGEM')
        bls = self.bls_populacao
        self._conta_tabela('empresa de navegação (formato)', Counter(formato_empresa(bl.empresa) for bl in bls))
        self._conta_tabela('empresa de navegação (as mais frequentes)', Counter(bl.empresa or '(vazia)' for bl in bls))
        self._formatos_por_tipo(bls)
        self._mapa_relatorio()
        self._cidades_relatorio()
        self._marcados('17')

    # ---------------------------------------------------------------- 18
    def _mais_especificos(self, c: rd.Conteiner) -> Dict[str, Tuple[str, float]]:
        """{sinal: (chave, valor)} do nível mais específico que tem típico."""
        achados = {}
        for sinal, chave, valor in rd.medidas_ncm(c):
            if sinal not in achados and chave in self.tipicos.get(sinal, {}):
                achados[sinal] = (chave, valor)
        return achados

    def _niveis_e_desvios(self, unicos: List[rd.Conteiner]):
        niveis, desvios = defaultdict(Counter), defaultdict(Counter)
        for c in unicos:
            achados = self._mais_especificos(c)
            for sinal in (rd.PESO_NCM, rd.DENSIDADE_NCM):
                if sinal not in achados:
                    niveis[sinal]['sem típico (ou sem medida)'] += 1
                    continue
                chave, valor = achados[sinal]
                niveis[sinal][f'NCM com {len(chave.split("|")[-1])} dígitos'] += 1
                desvios[sinal][rd.faixa(abs(rd.desvios(valor, self.tipicos[sinal][chave])), FAIXAS_DESVIOS)] += 1
        for sinal in (rd.PESO_NCM, rd.DENSIDADE_NCM):
            self._conta_tabela(f'{sinal} nível do típico usado', niveis[sinal])
            self._conta_tabela(f'{sinal} |desvios| do típico', desvios[sinal], faixas=FAIXAS_DESVIOS)

    def _dispersao(self):
        linhas = []
        for sinal, tabela in sorted(self.tipicos.items()):
            por_nivel = defaultdict(Counter)
            for chave, referencia in tabela.items():
                por_nivel[len(chave.split('|')[-1])][rd.faixa(10 ** referencia.mad, FAIXAS_DISPERSAO)] += 1
            for nivel, contagem in sorted(por_nivel.items(), reverse=True):
                linhas.append([sinal, nivel, sum(contagem.values())] + [contagem[r] for _, r in FAIXAS_DISPERSAO])
        self.escreve()
        self.escreve('Dispersão dos típicos (fator de 1 desvio robusto; estreito = sinal útil):')
        self.tabela(['sinal', 'dígitos', 'típicos'] + [r for _, r in FAIXAS_DISPERSAO], linhas)

    def _maiores_tipicos(self):
        tabela = {chave: t for chave, t in self.tipicos.get(rd.PESO_NCM, {}).items() if len(chave.split('|')[1]) == 4}
        linhas = [(chave, (referencias.descricao_posicao_ncm(chave.split('|')[1]) or '')[:50], t.n, rd.br(t.p10),
                   rd.br(10 ** t.mediana), rd.br(t.p90))
                  for chave, t in sorted(tabela.items(), key=lambda par: -par[1].n)[:TIPOS_NO_RELATORIO + 2]]
        self.escreve()
        self.escreve('Peso por contêiner das posições (4 dígitos) mais frequentes, em kg:')
        self.tabela(['tamanho|NCM', 'posição', 'contêineres', 'p10', 'mediana', 'p90'], linhas)

    def secao_18(self):
        self.titulo('18. PESO × VOLUME × NCM (contêineres de um NCM só)')
        fcl = self._fcl_da_populacao()
        unicos = [c for c in fcl if c.ncm_unico]
        self.tabela(['contêineres FCL', 'quantidade'], [
            ('todos', len(fcl)), ('com NCM', sum(bool(c.ncms) for c in fcl)), ('com um NCM só', len(unicos)),
            ('um NCM só, com peso e tamanho', sum(bool(c.peso and c.tamanho) for c in unicos)),
            ('um NCM só, com densidade', sum(rd.densidade(c) is not None for c in unicos))])
        self._niveis_e_desvios(unicos)
        self._dispersao()
        self._maiores_tipicos()
        self._marcados('18')

    # ---------------------------------------------------------------- V, A e CSV
    def _valida(self, sinal: str, grupos: List[Tuple[str, Set[str]]]):
        avaliaveis_secao, marcados = self.avaliaveis[sinal[:2]], set(self.marcas[sinal])
        base = avaliaveis_secao & self.populacao
        marcados_base = len(marcados & base)
        linhas = []
        for nome, ces in grupos:
            avaliaveis = avaliaveis_secao & ces
            no_grupo = len(marcados & avaliaveis)
            linhas.append((nome, len(avaliaveis), no_grupo, rd.pct(no_grupo / len(avaliaveis), 2) if avaliaveis
                           else '-', rd.br(rd.vezes(no_grupo, len(avaliaveis), marcados_base, len(base)), 1)))
        self.escreve()
        self.escreve(f'{sinal} — {rd.SINAIS[sinal]}:')
        self.tabela(['grupo', 'avaliáveis', 'marcados', 'taxa', 'vezes a população'], linhas)

    def secao_validacao(self):
        self.titulo('V. VALIDAÇÃO — os sinais nas referências')
        grupos = [('população (janela)', self.populacao)] + list(self.referencias.items())
        self.tabela(['grupo', 'CEs', 'carregados (sem exportação)'],
                    [(nome, len(ces), sum(ce in self.ces for ce in ces)) for nome, ces in grupos])
        self._conta_tabela('infração encontrada na RVF (todas; quais contam como declaração falsa?)', self.infracoes,
                           limite=60)
        for nome in ('peso', 'volume'):
            self._conta_tabela(f'{nome} da RVF ÷ declarado nos itens do CE', self.razoes_rvf[nome],
                               faixas=FAIXAS_RAZAO)
        for sinal in self._sinais_ativos():
            self._valida(sinal, grupos)

    def secao_amostras(self):
        self.titulo(f'A. AMOSTRAS ({self.amostras} CEs da população por sinal; todos no --csv)')
        for sinal in self._sinais_ativos():
            marcados = [ce for ce in self.marcas[sinal] if ce in self.populacao]
            self.escreve()
            self.escreve(f'{sinal} — {rd.SINAIS[sinal]}: {len(marcados)} CEs')
            for ce in sorted(marcados, key=lambda numero: numero[::-1])[:self.amostras]:   # espalha pelas datas
                conteiner, motivo = self.marcas[sinal][ce]
                self.escreve(f'  CE {ce}' + (f' contêiner {conteiner}' if conteiner else '') + f': {motivo}')

    def grava_csv(self, caminho: str):
        referencias_do_ce = defaultdict(list)
        for nome, ces in self.referencias.items():
            for ce in ces:
                referencias_do_ce[ce].append(nome)
        with open(caminho, 'w', encoding='utf-8', newline='') as arquivo:
            escritor = csv.writer(arquivo, delimiter=';')
            escritor.writerow(COLUNAS_CSV)
            for sinal in self._sinais_ativos():
                for ce, (conteiner, motivo) in sorted(self.marcas[sinal].items()):
                    escritor.writerow([sinal, rd.SINAIS[sinal], ce, conteiner, 'sim' if ce in self.populacao else 'não',
                                       ' | '.join(referencias_do_ce.get(ce, [])), motivo])


def roda(diag: DiagnosticoDeclaracao, caminho_csv: Optional[str] = None) -> bool:
    """O diagnóstico inteiro; False quando para na estrutura (falta índice)."""
    if not diag.secao_estrutura():
        return False
    diag.carrega_populacao()
    diag.aprende()
    diag.carrega_referencias()
    diag.calcula_marcas()
    diag.secao_populacao()
    for secao in diag.secoes:
        getattr(diag, 'secao_' + secao)()
    diag.secao_validacao()
    diag.secao_amostras()
    diag.resumo('C')
    if caminho_csv:
        diag.grava_csv(caminho_csv)
    return True


def _argumentos(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dias', type=int, default=DIAS_PADRAO, help='CEs criados nos N dias até --ate (padrão 30)')
    parser.add_argument('--ate', help='fim da janela (AAAA-MM-DD, exclusivo; padrão: agora)')
    parser.add_argument('--secoes', nargs='+', choices=SECOES, default=list(SECOES),
                        help='itens do backlog a diagnosticar (padrão: 16 17 18)')
    parser.add_argument('--amostras', type=int, default=5, help='CEs marcados por sinal no log (padrão 5)')
    parser.add_argument('--limite', type=float, default=LIMITE_CONSULTA,
                        help=f'segundos por consulta no MariaDB, 0 = sem limite (padrão {LIMITE_CONSULTA})')
    parser.add_argument('--saida', help='grava o relatório neste arquivo (padrão: só na tela)')
    parser.add_argument('--csv', help='grava todos os CEs marcados neste CSV (;)')
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
                                         args.amostras)
            roda(diag, args.csv)
    finally:
        if args.saida:
            saida.close()


if __name__ == '__main__':  # pragma: no cover
    main()
