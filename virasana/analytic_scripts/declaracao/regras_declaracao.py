# coding: utf-8
"""
SINAIS DE DECLARAÇÃO INVENTADA no Mercante — regras PURAS, sem banco (itens 16, 17 e 18 do `BACKLOG.md`; plano
aprovado pelo usuário em 23/10/2026: o passo 0 é o diagnóstico nos dados, `diagnostico_declaracao.py`). Se um sinal
valer, estas mesmas regras servem depois ao NAVI-I e/ou ao risco histórico.

Sinais em teste (1ª rodada em produção, 23/10/2026; decisões do usuário no mesmo dia):
  - 16e, CUBAGEM COPIADA: no CE, 2+ contêineres cheios com a mesma mercadoria (o mesmo conjunto de NCMs) e a MESMA
    cubagem, mas pesos ou volumes diferentes — a cubagem não foi medida (pedido do usuário: "no mínimo, dado
    inventado");
  - 17a, número do BL × companhia: o PREFIXO de 4 letras (tipo SCAC) do `numConhecimento` raro para a empresa de
    navegação do CE (mapa aprendido da base) — só nas empresas com um prefixo DOMINANTE (a agência que numera de cem
    jeitos não diz nada) e só em BL (10) e MBL (11): no HBL o número é do agente;
  - 18, peso × volume × NCM, por DIREÇÃO: nos contêineres de UM NCM só, o PESO POR CONTÊINER (por tamanho, 18a) e a
    DENSIDADE (peso ÷ cubagem, 18b) acima (+) ou abaixo (-) do típico do NCM, aprendido da base em escala log (mediana
    e MAD), no nível mais específico (8, 6 ou 4 dígitos) com amostras suficientes. Na 1ª rodada só a densidade ACIMA
    se separou nas referências; na 3ª (24/10/2026), só quando a cubagem é em m³ inteiro (estimada): densidade acima
    com cubagem com fração não separa. Peso abaixo de 100 kg (em toneladas?) ou acima da carga máxima (o 16b) não
    entra no 18 — nem nos típicos: é erro de digitação.

Qualidade do dado (contados, não são sinal de risco — 1ª rodada: sem força nas referências; erro de digitação):
  - 16a, cubagem acima da capacidade do tipo (`capacidades` de `navii/referencias/tipos_conteiner.json`);
  - 16b, peso acima da carga máxima do tipo (com folga: o valor publicado é um exemplo da frota).
Descartados na 1ª rodada: pares raros carga × tipo e carga × embalagem (16c/16d: carga fora de gabarito legítima;
embalagem vazia ou "99" na maioria) e cidade no número do BL (17b: nenhum prefixo bate com a origem em 90%).

Unidade dos 16a/16b e 18: o contêiner CHEIO (FCL) na família de CEs (master e filhotes) — peso e cubagem do contêiner
pela regra das pesagens (`pesagens_ciclo.peso_da_familia`: o item do master; sem ele, a soma dos filhotes). Do 16e: os
itens do próprio CE. Contêiner com item de uso parcial (LCL) fica de fora. Números do Mercante: texto com ponto e 3
casas decimais ("25040.000" = 25.040 kg; 1ª rodada, 100% dos pesos).
"""
import json
import math
import os
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from virasana.analytic_scripts.ciclo_de_vida.pesagens_ciclo import peso_da_familia
from virasana.navii import referencias

OCUPACAO_IMPOSSIVEL = 1.10        # cubagem acima de 110% da capacidade do tipo
CARGA_ACIMA = 1.10                # peso acima de 110% da carga máxima publicada (exemplo da frota: há folga)
PESO_MINIMO = 100.0               # kg: declarado abaixo disto não é peso de carga (como nas pesagens; em toneladas?)
QUANTIDADE_DIFERENTE = 0.10       # 16e: pesos (ou volumes) que diferem mais de 10% (maior ÷ menor − 1)
PREFIXO_RARO = 0.01               # prefixo em menos de 1% dos BLs da empresa de navegação
PREFIXO_DOMINANTE = 0.90          # ... de uma empresa cujo prefixo comum cobre 90%+ dos BLs
MINIMO_DA_EMPRESA = 50            # BLs da empresa para o mapa valer
NIVEIS_NCM = (8, 6, 4)
AMOSTRAS_MINIMAS = 30             # contêineres de um NCM só para o típico daquele nível valer
DESVIOS = 3.0                     # |desvios robustos| a partir dos quais peso / densidade são atípicos
MAD_MINIMO = 0.05                 # log10 (~12%): NCM de valores repetidos não vira alarme por qualquer diferença
ESCALA_MAD = 1.4826               # MAD -> desvio padrão (normal)
USO_PARCIAL = frozenset({'S', 'SIM', '1', 'T', 'TRUE'})
TIPOS_BL_DA_COMPANHIA = ('10', '11')   # BL único e MBL: o número é da companhia
TAMANHOS = {'2': "20'", '4': "40'", 'L': "45'"}
PES_DO_APELIDO = {'20': '22', '40': '42', '45': 'L5'}

OCUPACAO, CARGA_MAXIMA, CUBAGEM_REPETIDA = '16a', '16b', '16e'
PREFIXO = '17a'
PESO_NCM, DENSIDADE_NCM = '18a', '18b'          # chaves dos típicos; o sinal leva a direção: '18a+', '18b-'...
QUALIDADE = {
    OCUPACAO: 'cubagem acima da capacidade do contêiner (qualidade do dado)',
    CARGA_MAXIMA: 'peso acima da carga máxima do contêiner (qualidade do dado)',
}
SINAIS = {
    CUBAGEM_REPETIDA: 'mesma cubagem em contêineres da mesma mercadoria com quantidades diferentes',
    PREFIXO: 'prefixo do BL raro para a empresa de navegação',
    PESO_NCM + '+': 'peso por contêiner ACIMA do típico do NCM',
    PESO_NCM + '-': 'peso por contêiner ABAIXO do típico do NCM',
    DENSIDADE_NCM + '+': 'densidade ACIMA da típica do NCM',
    DENSIDADE_NCM + '-': 'densidade ABAIXO da típica do NCM',
}
DESCRICOES = {**QUALIDADE, **SINAIS}


# ---------------------------------------------------------------------- números e textos do Mercante

def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def numero(valor) -> Optional[float]:
    """Número do Mercante (texto): '25040.000', '33,5', '1.234,5' ou 2350 -> float; vazio ou inválido -> None. Com ponto
    E vírgula, o ponto é o milhar; com um só deles, é a casa decimal (como `pesagens_ciclo._kg`)."""
    texto = _texto(valor)
    if ',' in texto and '.' in texto:
        texto = texto.replace('.', '')
    try:
        return float(texto.replace(',', '.')) if texto else None
    except ValueError:
        return None


def formato(valor) -> str:
    """Classe do texto numérico (o diagnóstico mede o preenchimento real)."""
    texto, valor_numerico = _texto(valor), numero(valor)
    if not texto:
        return 'vazio'
    if valor_numerico is None:
        return 'inválido'
    if valor_numerico <= 0:
        return 'zero' if valor_numerico == 0 else 'negativo'
    if ',' in texto and '.' in texto:
        return 'milhar e decimal'
    if re.fullmatch(r'\d+\.\d{3}', texto):
        return 'decimal com 3 casas (padrão do Mercante)'
    if ',' in texto or '.' in texto:
        return 'decimal com ' + ('vírgula' if ',' in texto else 'ponto')
    return 'inteiro'


def br(valor: Optional[float], casas: int = 0) -> str:
    """1234.5 -> '1.234,5' (casas=1); None -> '-'."""
    if valor is None:
        return '-'
    return f'{valor:,.{casas}f}'.replace(',', '#').replace('.', ',').replace('#', '.')


def pct(fracao: Optional[float], casas: int = 1) -> str:
    return '-' if fracao is None else f'{fracao * 100:.{casas}f}%'.replace('.', ',')


def uso_parcial(valor) -> bool:
    return _texto(valor).upper() in USO_PARCIAL


def ncm(valor) -> Optional[str]:
    """Os dígitos do NCM (até 8); menos de 4 dígitos não é NCM."""
    digitos = re.sub(r'\D', '', _texto(valor))
    return digitos[:8] if len(digitos) >= 4 else None


def faixa(valor: float, faixas: Sequence[Tuple[float, str]]) -> str:
    """Rótulo da última faixa cujo limite inferior é <= valor."""
    rotulo = faixas[0][1]
    for limite, nome in faixas:
        if valor >= limite:
            rotulo = nome
    return rotulo


# ---------------------------------------------------------------------- tipo e capacidade do contêiner

def chave_tipo(codigo) -> Optional[str]:
    """Tamanho + grupo do tipo informado: '22G1' -> '22G'; '2210' (numérico antigo) -> '22G'; '40HC' -> '45G'."""
    tipo = referencias.tipo_conteiner(codigo)
    if not tipo:
        return None
    limpo = tipo['codigo']
    if len(limpo) == 4 and ((limpo[2].isalpha() and limpo[3].isdigit()) or limpo[2:].isdigit()):
        return limpo[:2] + tipo['grupo']
    tamanho = PES_DO_APELIDO.get(limpo[:-2])
    if tamanho == '42' and limpo[-2:] in ('HC', 'HQ'):
        tamanho = '45'
    return tamanho + tipo['grupo'] if tamanho else None


@lru_cache(maxsize=1)
def _tipos() -> dict:
    caminho = os.path.join(os.path.dirname(referencias.__file__), 'tipos_conteiner.json')
    with open(caminho, encoding='utf-8') as arquivo:
        return json.load(arquivo)


def capacidade(chave: Optional[str]) -> Optional[dict]:
    """{'m3', 'carga_maxima_kg', 'fonte'} do tamanho + grupo ('22G'); None sem fonte."""
    return None if not chave or chave.startswith('_') else _tipos().get('capacidades', {}).get(chave)


def nome_tipo(codigo) -> str:
    tipo = referencias.tipo_conteiner(codigo)
    return tipo['curta'] if tipo else (_texto(codigo) or 'tipo não informado')


def ocupacao(cubagem: Optional[float], chave: Optional[str]) -> Optional[float]:
    """Cubagem ÷ capacidade do tipo; None sem uma delas."""
    volume = (capacidade(chave) or {}).get('m3')
    return cubagem / volume if cubagem and cubagem > 0 and volume else None


def carga(peso: Optional[float], chave: Optional[str]) -> Optional[float]:
    """Peso ÷ carga máxima do tipo; None sem uma delas."""
    maxima = (capacidade(chave) or {}).get('carga_maxima_kg')
    return peso / maxima if peso and peso > 0 and maxima else None


# ---------------------------------------------------------------------- o contêiner declarado

@dataclass(frozen=True)
class Conteiner:
    """O contêiner cheio numa família de CEs, com o que foi declarado para ele."""
    conteiner: str
    familia: str                        # o master do topo (ou o próprio CE, sem master)
    ces: Tuple[str, ...]
    tipo: Optional[str]                 # código informado (o 1º não vazio)
    chave: Optional[str]                # tamanho + grupo ('22G')
    peso: Optional[float]
    origem_peso: str
    cubagem: Optional[float]
    origem_cubagem: str
    ncms: Tuple[Tuple[str, str], ...]   # (NCM, embalagem) declarados para o contêiner na família
    embalagem: str                      # do item (o 1º não vazio)
    uso_parcial: bool

    @property
    def tamanho(self) -> Optional[str]:
        return self.chave[0] if self.chave and self.chave[0] in TAMANHOS else None

    @property
    def ncm_unico(self) -> Optional[str]:
        codigos = {codigo for codigo, _ in self.ncms}
        return codigos.pop() if len(codigos) == 1 else None


def raiz(ce: str, master_de) -> str:
    """O master do topo da família (MBL > MHBL > HBL); o próprio CE sem master. `master_de.get(CE)` -> master."""
    for _ in range(5):
        master = master_de.get(ce)
        if not master or master == ce:
            break
        ce = master
    return ce


def _ncms_por_item(ncms: Iterable[dict], conteineres_do_ce: Dict[str, Set[str]]) -> Dict[tuple, List[tuple]]:
    """{(CE, contêiner): [(NCM, embalagem)]} — o NCM sem contêiner vai para o único contêiner do CE."""
    por_item = defaultdict(list)
    for linha in ncms:
        codigo, conteiner = ncm(linha['ncm']), _texto(linha['conteiner']).upper()
        do_ce = conteineres_do_ce.get(linha['ce'], set())
        if not conteiner and len(do_ce) == 1:
            conteiner = next(iter(do_ce))
        if codigo and conteiner:
            por_item[(linha['ce'], conteiner)].append((codigo, _texto(linha['embalagem'])))
    return por_item


def _primeiro(itens: List[dict], campo: str) -> str:
    return next((_texto(item[campo]) for item in itens if _texto(item[campo])), '')


def _monta(familia: str, conteiner: str, itens: List[dict], ncms_do_item: Dict[tuple, List[tuple]],
           master_de) -> Conteiner:
    ces = tuple(sorted({item['ce'] for item in itens}))
    peso, origem_peso = peso_da_familia([(i['ce'], master_de.get(i['ce']), numero(i['peso'])) for i in itens])
    cubagem, origem = peso_da_familia([(i['ce'], master_de.get(i['ce']), numero(i['cubagem'])) for i in itens])
    tipo = _primeiro(itens, 'tipo') or None
    return Conteiner(
        conteiner=conteiner, familia=familia, ces=ces, tipo=tipo, chave=chave_tipo(tipo), peso=peso,
        origem_peso=origem_peso, cubagem=cubagem, origem_cubagem=origem.replace('sem peso', 'sem cubagem'),
        ncms=tuple(sorted({par for ce in ces for par in ncms_do_item.get((ce, conteiner), [])})),
        embalagem=_primeiro(itens, 'embalagem'), uso_parcial=any(uso_parcial(i['uso_parcial']) for i in itens))


def conteineres(itens: Sequence[dict], ncms: Iterable[dict], master_de) -> List[Conteiner]:
    """Um `Conteiner` por (família, contêiner).

    itens: {'ce', 'conteiner', 'tipo', 'peso', 'cubagem', 'embalagem', 'uso_parcial'} (itensresumo);
    ncms: {'ce', 'conteiner', 'ncm', 'embalagem'} (ncmitemresumo); master_de: {CE: master ou None} (ou um objeto
    com `get`). A família é o master do topo (`raiz`).
    """
    por_conteiner, conteineres_do_ce = defaultdict(list), defaultdict(set)
    for item in itens:
        conteiner = _texto(item['conteiner']).upper()
        if conteiner:
            por_conteiner[(raiz(item['ce'], master_de), conteiner)].append(item)
            conteineres_do_ce[item['ce']].add(conteiner)
    ncms_do_item = _ncms_por_item(ncms, conteineres_do_ce)
    return [_monta(familia, conteiner, do_conteiner, ncms_do_item, master_de)
            for (familia, conteiner), do_conteiner in sorted(por_conteiner.items())]


# ---------------------------------------------------------------------- 16a/16b: qualidade do dado

def sinais_capacidade(c: Conteiner) -> List[Tuple[str, str]]:
    """[(código, motivo)]: cubagem acima da capacidade; peso acima da carga máxima (com a folga de CARGA_ACIMA)."""
    cap = capacidade(c.chave)
    if not cap or c.uso_parcial:
        return []
    sinais = []
    ocupado, carregado = ocupacao(c.cubagem, c.chave), carga(c.peso, c.chave)
    if ocupado is not None and ocupado > OCUPACAO_IMPOSSIVEL:
        sinais.append((OCUPACAO, f'cubagem de {br(c.cubagem, 1)} m³ num {nome_tipo(c.tipo)} — capacidade de '
                                 f'{br(cap["m3"], 1)} m³ ({pct(ocupado, 0)})'))
    if carregado is not None and carregado > CARGA_ACIMA:
        sinais.append((CARGA_MAXIMA, f'{br(c.peso)} kg num {nome_tipo(c.tipo)} — carga máxima de '
                                     f'{br(cap["carga_maxima_kg"])} kg ({pct(carregado, 0)})'))
    return sinais


# ---------------------------------------------------------------------- 16e: cubagem copiada

def _declarados_do_ce(itens: Sequence[dict], ncms: Sequence[dict]) -> Dict[str, dict]:
    """{contêiner: {'cubagem', 'peso', 'volumes', 'ncms'}} dos contêineres cheios de UM CE (itens do mesmo contêiner
    somados). NCM sem contêiner vale para todos os contêineres do CE (a mesma mercadoria declarada no CE)."""
    por_conteiner = defaultdict(lambda: {'cubagem': 0.0, 'peso': 0.0, 'volumes': 0.0, 'ncms': set()})
    parciais = set()
    for item in itens:
        conteiner = _texto(item['conteiner']).upper()
        if conteiner and uso_parcial(item['uso_parcial']):
            parciais.add(conteiner)
        elif conteiner:
            declarado = por_conteiner[conteiner]
            declarado['cubagem'] += numero(item['cubagem']) or 0.0
            declarado['peso'] += numero(item['peso']) or 0.0
    sem_conteiner = {ncm(linha['ncm']) for linha in ncms if not _texto(linha['conteiner'])} - {None}
    for linha in ncms:
        conteiner = _texto(linha['conteiner']).upper()
        if conteiner in por_conteiner and ncm(linha['ncm']):
            por_conteiner[conteiner]['ncms'].add(ncm(linha['ncm']))
            por_conteiner[conteiner]['volumes'] += numero(linha.get('volumes')) or 0.0
    for conteiner, declarado in por_conteiner.items():
        declarado['ncms'] |= sem_conteiner
    return {c: d for c, d in por_conteiner.items() if c not in parciais}


@dataclass(frozen=True)
class Repeticao:
    """O maior grupo de contêineres do CE com a mesma mercadoria e a mesma cubagem, mas quantidades diferentes."""
    conteineres: int
    cubagem: float
    motivo: str


def _diferem(valores: List[float]) -> bool:
    positivos = [v for v in valores if v > 0]
    return len(positivos) >= 2 and max(positivos) > (1 + QUANTIDADE_DIFERENTE) * min(positivos)


def cubagem_repetida(itens: Sequence[dict], ncms: Sequence[dict]) -> Tuple[bool, Optional[Repeticao]]:
    """16e num CE: (avaliável, repetição). Avaliável = 2+ contêineres cheios com cubagem e NCM. Repetição quando 2+
    deles, com o mesmo conjunto de NCMs, têm a MESMA cubagem e pesos ou volumes diferentes (> QUANTIDADE_DIFERENTE).

    itens e ncms: as linhas do CE (como em `conteineres`; ncms com 'volumes' = `qtdeVolumes`)."""
    declarados = {c: d for c, d in _declarados_do_ce(itens, ncms).items() if d['cubagem'] > 0 and d['ncms']}
    grupos = defaultdict(list)
    for conteiner, declarado in declarados.items():
        grupos[(frozenset(declarado['ncms']), round(declarado['cubagem'], 3))].append(conteiner)
    achados = [(chave, sorted(lista)) for chave, lista in grupos.items() if len(lista) >= 2 and (
        _diferem([declarados[c]['peso'] for c in lista]) or _diferem([declarados[c]['volumes'] for c in lista]))]
    if not achados:
        return len(declarados) >= 2, None
    (ncms_do_grupo, cubagem), lista = max(achados, key=lambda achado: len(achado[1]))
    pesos = [declarados[c]['peso'] for c in lista]
    volumes = [declarados[c]['volumes'] for c in lista if declarados[c]['volumes'] > 0]
    texto_volumes = f'; volumes de {br(min(volumes))} a {br(max(volumes))}' if _diferem(volumes) else ''
    return True, Repeticao(len(lista), cubagem, (
        f'{len(lista)} contêineres com NCM {", ".join(sorted(ncms_do_grupo))} e a mesma cubagem ({br(cubagem, 1)} m³), '
        f'pesos de {br(min(pesos))} a {br(max(pesos))} kg{texto_volumes}'))


# ---------------------------------------------------------------------- 17a: número do BL

def limpa_bl(valor) -> str:
    return re.sub(r'[^A-Z0-9]', '', _texto(valor).upper())


def _letras_iniciais(numero_bl: str) -> int:
    return len(re.match(r'[A-Z]*', numero_bl).group())


def formato_bl(valor) -> str:
    """Classe do número do BL pelas letras iniciais."""
    numero_bl = limpa_bl(valor)
    letras = _letras_iniciais(numero_bl)
    if not numero_bl:
        return 'vazio'
    if letras == 0:
        return 'só dígitos' if numero_bl.isdigit() else 'começa com dígito'
    if letras == len(numero_bl):
        return 'só letras'
    if letras == 4:
        return '4 letras + dígitos' if numero_bl[4:].isdigit() else '4 letras + misto'
    return '7+ letras + resto' if letras >= 7 else f'{letras} letras + resto'


def prefixo_bl(valor) -> Optional[str]:
    """As 4 letras iniciais (tipo SCAC), quando o número começa com 4+ letras e tem dígito."""
    numero_bl = limpa_bl(valor)
    tem_digito = any(ch.isdigit() for ch in numero_bl)
    return numero_bl[:4] if _letras_iniciais(numero_bl) >= 4 and tem_digito else None


@dataclass(frozen=True)
class BL:
    ce: str
    tipo_bl: str
    empresa: str
    numero: str

    @property
    def da_companhia(self) -> bool:
        return self.tipo_bl in TIPOS_BL_DA_COMPANHIA

    @property
    def prefixo(self) -> Optional[str]:
        return prefixo_bl(self.numero)


def mapa_prefixos(bls: Iterable[BL]) -> Dict[str, Counter]:
    """{empresa: Counter(prefixo)} dos BLs da companhia ('' = sem prefixo)."""
    mapa = defaultdict(Counter)
    for bl in bls:
        if bl.da_companhia and bl.empresa:
            mapa[bl.empresa][bl.prefixo or ''] += 1
    return dict(mapa)


def sinais_bl(bl: BL, mapa: Dict[str, Counter]) -> List[Tuple[str, str]]:
    """[(17a, motivo)] quando o prefixo é raro numa empresa de prefixo dominante."""
    contagem = mapa.get(bl.empresa) or Counter()
    total = sum(contagem.values())
    if not bl.da_companhia or not bl.prefixo or total < MINIMO_DA_EMPRESA:
        return []
    comum, quantidade = contagem.most_common(1)[0]
    if quantidade < PREFIXO_DOMINANTE * total or contagem[bl.prefixo] >= PREFIXO_RARO * total:
        return []
    return [(PREFIXO, f'BL {bl.numero}: prefixo {bl.prefixo} em {pct(contagem[bl.prefixo] / total)} dos BLs da '
                      f'empresa {bl.empresa} (o comum: {comum or "sem prefixo"}, {pct(quantidade / total, 0)})')]


# ---------------------------------------------------------------------- 18: peso e densidade pelo NCM

@dataclass(frozen=True)
class Tipico:
    """O típico de um NCM (num tamanho de contêiner, para o peso): mediana e MAD em log10; p10 e p90 na unidade."""
    chave: str
    n: int
    mediana: float
    mad: float
    p10: float
    p90: float


def _quantil(ordenados: Sequence[float], q: float) -> float:
    return ordenados[min(len(ordenados) - 1, int(q * len(ordenados)))]


def tipico(valores: Sequence[float], chave: str = '') -> Optional[Tipico]:
    positivos = sorted(v for v in valores if v and v > 0)
    if not positivos:
        return None
    logs = [math.log10(v) for v in positivos]
    mediana = statistics.median(logs)
    mad = max(ESCALA_MAD * statistics.median(abs(x - mediana) for x in logs), MAD_MINIMO)
    return Tipico(chave, len(positivos), mediana, mad, _quantil(positivos, 0.1), _quantil(positivos, 0.9))


def tipicos(amostras: Dict[str, List[float]], minimo: int = AMOSTRAS_MINIMAS) -> Dict[str, Tipico]:
    """{chave: Tipico} das chaves com pelo menos `minimo` amostras."""
    return {chave: tipico(valores, chave) for chave, valores in amostras.items() if len(valores) >= minimo}


def desvios(valor: float, referencia: Tipico) -> float:
    return (math.log10(valor) - referencia.mediana) / referencia.mad


def densidade(c: Conteiner) -> Optional[float]:
    """Peso ÷ cubagem (kg/m³) — sem ela quando a cubagem é impossível para o tipo."""
    if not c.peso or c.peso <= 0 or not c.cubagem or c.cubagem <= 0:
        return None
    ocupado = ocupacao(c.cubagem, c.chave)
    return None if ocupado is not None and ocupado > OCUPACAO_IMPOSSIVEL else c.peso / c.cubagem


def peso_comparavel(c: Conteiner) -> bool:
    """De PESO_MINIMO até a carga máxima do tipo com a folga (acima, é o 16b: erro de digitação)."""
    if not c.peso or c.peso < PESO_MINIMO:
        return False
    carregado = carga(c.peso, c.chave)
    return carregado is None or carregado <= CARGA_ACIMA


def medidas_ncm(c: Conteiner) -> List[Tuple[str, str, float]]:
    """[(medida, chave, valor)] do contêiner FCL de um NCM só e de peso comparável, do nível mais específico (8) ao
    mais geral (4): peso por contêiner (18a) na chave 'tamanho|NCM' e densidade (18b) na chave 'NCM'."""
    unico = c.ncm_unico
    if not unico or c.uso_parcial or not peso_comparavel(c):
        return []
    niveis = [unico[:n] for n in NIVEIS_NCM if len(unico) >= n]
    medidas = []
    if c.tamanho:
        medidas += [(PESO_NCM, f'{c.tamanho}|{nivel}', c.peso) for nivel in niveis]
    valor = densidade(c)
    if valor:
        medidas += [(DENSIDADE_NCM, nivel, valor) for nivel in niveis]
    return medidas


def _descreve_chave(chave: str) -> str:
    if '|' in chave:
        tamanho, nivel = chave.split('|')
        return f'NCM {nivel} em contêiner de {TAMANHOS[tamanho]}'
    return f'NCM {chave}'


def desvios_ncm(c: Conteiner, tabelas: Dict[str, Dict[str, Tipico]]) -> Dict[str, Tuple[str, float, float]]:
    """{medida: (chave, valor, desvios)} pelo típico do nível mais específico que existe (18a peso, 18b densidade)."""
    achados = {}
    for medida, chave, valor in medidas_ncm(c):
        referencia = tabelas.get(medida, {}).get(chave)
        if medida not in achados and referencia is not None:
            achados[medida] = (chave, valor, desvios(valor, referencia))
    return achados


def sinais_ncm(c: Conteiner, tabelas: Dict[str, Dict[str, Tipico]]) -> List[Tuple[str, str]]:
    """[(sinal com direção, motivo)] quando o peso / a densidade fica a DESVIOS ou mais do típico do nível mais
    específico: '18a+' / '18a-' (peso), '18b+' / '18b-' (densidade)."""
    sinais = []
    for medida, (chave, valor, distancia) in desvios_ncm(c, tabelas).items():
        if abs(distancia) >= DESVIOS:
            referencia, unidade = tabelas[medida][chave], 'kg' if medida == PESO_NCM else 'kg/m³'
            sinais.append((medida + ('+' if distancia > 0 else '-'),
                           f'{br(valor)} {unidade}; típico do {_descreve_chave(chave)}: {br(referencia.p10)} a '
                           f'{br(referencia.p90)} {unidade} ({referencia.n} contêineres) — {br(distancia, 1)} desvios'))
    return sinais


def cubagem_redonda(cubagem: Optional[float]) -> bool:
    """Cubagem em metro cúbico inteiro (68,000): o valor estimado de praxe — a densidade dela é menos confiável."""
    return cubagem is not None and cubagem > 0 and abs(cubagem - round(cubagem)) < 1e-6


# ---------------------------------------------------------------------- validação

def vezes(marcados: int, total: int, marcados_base: int, total_base: int) -> Optional[float]:
    """Taxa de marcados no grupo ÷ taxa na base (população ou controle); None sem base."""
    if not total or not total_base or not marcados_base:
        return None
    return (marcados / total) / (marcados_base / total_base)
