# coding: utf-8
"""
SINAIS DE DECLARAÇÃO INVENTADA no Mercante — regras PURAS, sem banco (itens 16, 17 e 18 do `BACKLOG.md`; plano
aprovado pelo usuário em 23/10/2026: o passo 0 é o diagnóstico nos dados, `diagnostico_declaracao.py`). Se um sinal
valer, estas mesmas regras servem depois ao NAVI-I e/ou ao risco histórico.

  - 16, contêiner × cubagem × carga × embalagem: OCUPAÇÃO = cubagem declarada ÷ capacidade do tipo (`capacidades` de
    `navii/referencias/tipos_conteiner.json`, de fonte pública) — acima de 110%, impossível; PESO acima da carga
    máxima do tipo (com folga: o valor publicado é um exemplo da frota); PARES raros na própria base: para cada
    capítulo do NCM, o grupo do contêiner (dry, reefer, tanque...) e a embalagem pouco usados com aquela carga — são
    candidatos que o usuário revisa (reefer com máquina pode ser normal; tanque com tecido, não);
  - 17, número do BL × companhia × origem: o PREFIXO de 4 letras (tipo SCAC) do `numConhecimento` raro para a empresa
    de navegação do CE (mapa aprendido da base); a CIDADE logo depois do prefixo diferente do porto de origem — só nos
    prefixos em que ela costuma bater. Só BL (10) e MBL (11): no HBL o número é do agente, de formato livre;
  - 18, peso × volume × NCM: nos contêineres de UM NCM só, o PESO POR CONTÊINER (por tamanho) e a DENSIDADE (peso ÷
    cubagem) comparados com o típico do NCM, aprendido da base em escala log (mediana e MAD), no nível mais
    específico (8, 6 ou 4 dígitos) com amostras suficientes.

Unidade dos 16 e 18: o contêiner CHEIO (FCL) na família de CEs (master e filhotes) — peso e cubagem do contêiner pela
regra das pesagens (`pesagens_ciclo.peso_da_familia`: o item do master; sem ele, a soma dos filhotes). Contêiner com
item de uso parcial (LCL) fica de fora. Os limiares abaixo são as propostas do plano, a calibrar com o diagnóstico.
"""
import json
import math
import os
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

from virasana.analytic_scripts.ciclo_de_vida.pesagens_ciclo import peso_da_familia
from virasana.navii import referencias

OCUPACAO_IMPOSSIVEL = 1.10        # cubagem acima de 110% da capacidade do tipo
CARGA_ACIMA = 1.10                # peso acima de 110% da carga máxima publicada (exemplo da frota: há folga)
FRACAO_PAR_RARO = 0.002           # grupo do contêiner / embalagem em menos de 0,2% dos contêineres daquela carga
MINIMO_DO_CAPITULO = 500          # contêineres do capítulo para "raro" querer dizer algo
PREFIXO_RARO = 0.01               # prefixo em menos de 1% dos BLs da empresa de navegação
MINIMO_DA_EMPRESA = 50            # BLs da empresa para o mapa valer
CIDADE_BATE = 0.90                # prefixos em que a cidade do número bate com a origem em 90%+ dos BLs
NIVEIS_NCM = (8, 6, 4)
AMOSTRAS_MINIMAS = 30             # contêineres de um NCM só para o típico daquele nível valer
DESVIOS = 3.0                     # |desvios robustos| a partir dos quais peso / densidade são atípicos
MAD_MINIMO = 0.05                 # log10 (~12%): NCM de valores repetidos não vira alarme por qualquer diferença
ESCALA_MAD = 1.4826               # MAD -> desvio padrão (normal)
USO_PARCIAL = frozenset({'S', 'SIM', '1', 'T', 'TRUE'})
TIPOS_BL_DA_COMPANHIA = ('10', '11')   # BL único e MBL: o número é da companhia
TAMANHOS = {'2': "20'", '4': "40'", 'L': "45'"}
PES_DO_APELIDO = {'20': '22', '40': '42', '45': 'L5'}

OCUPACAO, CARGA_MAXIMA, TIPO_CARGA, EMBALAGEM_CARGA = '16a', '16b', '16c', '16d'
PREFIXO, CIDADE = '17a', '17b'
PESO_NCM, DENSIDADE_NCM = '18a', '18b'
SINAIS = {
    OCUPACAO: 'cubagem acima da capacidade do contêiner',
    CARGA_MAXIMA: 'peso acima da carga máxima do contêiner',
    TIPO_CARGA: 'tipo de contêiner raro para a carga',
    EMBALAGEM_CARGA: 'embalagem rara para a carga',
    PREFIXO: 'prefixo do BL raro para a empresa de navegação',
    CIDADE: 'cidade no número do BL diferente da origem',
    PESO_NCM: 'peso por contêiner atípico para o NCM',
    DENSIDADE_NCM: 'densidade atípica para o NCM',
}


# ---------------------------------------------------------------------- números e textos do Mercante

def _texto(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def numero(valor) -> Optional[float]:
    """Número do Mercante (texto): '33,5', '33.5', '1.234,5' ou 2350 -> float; vazio ou inválido -> None. Com ponto E
    vírgula, o ponto é o milhar; com um só deles, é a casa decimal (como `pesagens_ciclo._kg`)."""
    texto = _texto(valor)
    if ',' in texto and '.' in texto:
        texto = texto.replace('.', '')
    try:
        return float(texto.replace(',', '.')) if texto else None
    except ValueError:
        return None


def formato(valor) -> str:
    """Classe do texto numérico — o diagnóstico mede o preenchimento real (ponto com 3 casas pode ser milhar)."""
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
        return 'ponto com 3 casas (milhar?)'
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


def nome_grupo(grupo: Optional[str]) -> str:
    """'R' -> 'refrigerado (reefer)'."""
    dados = _tipos().get('grupos', {}).get(grupo or '') or _tipos().get('nao_iso', {}).get(grupo or '')
    return dados['nome'] if dados else (grupo or '?')


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
    familia: str                        # o master (ou o próprio CE, sem master)
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
    def grupo(self) -> Optional[str]:
        return self.chave[-1] if self.chave else None

    @property
    def tamanho(self) -> Optional[str]:
        return self.chave[0] if self.chave and self.chave[0] in TAMANHOS else None

    @property
    def ncm_unico(self) -> Optional[str]:
        codigos = {codigo for codigo, _ in self.ncms}
        return codigos.pop() if len(codigos) == 1 else None

    @property
    def capitulos(self) -> Set[str]:
        return {codigo[:2] for codigo, _ in self.ncms}


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
           master_de: Dict[str, Optional[str]]) -> Conteiner:
    ces = tuple(sorted({item['ce'] for item in itens}))
    peso, origem_peso = peso_da_familia([(i['ce'], master_de.get(i['ce']), numero(i['peso'])) for i in itens])
    cubagem, origem = peso_da_familia([(i['ce'], master_de.get(i['ce']), numero(i['cubagem'])) for i in itens])
    tipo = _primeiro(itens, 'tipo') or None
    return Conteiner(
        conteiner=conteiner, familia=familia, ces=ces, tipo=tipo, chave=chave_tipo(tipo), peso=peso,
        origem_peso=origem_peso, cubagem=cubagem, origem_cubagem=origem.replace('sem peso', 'sem cubagem'),
        ncms=tuple(sorted({par for ce in ces for par in ncms_do_item.get((ce, conteiner), [])})),
        embalagem=_primeiro(itens, 'embalagem'), uso_parcial=any(uso_parcial(i['uso_parcial']) for i in itens))


def conteineres(itens: Sequence[dict], ncms: Iterable[dict], master_de: Dict[str, Optional[str]]) -> List[Conteiner]:
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


# ---------------------------------------------------------------------- 16: capacidade e pares raros

def sinais_capacidade(c: Conteiner) -> List[Tuple[str, str]]:
    """[(sinal, motivo)]: cubagem acima da capacidade; peso acima da carga máxima (com a folga de CARGA_ACIMA)."""
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


def pares_tipo_carga(c: Conteiner) -> Set[Tuple[str, str]]:
    """{(capítulo, grupo do contêiner)} do contêiner FCL de tipo conhecido."""
    return {(capitulo, c.grupo) for capitulo in c.capitulos} if c.grupo and not c.uso_parcial else set()


def pares_embalagem_carga(c: Conteiner) -> Set[Tuple[str, str]]:
    """{(capítulo, embalagem)} de cada NCM com embalagem informada."""
    return {(codigo[:2], embalagem) for codigo, embalagem in c.ncms if embalagem} if not c.uso_parcial else set()


def pares_raros(contagens: Counter, fracao: float = FRACAO_PAR_RARO,
                minimo: int = MINIMO_DO_CAPITULO) -> FrozenSet[Tuple[str, str]]:
    """Pares (carga, valor) com menos de `fracao` dos contêineres daquela carga — só nas cargas com pelo menos
    `minimo` contêineres (com poucos, raro não quer dizer nada)."""
    totais = Counter()
    for (chave, _), quantidade in contagens.items():
        totais[chave] += quantidade
    return frozenset(par for par, quantidade in contagens.items()
                     if totais[par[0]] >= minimo and quantidade < fracao * totais[par[0]])


def descreve_carga(capitulo: str) -> str:
    descricao = referencias.descricao_posicao_ncm(capitulo) or ''
    return f'capítulo {capitulo}' + (f' ({descricao[:60]})' if descricao else '')


def sinais_pares(c: Conteiner, raros_tipo: FrozenSet, raros_embalagem: FrozenSet) -> List[Tuple[str, str]]:
    """[(sinal, motivo)] — um por sinal: o 1º par raro de cada (carga × tipo; carga × embalagem)."""
    tipo = [f'{nome_tipo(c.tipo)} com carga do {descreve_carga(capitulo)} — par raro na base'
            for capitulo, grupo in sorted(pares_tipo_carga(c)) if (capitulo, grupo) in raros_tipo]
    embalagem = [f'embalagem {codigo} com carga do {descreve_carga(capitulo)} — par raro na base'
                 for capitulo, codigo in sorted(pares_embalagem_carga(c)) if (capitulo, codigo) in raros_embalagem]
    return [(sinal, motivos[0]) for sinal, motivos in ((TIPO_CARGA, tipo), (EMBALAGEM_CARGA, embalagem)) if motivos]


# ---------------------------------------------------------------------- 17: número do BL

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
    if letras >= 7:
        return '7+ letras (prefixo + cidade?)'
    return f'{letras} letras + resto'


def prefixo_bl(valor) -> Optional[str]:
    """As 4 letras iniciais (tipo SCAC), quando o número começa com 4+ letras e tem dígito."""
    numero_bl = limpa_bl(valor)
    tem_digito = any(ch.isdigit() for ch in numero_bl)
    return numero_bl[:4] if _letras_iniciais(numero_bl) >= 4 and tem_digito else None


def cidade_bl(valor) -> Optional[str]:
    """As 3 letras logo depois do prefixo (código de cidade em algumas companhias), com 7+ letras iniciais."""
    numero_bl = limpa_bl(valor)
    return numero_bl[4:7] if prefixo_bl(numero_bl) and _letras_iniciais(numero_bl) >= 7 else None


def cidade_do_porto(locode) -> Optional[str]:
    """'CNSHA' (UN/LOCODE) -> 'SHA'."""
    limpo = re.sub(r'[^A-Z]', '', _texto(locode).upper())
    return limpo[2:5] if len(limpo) == 5 else None


@dataclass(frozen=True)
class BL:
    ce: str
    tipo_bl: str
    empresa: str
    numero: str
    origem: str

    @property
    def da_companhia(self) -> bool:
        return self.tipo_bl in TIPOS_BL_DA_COMPANHIA

    @property
    def prefixo(self) -> Optional[str]:
        return prefixo_bl(self.numero)

    @property
    def cidade(self) -> Optional[str]:
        return cidade_bl(self.numero)


def mapa_prefixos(bls: Iterable[BL]) -> Dict[str, Counter]:
    """{empresa: Counter(prefixo)} dos BLs da companhia ('' = sem prefixo)."""
    mapa = defaultdict(Counter)
    for bl in bls:
        if bl.da_companhia and bl.empresa:
            mapa[bl.empresa][bl.prefixo or ''] += 1
    return dict(mapa)


def taxas_cidade(bls: Iterable[BL]) -> Dict[str, Tuple[int, int]]:
    """{prefixo: (BLs em que a cidade do número bate com a origem, BLs com cidade e origem)}."""
    taxas = defaultdict(lambda: [0, 0])
    for bl in bls:
        origem = cidade_do_porto(bl.origem)
        if bl.da_companhia and bl.cidade and origem:
            taxas[bl.prefixo][0] += bl.cidade == origem
            taxas[bl.prefixo][1] += 1
    return {prefixo: (bate, total) for prefixo, (bate, total) in taxas.items()}


def prefixos_com_cidade(taxas: Dict[str, Tuple[int, int]], minimo: float = CIDADE_BATE,
                        amostras: int = AMOSTRAS_MINIMAS) -> FrozenSet[str]:
    return frozenset(p for p, (bate, total) in taxas.items() if total >= amostras and bate >= minimo * total)


def sinais_bl(bl: BL, mapa: Dict[str, Counter], prefixos_cidade: FrozenSet[str]) -> List[Tuple[str, str]]:
    if not bl.da_companhia:
        return []
    sinais = []
    contagem = mapa.get(bl.empresa) or Counter()
    total = sum(contagem.values())
    if bl.prefixo and total >= MINIMO_DA_EMPRESA and contagem[bl.prefixo] < PREFIXO_RARO * total:
        comum, quantidade = contagem.most_common(1)[0]
        sinais.append((PREFIXO, f'BL {bl.numero}: prefixo {bl.prefixo} em {pct(contagem[bl.prefixo] / total)} dos '
                                f'BLs da empresa {bl.empresa} (o comum: {comum or "sem prefixo"}, '
                                f'{pct(quantidade / total, 0)})'))
    origem = cidade_do_porto(bl.origem)
    if bl.prefixo in prefixos_cidade and bl.cidade and origem and bl.cidade != origem:
        sinais.append((CIDADE, f'BL {bl.numero}: o número indica {bl.cidade}; origem declarada {bl.origem}'))
    return sinais


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


def medidas_ncm(c: Conteiner) -> List[Tuple[str, str, float]]:
    """[(sinal, chave, valor)] do contêiner FCL de um NCM só, do nível mais específico (8) ao mais geral (4):
    peso por contêiner na chave 'tamanho|NCM' e densidade na chave 'NCM'."""
    unico = c.ncm_unico
    if not unico or c.uso_parcial:
        return []
    niveis = [unico[:n] for n in NIVEIS_NCM if len(unico) >= n]
    medidas = []
    if c.peso and c.peso > 0 and c.tamanho:
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


def sinais_ncm(c: Conteiner, tabelas: Dict[str, Dict[str, Tipico]]) -> List[Tuple[str, str]]:
    """[(sinal, motivo)] quando o peso / a densidade fica a DESVIOS ou mais do típico do nível mais específico."""
    sinais, vistos = [], set()
    for sinal, chave, valor in medidas_ncm(c):
        referencia = tabelas.get(sinal, {}).get(chave)
        if sinal in vistos or referencia is None:
            continue
        vistos.add(sinal)
        distancia = desvios(valor, referencia)
        if abs(distancia) >= DESVIOS:
            unidade = 'kg' if sinal == PESO_NCM else 'kg/m³'
            sinais.append((sinal, f'{br(valor)} {unidade}; típico do {_descreve_chave(chave)}: {br(referencia.p10)} a '
                                  f'{br(referencia.p90)} {unidade} ({referencia.n} contêineres) — '
                                  f'{br(distancia, 1)} desvios'))
    return sinais


# ---------------------------------------------------------------------- validação

def vezes(marcados: int, total: int, marcados_populacao: int, total_populacao: int) -> Optional[float]:
    """Taxa de marcados no grupo ÷ taxa na população; None sem base."""
    if not total or not total_populacao or not marcados_populacao:
        return None
    return (marcados / total) / (marcados_populacao / total_populacao)
