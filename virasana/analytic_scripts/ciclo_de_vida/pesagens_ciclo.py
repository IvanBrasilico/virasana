# coding: utf-8
"""
Pesagens (PesagemVeiculo da API Recintos) no Ciclo de Vida — regras PURAS, sem banco (plano aprovado em 20/10/2026:
CICLO_DE_VIDA.md, "Fora de Santos e pesagens"). Usadas pelo diagnóstico P0 (`diagnostico_pesagens.py`) e, depois,
pela captura (P1) e pelo diagnóstico da divergência (P2).

  - divisão da balança: a maior divisão (kg) de que quase todos os pesos do recinto são múltiplos — balança
    rodoviária 10/20/50/70 kg, RTG 50/100 kg (usuário). Peso fora da divisão = valor digitado, não lido da balança
    como manda o SICA;
  - peso do contêiner: conjunto carregado − conjunto sem o contêiner (mesmas placas); no RTG, o peso já é o do
    contêiner. Base de comparação (usuário, 20/10/2026): medido − tara do contêiner = `Item.pesoBruto` (Mercante);
  - padrões de digitação entre declarado e medido: ordem de grandeza (×10ⁿ), dígito omitido ou a mais, dígitos
    vizinhos trocados. Um dígito SUBSTITUÍDO por outro (25100 x 28100) não entra: não se distingue de uma diferença
    real;
  - classificação prévia: coerente, digitação provável, dois contêineres no conjunto provável, divergência relevante,
    sem dado. Tolerâncias PROVISÓRIAS, a calibrar com o P0.
"""
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

DIVISOES = (100, 70, 50, 20, 10, 5, 2, 1)     # kg, da maior para a menor
PROPORCAO_DIVISAO = 0.98                       # dos pesos múltiplos da divisão
TARA_TIPICA = {'2': 2200.0, '4': 3800.0, 'L': 4500.0}   # 1º caractere do código ISO: 20', 40', 45'
TOLERANCIA_RELATIVA = 0.05                     # provisória (P0 calibra)
TOLERANCIA_ABSOLUTA = 300.0                    # kg: tara do conjunto (combustível, motorista) e resolução
DOIS_CONTEINERES = (1.6, 2.4)                  # medido / declarado, contêiner de 20'

UM_DIGITO = 'um dígito a mais ou a menos'
COERENTE = 'coerente'
DIGITACAO = 'digitação provável'
DOIS = 'dois contêineres no conjunto (provável)'
DIVERGENCIA = 'divergência relevante'
SEM_DADO = 'sem dado'


def _multiplo(peso: float, divisao: int) -> bool:
    return abs(peso / divisao - round(peso / divisao)) < 1e-6


def divisao_da_balanca(pesos: Iterable[float]) -> Optional[int]:
    """A maior divisão de DIVISOES de que >= PROPORCAO_DIVISAO dos pesos (> 0) são múltiplos; None sem pesos ou com
    pesos fracionados (nem o kg inteiro alcança a proporção)."""
    pesos = [float(p) for p in pesos if p and float(p) > 0]
    if not pesos:
        return None
    contagens = {d: sum(1 for p in pesos if _multiplo(p, d)) for d in DIVISOES}
    return divisao_pelas_contagens(contagens, len(pesos))


def divisao_pelas_contagens(contagens: Dict[int, int], total: int) -> Optional[int]:
    """A mesma regra a partir das contagens de múltiplos por divisão (o diagnóstico conta no banco)."""
    if not total:
        return None
    for divisao in DIVISOES:
        if contagens.get(divisao, 0) >= PROPORCAO_DIVISAO * total:
            return divisao
    return None


def fora_da_divisao(peso: Optional[float], divisao: Optional[int]) -> bool:
    """Peso que a balança daquela divisão não mostraria: foi digitado (na de 1 kg, o peso fracionado)."""
    return bool(peso) and bool(divisao) and not _multiplo(float(peso), divisao)


def tara_do_conteiner(tara_informada, tipo_iso: Optional[str]) -> Tuple[Optional[float], str]:
    """(tara, origem): a informada (Mercante/desembarque) quando plausível (1 a 6 t); senão a típica do tamanho
    (1º caractere do código ISO); senão (None, 'sem tara')."""
    try:
        tara = float(str(tara_informada).replace(',', '.')) if tara_informada not in (None, '') else None
    except ValueError:
        tara = None
    if tara and 1000 <= tara <= 6000:
        return tara, 'informada'
    tipica = TARA_TIPICA.get((tipo_iso or ' ')[0].upper())
    return (tipica, 'típica do tamanho') if tipica else (None, 'sem tara')


def _variantes(valor: int) -> Iterator[Tuple[int, str]]:
    """Valores que um erro de digitação faria a partir de `valor` (kg inteiros), com o nome do padrão."""
    for fator, nome in ((10, '×10'), (100, '×100'), (1000, '×1000')):
        yield valor * fator, f'ordem de grandeza ({nome})'
        if valor // fator:
            yield valor // fator, f'ordem de grandeza ({nome.replace("×", "÷")})'
    texto = str(valor)
    for posicao in range(len(texto)):
        if len(texto) > 1:
            yield int(texto[:posicao] + texto[posicao + 1:] or 0), UM_DIGITO
    for posicao in range(len(texto) + 1):
        for digito in '0123456789':
            novo = texto[:posicao] + digito + texto[posicao:]
            if novo[0] != '0':
                yield int(novo), UM_DIGITO
    for posicao in range(len(texto) - 1):
        if texto[posicao] != texto[posicao + 1]:
            yield int(texto[:posicao] + texto[posicao + 1] + texto[posicao] + texto[posicao + 2:]), \
                'dígitos vizinhos trocados'


def tolerancia(referencia: float, divisao: Optional[int] = None) -> float:
    return max(TOLERANCIA_ABSOLUTA, TOLERANCIA_RELATIVA * abs(referencia)) + 2 * (divisao or 0)


def padrao_digitacao(declarado: float, medido: float, divisao: Optional[int] = None) -> Optional[str]:
    """O padrão de digitação que leva o declarado ao medido (ou o medido ao declarado), dentro da tolerância; None
    se nenhum explica ou se os dois já são coerentes. A tolerância relativa vale sobre o valor transformado (×10 de
    2.510 ≈ 25.100); fora da tolerância, a variante dentro da dela já fica mais perto que o original."""
    if not declarado or not medido or abs(declarado - medido) <= tolerancia(declarado, divisao):
        return None             # já coerentes: nenhuma digitação a explicar
    for origem, alvo in ((declarado, medido), (medido, declarado)):
        for variante, nome in _variantes(int(round(origem))):
            if variante and abs(variante - alvo) <= tolerancia(variante, divisao):
                return nome
    return None


def classifica(declarado: Optional[float], medido: Optional[float], divisao: Optional[int] = None,
               tipo_iso: Optional[str] = None) -> Tuple[str, str]:
    """(categoria, detalhe) da comparação medido (já sem a tara do contêiner) x declarado (`Item.pesoBruto`)."""
    if not declarado or not medido or medido <= 0:
        return SEM_DADO, 'sem peso declarado ou medido'
    diferenca = medido - declarado
    percentual = 100 * diferenca / declarado
    if abs(diferenca) <= tolerancia(declarado, divisao):
        return COERENTE, f'{percentual:+.1f}%'
    padrao = padrao_digitacao(declarado, medido, divisao)
    if padrao:
        return DIGITACAO, f'{padrao}; {percentual:+.0f}%'
    razao = medido / declarado
    if (tipo_iso or ' ')[0] == '2' and DOIS_CONTEINERES[0] <= razao <= DOIS_CONTEINERES[1]:
        return DOIS, f'medido {razao:.1f}× o declarado'
    return DIVERGENCIA, f'{percentual:+.0f}%'


def pareia_tara(carregada: dict, candidatas: List[dict], horas: float) -> Optional[dict]:
    """A pesagem do MESMO conjunto sem contêiner mais próxima da carregada (mesmo recinto, até `horas`): mesma placa
    e, se as duas informam o semirreboque, o mesmo semirreboque. Pesagens como dicts com data, recinto, placa,
    semirreboque, conteiner e peso."""
    melhor = None
    for candidata in candidatas:
        if candidata['conteiner'] or candidata['placa'] != carregada['placa'] or \
                candidata['recinto'] != carregada['recinto'] or not candidata['peso']:
            continue
        if carregada['semirreboque'] and candidata['semirreboque'] and \
                candidata['semirreboque'] != carregada['semirreboque']:
            continue
        distancia = abs((candidata['data'] - carregada['data']).total_seconds()) / 3600
        if distancia <= horas and (melhor is None or distancia < melhor[0]):
            melhor = (distancia, candidata)
    return melhor[1] if melhor else None
