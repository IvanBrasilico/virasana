# coding: utf-8
"""
Pesagens (PesagemVeiculo da API Recintos) no Ciclo de Vida — regras PURAS, sem banco (plano aprovado em 20/10/2026:
CICLO_DE_VIDA.md, "Fora de Santos e pesagens"). Usadas pelo diagnóstico (`diagnostico_pesagens.py`) e, depois, pela
captura (P1) e pelo diagnóstico da divergência (P2).

  - divisão da balança: a maior divisão (kg) de que quase todos os pesos do recinto são múltiplos — balança
    rodoviária 10/20/50/70 kg, RTG 50/100 kg (usuário). Peso fora da divisão = valor digitado, não lido da balança
    como manda o SICA (P0: ≤ 1% em todos os recintos — fica como marca);
  - peso do contêiner: conjunto carregado − conjunto sem o contêiner (mesmas placas); no RTG, o peso já é o do
    contêiner. Base de comparação (usuário, 20/10/2026): medido − tara do contêiner = `Item.pesoBruto` (Mercante);
  - tara do conjunto (P0b, usuário aprovou em 20/10/2026): o par na visita; senão a pesagem vazia do conjunto em 7 dias;
    senão a `taraConjunto` da API (P0: mediana 0 kg do par, 92% a menos de 500 kg) — sempre com a origem, e só entre 8
    e 30 t (P0b, `pesagens2.log`: em Suape a `taraConjunto` é a tara do CONTÊINER, ~4,6 t; em Pecém há "pares" de
    52 a 61 t e "vazia" de 4,4 t; o par tem p10 de 11,8 t);
  - peso declarado do CONTÊINER (P0b): o item do master (o contêiner inteiro); sem ele, a soma dos filhotes (carga
    consolidada: cada filhote só tem a sua parte); um CE só, o item dele;
  - mesma operação (P0b): a pesagem que vem depois de um DESEMBARQUE do contêiner — e, pela direção do acesso da
    mesma placa (P0b, `pesagens2.log`): no operador do desembarque, a SAÍDA é a retirada da importação e a ENTRADA, o
    contêiner voltando (exportação, devolução); noutro recinto, o recinto SECO é a chegada da importação (84 a 92%
    coerente) e outro operador portuário, exportação ou baldeação (24 a 36% coerente). Sem desembarque antes, outra
    operação;
  - na visita, a pesagem CARREGADA é a mais pesada: a outra do mesmo contêiner (o caminhão chegando para retirar, com o
    número já informado) é a do conjunto sem ele — e serve de tara;
  - classificação: coerente; diferença moderada (até 20%); contêiner vazio na pesagem (medido ≈ 0); pesagem sem o
    contêiner no conjunto (medido ≈ − tara do contêiner); declarado inválido (< 100 kg); digitação provável (acima de
    20%: ordem de grandeza, dígito a mais ou a menos, vizinhos trocados — um dígito SUBSTITUÍDO não entra, não se
    distingue de diferença real); dois contêineres no conjunto (20'); divergência relevante; sem dado.
"""
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

DIVISOES = (100, 70, 50, 20, 10, 5, 2, 1)     # kg, da maior para a menor
PROPORCAO_DIVISAO = 0.98                       # dos pesos múltiplos da divisão
TARA_TIPICA = {'2': 2200.0, '4': 3800.0, 'L': 4500.0}   # 1º caractere do código ISO: 20', 40', 45'
TOLERANCIA_RELATIVA = 0.05                     # coerente até 5% ...
TOLERANCIA_ABSOLUTA = 300.0                    # ... ou 300 kg (tara do conjunto: combustível, motorista)
LIMITE_MODERADA = 0.20                         # até 20%: diferença moderada; a digitação só é procurada acima
VAZIO_ATE = 800.0                              # |medido| até isto: o contêiner estava vazio na pesagem
DECLARADO_MINIMO = 100.0                       # kg: declarado abaixo disto não é peso de carga (ex.: 1 kg)
DIFERENCA_MINIMA_PAR = 1000.0                  # kg: com e sem o contêiner (o mais leve vazio tem ~1,8 t)
TARA_CONJUNTO_PLAUSIVEL = (8000.0, 30000.0)    # kg: tara do conjunto (par, vazia ou API) fora disto é descartada
DOIS_CONTEINERES = (1.6, 2.4)                  # medido / declarado, contêiner de 20'
DIAS_OPERACAO = 60                             # desembarque até N dias antes da pesagem: a mesma operação

UM_DIGITO = 'um dígito a mais ou a menos'
COERENTE = 'coerente'
MODERADA = 'diferença moderada (até 20%)'
DIGITACAO = 'digitação provável'
DOIS = 'dois contêineres no conjunto (provável)'
DIVERGENCIA = 'divergência relevante'
VAZIO_NA_PESAGEM = 'contêiner vazio na pesagem'
SEM_CONTEINER = 'pesagem sem o contêiner no conjunto'
DECLARADO_INVALIDO = 'declarado inválido (< 100 kg)'
SEM_DADO = 'sem dado'
COMPARAVEIS = (COERENTE, MODERADA, DIGITACAO, DOIS, DIVERGENCIA)

RETIRADA = 'saída do operador do desembarque (retirada)'
OPERADOR_SEM_ACESSO = 'no operador do desembarque, sem acesso da placa'
VOLTA_AO_OPERADOR = 'entrada no operador do desembarque (exportação, devolução)'
CHEGADA_AO_SECO = 'chegada a recinto seco depois do desembarque'
OUTRO_OPERADOR = 'em outro operador portuário (exportação, baldeação)'
SEM_DESEMBARQUE = 'sem desembarque antes (outra operação)'
OPERACOES_DA_IMPORTACAO = (RETIRADA, OPERADOR_SEM_ACESSO, CHEGADA_AO_SECO)   # as que se comparam com o declarado

PAR, VAZIA, API = 'par na visita', 'vazia em 7 dias', 'taraConjunto da API'


def _multiplo(peso: float, divisao: int) -> bool:
    return abs(peso / divisao - round(peso / divisao)) < 1e-6


def _kg(valor) -> Optional[float]:
    """Peso do Mercante ('2.350' ou '2350,5' vêm como texto): float ou None."""
    try:
        return float(str(valor).replace(',', '.')) if valor not in (None, '') else None
    except ValueError:
        return None


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
    tara = _kg(tara_informada)
    if tara and 1000 <= tara <= 6000:
        return tara, 'informada'
    tipica = TARA_TIPICA.get((tipo_iso or ' ')[0].upper())
    return (tipica, 'típica do tamanho') if tipica else (None, 'sem tara')


def tara_do_conjunto(par: Optional[float], vazia: Optional[float], api: Optional[float]) -> Tuple[Optional[float], str]:
    """(tara do conjunto, origem): o par na visita; a pesagem vazia do conjunto em 7 dias; a `taraConjunto` da API — a
    primeira plausível (TARA_CONJUNTO_PLAUSIVEL); senão (None, 'sem tara')."""
    for tara, origem in ((par, PAR), (vazia, VAZIA), (api, API)):
        if tara and TARA_CONJUNTO_PLAUSIVEL[0] <= tara <= TARA_CONJUNTO_PLAUSIVEL[1]:
            return tara, origem
    return None, 'sem tara'


def peso_da_familia(itens: Sequence[Tuple[str, Optional[str], object]]) -> Tuple[Optional[float], str]:
    """(peso declarado do CONTÊINER, origem) pelos itens [(CE, master do CE, pesoBruto)] da MESMA família (master e
    filhotes) que têm o contêiner: o item do master; sem ele, a soma dos filhotes; um CE só, o item dele."""
    pesos = [(ce, master, _kg(peso)) for ce, master, peso in itens if _kg(peso) is not None]
    masters = {master for _, master, _ in pesos if master}
    do_master = [peso for ce, master, peso in pesos if not master and ce in masters]
    if do_master:
        return do_master[0], 'item do master'
    filhotes = [peso for _, master, peso in pesos if master]
    if len(filhotes) > 1:
        return sum(filhotes), f'soma de {len(filhotes)} filhotes'
    if pesos:
        return pesos[0][2], 'item do filhote' if filhotes else 'item do CE'
    return None, 'sem peso'


def contexto_da_pesagem(data: datetime, recinto: str, movimentos: Iterable[Tuple[datetime, str, bool]],
                        operadores: Iterable[str] = (), direcao: Optional[str] = None,
                        dias: int = DIAS_OPERACAO) -> str:
    """A operação da pesagem pelo último movimento do contêiner [(data, recinto, é desembarque)] até `dias` antes e pela
    `direcao` ('E'/'S') do acesso da mesma placa: no operador do desembarque, a saída (retirada), a entrada (o
    contêiner voltando) ou sem acesso; noutro recinto, recinto seco (chegada) ou outro dos `operadores` portuários;
    sem desembarque antes (o último foi embarque, ou nada), outra operação."""
    antes = [m for m in movimentos if data - timedelta(days=dias) <= m[0] <= data]
    if not antes:
        return SEM_DESEMBARQUE
    _, recinto_movimento, desembarque = max(antes, key=lambda m: m[0])
    if not desembarque:
        return SEM_DESEMBARQUE
    if recinto_movimento != recinto:
        return OUTRO_OPERADOR if recinto in set(operadores) else CHEGADA_AO_SECO
    return {'S': RETIRADA, 'E': VOLTA_AO_OPERADOR}.get(direcao, OPERADOR_SEM_ACESSO)


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
    se nenhum explica ou se a diferença não passa de LIMITE_MODERADA (P0: com a tolerância de 5%, os padrões
    "explicavam" diferenças de 7 a 9%). A tolerância vale sobre o valor transformado (×10 de 2.510 ≈ 25.100)."""
    if not declarado or not medido or abs(declarado - medido) <= LIMITE_MODERADA * abs(declarado):
        return None
    for origem, alvo in ((declarado, medido), (medido, declarado)):
        for variante, nome in _variantes(int(round(origem))):
            if variante and abs(variante - alvo) <= tolerancia(variante, divisao):
                return nome
    return None


def _fora_da_comparacao(declarado: float, medido: float) -> Optional[Tuple[str, str]]:
    """Os casos em que o medido não é peso de carga: declarado inválido, contêiner vazio, sem o contêiner."""
    if declarado < DECLARADO_MINIMO:
        return DECLARADO_INVALIDO, f'declarado {declarado:.0f} kg'
    if medido < -VAZIO_ATE:
        return SEM_CONTEINER, f'medido {medido:.0f} kg: o conjunto estava sem o contêiner'
    if abs(medido) <= VAZIO_ATE:
        return VAZIO_NA_PESAGEM, f'medido {medido:.0f} kg x declarado {declarado:.0f} kg'
    return None


def classifica(declarado: Optional[float], medido: Optional[float], divisao: Optional[int] = None,
               tipo_iso: Optional[str] = None) -> Tuple[str, str]:
    """(categoria, detalhe) da comparação medido (já sem a tara do contêiner) x declarado (peso do contêiner)."""
    if not declarado or medido is None:
        return SEM_DADO, 'sem peso declarado ou medido'
    diferenca = medido - declarado
    percentual = 100 * diferenca / declarado
    if abs(diferenca) <= tolerancia(declarado, divisao) and declarado >= DECLARADO_MINIMO:
        return COERENTE, f'{percentual:+.1f}%'
    fora = _fora_da_comparacao(declarado, medido)
    if fora:
        return fora
    if abs(diferenca) <= LIMITE_MODERADA * declarado:
        return MODERADA, f'{percentual:+.1f}%'
    padrao = padrao_digitacao(declarado, medido, divisao)
    if padrao:
        return DIGITACAO, f'{padrao}; {percentual:+.0f}%'
    razao = medido / declarado
    if (tipo_iso or ' ')[0] == '2' and DOIS_CONTEINERES[0] <= razao <= DOIS_CONTEINERES[1]:
        return DOIS, f'medido {razao:.1f}× o declarado'
    return DIVERGENCIA, f'{percentual:+.0f}%'


def _mesma_visita(candidata: dict, carregada: dict, horas: float) -> bool:
    return (candidata['placa'] == carregada['placa'] and candidata['recinto'] == carregada['recinto']
            and abs((candidata['data'] - carregada['data']).total_seconds()) <= horas * 3600)


def pareia_tara(carregada: dict, candidatas: List[dict], horas: float) -> Optional[dict]:
    """A pesagem do MESMO conjunto sem o contêiner mais próxima da carregada (mesmo recinto, até `horas`): mesma placa;
    se as duas informam o semirreboque, o mesmo; sem contêiner — ou com o MESMO contêiner informado e mais leve em pelo
    menos DIFERENCA_MINIMA_PAR (o caminhão chegando para retirar já com o número). Pesagens como dicts com data,
    recinto, placa, semirreboque, conteiner e peso."""
    melhor = None
    for candidata in candidatas:
        if not candidata['peso'] or not _mesma_visita(candidata, carregada, horas):
            continue
        if candidata['conteiner'] and (candidata['conteiner'] != carregada['conteiner'] or
                                       candidata['peso'] > (carregada['peso'] or 0) - DIFERENCA_MINIMA_PAR):
            continue
        if carregada['semirreboque'] and candidata['semirreboque'] and \
                candidata['semirreboque'] != carregada['semirreboque']:
            continue
        distancia = abs((candidata['data'] - carregada['data']).total_seconds()) / 3600
        if melhor is None or distancia < melhor[0]:
            melhor = (distancia, candidata)
    return melhor[1] if melhor else None


def e_a_carregada(pesagem: dict, candidatas: List[dict], horas: float) -> bool:
    """False se na mesma visita há outra pesagem do MESMO contêiner mais pesada em DIFERENCA_MINIMA_PAR: esta é a do
    conjunto sem ele (chegando para retirar, ou saindo depois de deixar)."""
    return not any(c['conteiner'] == pesagem['conteiner'] and c['peso'] and
                   c['peso'] >= (pesagem['peso'] or 0) + DIFERENCA_MINIMA_PAR and _mesma_visita(c, pesagem, horas)
                   for c in candidatas)
