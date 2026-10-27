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

P1/P2 (fim do arquivo; usuário, 20/10/2026: "implementação total das pesagens", sem código por recinto — o recinto
transmite como manda a especificação, e o erro é dele): as pesagens de um CICLO. A etapa sai do próprio ciclo, não da
placa: a pesagem até 30 min da SAÍDA do operador do desembarque é a retirada; até 30 min da ENTRADA no seco, a
chegada (P0c: 71% e 88% coerentes; a volta ao operador, 23%). As outras do contêiner dentro do ciclo ficam como dado
bruto. Tara do conjunto: o par na visita ou a `taraConjunto` (8–30 t) — a "vazia em 7 dias" ficou de fora (P0c: 43
casos, 53% coerente, e a consulta por placa de 7 dias é cara). A PRINCIPAL é a primeira comparada com classificação;
a outra comparada CONFIRMA ou não o medido (P2).
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from virasana.analytic_scripts.ciclo_de_vida import linha_do_tempo as ldt

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


# ---------------------------------------------------------------------- P1/P2: as pesagens de um ciclo

JANELA_ETAPA_MINUTOS = 30          # pesagem até isto da saída do operador / da entrada no seco: a da etapa
HORAS_PAR = 12                     # par na mesma visita
OUTRA = 'outra pesagem no ciclo'
DIAGNOSTICOS = {                   # classificação -> código estável (filtro do NAVI-I, tabela)
    COERENTE: 'coerente', MODERADA: 'moderada', DIGITACAO: 'digitacao', DOIS: 'dois',
    DIVERGENCIA: 'divergencia', VAZIO_NA_PESAGEM: 'vazio', SEM_CONTEINER: 'sem_conteiner',
    DECLARADO_INVALIDO: 'declarado_invalido', SEM_DADO: 'sem_dado',
}
CONFIRMADA = 'confirmada pela outra pesagem do ciclo'


@dataclass(frozen=True)
class Etapas:
    """O que as pesagens precisam do ciclo: operador e data do desembarque, saída do operador, entrada no seco e
    o fim do ciclo (None: em aberto)."""
    recinto_desembarque: str
    desembarque: datetime
    saida: Optional[datetime] = None
    recinto_seco: Optional[str] = None
    entrada_seco: Optional[datetime] = None
    fim: Optional[datetime] = None


def etapas_do_ciclo(ciclo: ldt.CicloCalculado) -> Etapas:
    """As etapas de um ciclo calculado — só as ligações gravadas (confiança alta ou média). Fim: a saída do seco, o
    embarque ou, sem seco, a saída do operador; sem nenhum, o ciclo está em aberto."""
    def data(ligacao: Optional[ldt.Ligacao]) -> Optional[datetime]:
        return ligacao.evento.data if ligacao is not None and ligacao.confianca != ldt.BAIXA else None
    seco = ciclo.entrada_seco if data(ciclo.entrada_seco) else None
    saida = data(ciclo.saida)
    fim = data(ciclo.saida_seco) or data(ciclo.embarque) or (saida if seco is None else None)
    return Etapas(ciclo.desembarque.recinto, ciclo.desembarque.data, saida, seco.evento.recinto if seco else None,
                  seco.evento.data if seco else None, fim)


def operacao_da_pesagem(pesagem: dict, etapas: Etapas, janela_minutos: float = JANELA_ETAPA_MINUTOS) -> Optional[str]:
    """RETIRADA (perto da saída, no operador do desembarque), CHEGADA_AO_SECO (perto da entrada, no seco), OUTRA
    (dentro do ciclo) ou None (fora do ciclo: antes do desembarque ou depois do fim)."""
    janela = timedelta(minutes=janela_minutos)
    if pesagem['data'] < etapas.desembarque or (etapas.fim is not None and pesagem['data'] > etapas.fim + janela):
        return None

    def perto(data: Optional[datetime]) -> bool:
        return data is not None and abs(pesagem['data'] - data) <= janela
    if pesagem['recinto'] == etapas.recinto_desembarque and perto(etapas.saida):
        return RETIRADA
    if etapas.recinto_seco and pesagem['recinto'] == etapas.recinto_seco and perto(etapas.entrada_seco):
        return CHEGADA_AO_SECO
    return OUTRA


@dataclass(frozen=True)
class Declarado:
    """O peso declarado do contêiner na família de CEs, a tara do contêiner e o tipo ISO."""
    ce: Optional[str]
    peso: Optional[float]
    origem: str
    tara_conteiner: Optional[float]
    origem_tara_conteiner: str
    tipo_iso: Optional[str] = None


def compara(pesagem: dict, candidatas: List[dict], declarado: Declarado, divisao: Optional[int] = None) -> dict:
    """Os campos da comparação de uma pesagem CARREGADA: tara do conjunto (par na visita ou `taraConjunto`), medido,
    diferença e classificação."""
    par = pareia_tara(pesagem, candidatas, HORAS_PAR)
    conjunto, origem_tara = tara_do_conjunto(par['peso'] if par else None, None, pesagem.get('tara_conjunto'))
    campos = {'tara_pesagem_id': par['id'] if par and conjunto == par['peso'] else None,
              'tara_conjunto': conjunto, 'origem_tara_conjunto': origem_tara,
              'tara_conteiner': declarado.tara_conteiner, 'origem_tara_conteiner': declarado.origem_tara_conteiner,
              'peso_declarado': declarado.peso, 'origem_declarado': declarado.origem,
              'numeroCEmercante': declarado.ce, 'divisao': divisao,
              'fora_da_divisao': fora_da_divisao(pesagem['peso'], divisao)}
    if conjunto is None or not pesagem['peso'] or declarado.peso is None:
        motivo = 'sem tara do conjunto' if conjunto is None else 'sem peso declarado ou pesado'
        return {**campos, 'classificacao': SEM_DADO, 'detalhe': motivo}
    medido = pesagem['peso'] - conjunto - (declarado.tara_conteiner or 0)
    categoria, detalhe = classifica(declarado.peso, medido, divisao, declarado.tipo_iso)
    diferenca = medido - declarado.peso
    return {**campos, 'peso_medido': medido, 'diferenca_kg': diferenca, 'classificacao': categoria,
            'detalhe': detalhe, 'diferenca_pct': 100 * diferenca / declarado.peso if declarado.peso else None,
            'razao': medido / declarado.peso if declarado.peso else None}


def _linha_base(pesagem: dict, operacao: str) -> dict:
    return {'pesagem_id': pesagem['id'], 'data': pesagem['data'], 'codigoRecinto': pesagem['recinto'],
            'placa': pesagem['placa'], 'semirreboque': pesagem['semirreboque'], 'peso_bruto': pesagem['peso'],
            'captura_automatica': pesagem.get('captura'), 'operacao': operacao, 'principal': False}


def linhas_do_ciclo(etapas: Etapas, pesagens: List[dict], candidatas: List[dict], declarado: Declarado,
                    divisoes: Optional[Dict[str, int]] = None) -> List[dict]:
    """As pesagens do contêiner dentro do ciclo (ordem de data), comparadas as carregadas da retirada e da chegada ao
    seco; marca a PRINCIPAL e a confirmação (P2). `candidatas`: as pesagens das mesmas placas (para o par)."""
    divisoes = divisoes or {}
    linhas = []
    for pesagem in sorted(pesagens, key=lambda p: (p['data'], p['id'])):
        operacao = operacao_da_pesagem(pesagem, etapas)
        if operacao is None:
            continue
        linha = _linha_base(pesagem, operacao)
        if operacao != OUTRA:
            if e_a_carregada(pesagem, candidatas, HORAS_PAR):
                linha.update(compara(pesagem, candidatas, declarado, divisoes.get(pesagem['recinto'])))
            else:
                linha.update(classificacao=SEM_CONTEINER, detalhe='a carregada é a outra pesagem da visita')
        if linha.get('classificacao'):
            linha['diagnostico'] = DIAGNOSTICOS[linha['classificacao']]
        linhas.append(linha)
    _marca_principal(linhas)
    return linhas


def _marca_principal(linhas: List[dict]):
    """Principal: a primeira COMPARADA (a carregada da retirada ou da chegada — `compara` preencheu a origem do
    declarado) com classificação que não seja "sem dado" (senão, a primeira comparada). Com outra comparada que também
    mediu, a confirmação: os dois medidos dentro da tolerância."""
    comparadas = [linha for linha in linhas if 'origem_declarado' in linha]
    if not comparadas:
        return
    principal = next((linha for linha in comparadas if linha['classificacao'] != SEM_DADO), comparadas[0])
    principal['principal'] = True
    outra = next((linha for linha in comparadas if linha is not principal and linha.get('peso_medido') is not None),
                 None)
    if outra is None or principal.get('peso_medido') is None:
        return
    delta = principal['peso_medido'] - outra['peso_medido']
    referencia = max(abs(principal['peso_medido']), abs(outra['peso_medido']))
    principal['confirmacao'] = (CONFIRMADA if abs(delta) <= tolerancia(referencia)
                                else f'a outra pesagem do ciclo mediu {outra["peso_medido"]:.0f} kg ({delta:+.0f})')
