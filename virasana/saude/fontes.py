"""Fontes de dados: a carga do MERCANTE e a da API RECINTOS (eventos que os recintos transmitem).

Mercante: o CE e o item mais novos, pela chave primária (nunca varrer as tabelas — 28 M linhas).

API Recintos: um extrator externo pergunta ao Fichas, por recinto e tipo de evento, até onde já pesquisou
(`apirecintos_controle_extracao.ultimaDataPesquisada`, "ponteiro", junto com o evento mais novo — a mesma regra de
`bhadrasana/routes/apirecintos.py`), consulta a API e grava os eventos. Medidas:
- extrator parado: em cada tipo, a posição mais nova (ponteiro ou evento transmitido) mais velha que
  EXTRATOR_PARADO_HORAS;
- recinto que parou de transmitir: nas últimas 24 h, menos de FRACAO_MINIMA do que ele transmitiu nas MESMAS 24 h da
  semana anterior (o mesmo dia da semana), entre os que transmitiram ao menos MINIMO_DIARIO; o detalhe diz o último
  evento e o ponteiro dele (ponteiro andando = o recinto não manda; parado = o extrator não pergunta);
- volume geral de um tipo abaixo de QUEDA_GERAL da semana anterior;
- salto de rejeições (registros que a carga recusou) por tabela e motivo.
Limiares provisórios: calibrar com as primeiras semanas de relatório.
"""
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text

from virasana.saude.verificacao import (
    ATENCAO, FALHA, OK, Resultado, como_data, ha_quanto, horas_desde, numero, pior,
)

MERCANTE = 'Mercante (carga)'
MERCANTE_ATENCAO_HORAS = 6
MERCANTE_FALHA_HORAS = 24

API = 'API Recintos (carga)'
# tipoEvento do controle da extração -> (tabela, rótulo); '99' são as imagens (verificação das imagens)
TABELAS_API = (('1', 'apirecintos_acessosveiculo', 'acessos'),
               ('3', 'apirecintos_pesagensveiculo', 'pesagens'),
               ('4', 'apirecintos_embarquedesembarque', 'embarques/desembarques'),
               ('25', 'apirecintos_inspecoesnaoinvasivas', 'inspeções (escaneamentos)'))
CONTROLE = 'apirecintos_controle_extracao'
REJEITADOS = 'apirecintos_registros_rejeitados'
EXTRATOR_PARADO_HORAS = 6
MINIMO_DIARIO = 50               # recinto "ativo" no tipo: transmitiu ao menos isso nas 24 h da semana anterior
FRACAO_MINIMA = 0.1              # ... e nas últimas 24 h, menos que esta fração disso: parou (ou quase)
QUEDA_GERAL = 0.5                # volume total do tipo abaixo desta fração da semana anterior
REJEICOES_MINIMAS = 100          # rejeições nas últimas 24 h, por tabela e motivo, para contar como salto ...
REJEICOES_SALTO = 3              # ... e mais que este múltiplo das mesmas 24 h da semana anterior

Janelas = Tuple   # (agora, ontem, base_inicio, base_fim): últimas 24 h e as mesmas 24 h da semana anterior


def janelas(agora) -> Janelas:
    return agora, agora - timedelta(days=1), agora - timedelta(days=8), agora - timedelta(days=7)


# ---------------------------------------------------------------------------------------------------- Mercante
def verifica_mercante(contexto) -> Resultado:
    with contexto.engine.connect() as conexao:
        ce = conexao.execute(text('SELECT ID, create_date FROM conhecimentosresumo ORDER BY ID DESC LIMIT 1')).first()
        item = conexao.execute(text('SELECT ID, create_date FROM itensresumo ORDER BY ID DESC LIMIT 1')).first()
    horas = {nome: horas_desde(contexto.agora, linha[1]) if linha else None
             for nome, linha in (('CE', ce), ('item', item))}
    estados = [FALHA if h is None or h > MERCANTE_FALHA_HORAS else ATENCAO if h > MERCANTE_ATENCAO_HORAS else OK
               for h in horas.values()]
    resumo = f'CE mais novo carregado {ha_quanto(horas["CE"])}; item {ha_quanto(horas["item"])}'
    detalhes = [] if pior(estados) == OK else [
        f'a carga do Mercante está atrasada: CEs novos não chegam ao NAVI-I, ao ciclo de vida (ciclos sem CE) nem ao '
        f'risco histórico (limites: {MERCANTE_ATENCAO_HORAS} h atenção, {MERCANTE_FALHA_HORAS} h falha)']
    return Resultado(MERCANTE, pior(estados), resumo, detalhes)


# ---------------------------------------------------------------------------------------------------- API Recintos
def verifica_api_recintos(contexto) -> Resultado:
    agora = contexto.agora
    with contexto.engine.connect() as conexao:
        nomes = _nomes_dos_recintos(conexao)
        ponteiros = ponteiros_da_extracao(conexao)
        estados, detalhes, partes = [], [], []
        for tipo, tabela, rotulo in TABELAS_API:
            estado, detalhe, parte = _tipo_de_evento(conexao, agora, tipo, tabela, rotulo, ponteiros, nomes)
            estados.append(estado)
            detalhes += detalhe
            partes.append(parte)
        estado, detalhe = _rejeicoes(conexao, janelas(agora))
    return Resultado(API, pior(estados + [estado]), '; '.join(partes), detalhes + detalhe)


def _tipo_de_evento(conexao, agora, tipo, tabela, rotulo, ponteiros, nomes) -> Tuple[str, List[str], str]:
    """(estado, detalhes, resumo) de um tipo de evento: extrator, recintos que pararam e volume geral."""
    volume = _volume_por_recinto(conexao, tabela, janelas(agora))
    ultimas = sum(u for u, _ in volume.values())
    base = sum(b for _, b in volume.values())
    posicao = max(filter(None, [_mais_novo(conexao, tabela)] + [d for (_, t), d in ponteiros.items() if t == tipo]),
                  default=None)
    horas = horas_desde(agora, posicao)
    resumo = f'{rotulo} {numero(ultimas)} (sem. ant. {numero(base)})'
    if horas is None or horas > EXTRATOR_PARADO_HORAS:
        parado = f'{rotulo}: nada transmitido nem pesquisado {ha_quanto(horas)} — extrator da API parado?'
        return FALHA, [parado], resumo
    parados = [_recinto_parado(conexao, tabela, rotulo, recinto, u, b, ponteiros.get((recinto, tipo)), nomes, agora)
               for recinto, (u, b) in sorted(volume.items(), key=lambda par: -par[1][1])
               if b >= MINIMO_DIARIO and u < FRACAO_MINIMA * b]
    if not parados and base >= MINIMO_DIARIO and ultimas < QUEDA_GERAL * base:   # queda que nenhum recinto explica
        parados = [f'{rotulo}: {numero(ultimas)} nas últimas 24 h, {numero(base)} nas mesmas 24 h da semana anterior '
                   '— queda geral']
    return (ATENCAO if parados else OK), parados, resumo


def _recinto_parado(conexao, tabela, rotulo, recinto, ultimas, base, ponteiro, nomes, agora) -> str:
    # nome da tabela fixo (TABELAS_API); o último do recinto pelo índice de codigoRecinto (que leva o id)
    ultimo = conexao.execute(text(f'SELECT dataHoraTransmissao FROM {tabela} '  # nosec B608
                                  'WHERE codigoRecinto = :recinto ORDER BY id DESC LIMIT 1'),
                             {'recinto': recinto}).scalar()
    return (f'{rotulo}: recinto {recinto} {nomes.get(recinto, "")} — {numero(ultimas)} nas últimas 24 h, '
            f'{numero(base)} nas mesmas 24 h da semana anterior; último transmitido '
            f'{ha_quanto(horas_desde(agora, ultimo))}, ponteiro da extração {ha_quanto(horas_desde(agora, ponteiro))}')


def _volume_por_recinto(conexao, tabela: str, janela: Janelas) -> Dict[str, Tuple[int, int]]:
    """{recinto: (eventos transmitidos nas últimas 24 h, nas mesmas 24 h da semana anterior)} — pelo índice de
    dataHoraTransmissao, só as duas janelas."""
    agora, ontem, base_inicio, base_fim = janela
    # nome da tabela fixo (TABELAS_API)
    linhas = conexao.execute(text(
        f'SELECT codigoRecinto, SUM(CASE WHEN dataHoraTransmissao >= :ontem THEN 1 ELSE 0 END), '  # nosec B608
        'SUM(CASE WHEN dataHoraTransmissao < :base_fim THEN 1 ELSE 0 END) '
        f'FROM {tabela} WHERE (dataHoraTransmissao >= :ontem AND dataHoraTransmissao < :agora) '
        'OR (dataHoraTransmissao >= :base_inicio AND dataHoraTransmissao < :base_fim) GROUP BY codigoRecinto'),
        {'agora': agora, 'ontem': ontem, 'base_inicio': base_inicio, 'base_fim': base_fim})
    return {(recinto or '').strip(): (int(u or 0), int(b or 0)) for recinto, u, b in linhas}


def _mais_novo(conexao, tabela: str):
    # nome da tabela fixo (TABELAS_API); MAX pelo índice de dataHoraTransmissao
    return como_data(conexao.execute(text(f'SELECT MAX(dataHoraTransmissao) FROM {tabela}')).scalar())  # nosec B608


def ponteiros_da_extracao(conexao) -> Dict[Tuple[str, str], object]:
    """{(recinto, tipoEvento): ultimaDataPesquisada} — a tabela é pequena (um por recinto e tipo)."""
    return {((recinto or '').strip(), (tipo or '').strip()): como_data(data) for recinto, tipo, data in conexao.execute(
        text(f'SELECT codigoRecinto, tipoEvento, ultimaDataPesquisada FROM {CONTROLE}'))}  # nosec B608


def _nomes_dos_recintos(conexao) -> Dict[str, str]:
    try:
        return {(codigo or '').strip(): nome for codigo, nome in conexao.execute(
            text('SELECT cod_siscomex, nome FROM ovr_recintos WHERE cod_siscomex IS NOT NULL'))}
    except Exception:  # noqa: sem a tabela de recintos, o relatório sai só com os códigos
        return {}


def _rejeicoes(conexao, janela: Janelas) -> Tuple[str, List[str]]:
    """Salto de rejeições por tabela e motivo (pela data em que a carga recusou o registro)."""
    agora, ontem, base_inicio, base_fim = janela
    linhas = conexao.execute(text(
        'SELECT nomeTabela, motivo, SUM(CASE WHEN create_date >= :ontem THEN 1 ELSE 0 END), '  # nosec B608
        'SUM(CASE WHEN create_date < :base_fim THEN 1 ELSE 0 END) '
        f'FROM {REJEITADOS} WHERE (create_date >= :ontem AND create_date < :agora) '
        'OR (create_date >= :base_inicio AND create_date < :base_fim) GROUP BY nomeTabela, motivo'),
        {'agora': agora, 'ontem': ontem, 'base_inicio': base_inicio, 'base_fim': base_fim})
    detalhes = [f'rejeições em {tabela} ({motivo}): {numero(int(u))} nas últimas 24 h, {numero(int(b or 0))} nas '
                'mesmas 24 h da semana anterior' for tabela, motivo, u, b in linhas
                if int(u or 0) >= REJEICOES_MINIMAS and int(u) > REJEICOES_SALTO * int(b or 0)]
    return (ATENCAO if detalhes else OK), detalhes


def posicao_do_extrator(ponteiros: Dict[Tuple[str, str], object], tipo: str) -> Optional[object]:
    """Ponteiro mais novo de um tipo (a verificação das imagens usa o '99')."""
    return max((d for (_, t), d in ponteiros.items() if t == tipo and d), default=None)
