# coding: utf-8
"""
Extrato do CE (`/navii/extrato?CE=<número>`, pedido do usuário em 15/10/2026).

Nos cards do NAVI-I, um CE com vários contêineres repete os dados do CE em cada
card. O extrato mostra o CE UMA vez (tudo o que o card traz: Mercante, família,
navio, risco histórico, CORAD, DTA, Fichas OVR) e depois um card por contêiner,
com destaque para a imagem e para os eventos — linha do tempo e escaneamentos do
ciclo de vida e os eventos da API Recintos do contêiner na janela da operação.

Reaproveita as buscas e a montagem dos cards do NAVI-I (`busca_navii`): o
cabeçalho é o card do CE sem contêiner; os contêineres são os cards da busca por
`ce` (sem limite de data). Os eventos da API Recintos vêm em lote (uma consulta
por tabela, pelo índice do contêiner), filtrados depois pela janela de cada
contêiner.
"""
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy import bindparam, text

from virasana.integracao.mercante.mercantealchemy import Conhecimento
from virasana.navii import tabelas_opcionais
from virasana.navii.busca_navii import (
    TIPO_TRAFEGO_EXPORTACAO, ResultadoNavii, busca_ces_navii, busca_familias_navii, busca_manifestos_navii,
    busca_nomes_recintos, monta_cards_navii,
)

LIMITE_CONTEINERES = 300
ANTES = timedelta(days=5)        # eventos desde alguns dias antes do desembarque (ou da atracação prevista)
DEPOIS = timedelta(days=5)       # e até alguns dias depois da última etapa do ciclo
SEM_FIM = timedelta(days=120)    # ciclo sem etapa final: até 120 dias depois do início

# (tabela, colunas, condição extra, tipo do evento por linha) — SQL fixo; contêineres e datas vão como parâmetro
CONSULTAS_EVENTOS = {
    'apirecintos_embarquedesembarque': (
        'numeroConteiner, dataHoraOcorrencia, codigoRecinto, embarqueDesembarque, pesoBrutoBalanca, escala', ''),
    'apirecintos_acessosveiculo': (
        'numeroConteiner, dataHoraOcorrencia, codigoRecinto, direcao, vazioConteiner, placa', "AND operacao = 'C'"),
    'apirecintos_inspecoesnaoinvasivas': ('numeroConteiner, dataHoraOcorrencia, codigoRecinto, vazio, placa', ''),
    'apirecintos_pesagensveiculo': ('numeroConteiner, dataHoraOcorrencia, codigoRecinto, pesoBrutoBalanca, placa', ''),
}


def normaliza_ce(numero: Optional[str]) -> str:
    return re.sub(r'\D', '', numero or '')


def _data(valor) -> Optional[datetime]:
    if valor is None or isinstance(valor, datetime):
        return valor
    try:
        return datetime.fromisoformat(str(valor)[:19])
    except ValueError:
        return None


def janela_do_conteiner(card: dict) -> Optional[Tuple[datetime, datetime]]:
    """Período dos eventos de um contêiner: do desembarque (ou da atracação prevista) − ANTES até a última etapa
    + DEPOIS, se o ciclo já terminou (entregue, saída do seco ou embarque); com o ciclo em aberto, até pelo menos
    o início + SEM_FIM (eventos novos continuam entrando). None se não há data nenhuma."""
    etapas = card.get('linhaDoTempo') or {}
    datas = {chave: _data((etapas.get(chave) or {}).get('data'))
             for chave in ('desembarque', 'saida', 'entradaRecintoSeco', 'saidaRecintoSeco', 'embarque')}
    presentes = [d for d in datas.values() if d]
    base = datas['desembarque'] or (min(presentes) if presentes else _data(card.get('dataPrevistaAtracacao')))
    if base is None:
        return None
    terminou = card.get('concluido') or datas['saidaRecintoSeco'] or datas['embarque']
    fim = max(presentes) + DEPOIS if presentes else base
    return base - ANTES, fim if terminou else max(fim, base + SEM_FIM)


def _junta(*partes) -> str:
    return ' · '.join(p for p in partes if p)


def _placa(placa) -> str:
    return f'placa {placa}' if placa else ''


def _kg(peso, prefixo: str = '') -> str:
    return f'{prefixo}{float(peso):,.0f} kg'.replace(',', '.') if peso else ''


def _desembarque(linha) -> Tuple[str, str]:
    _, _, _, tipo, peso, escala = linha
    return 'Embarque' if tipo == 'E' else 'Desembarque', _junta(f'escala {escala}' if escala else '',
                                                                _kg(peso, 'balança '))


def _acesso(linha) -> Tuple[str, str]:
    _, _, _, direcao, vazio, placa = linha
    return 'Entrada no recinto' if direcao == 'E' else 'Saída do recinto', _junta(_placa(placa), 'vazio' * bool(vazio))


def _inspecao(linha) -> Tuple[str, str]:
    _, _, _, vazio, placa = linha
    return 'Escaneamento (inspeção não invasiva)', _junta(_placa(placa), 'declarado vazio' * bool(vazio))


def _pesagem(linha) -> Tuple[str, str]:
    _, _, _, peso, placa = linha
    return 'Pesagem', _junta(_kg(peso), _placa(placa))


DESCRICAO_EVENTO = {'apirecintos_embarquedesembarque': _desembarque, 'apirecintos_acessosveiculo': _acesso,
                    'apirecintos_inspecoesnaoinvasivas': _inspecao, 'apirecintos_pesagensveiculo': _pesagem}


def _evento(tabela: str, linha) -> dict:
    """Uma linha de uma tabela da API Recintos como evento do extrato (contêiner, data e recinto nas 3 primeiras
    colunas)."""
    tipo, detalhe = DESCRICAO_EVENTO[tabela](linha)
    return {'data': _data(linha[1]), 'recinto': linha[2], 'tipo': tipo, 'detalhe': detalhe}


def eventos_api_recintos(session, cards: List[dict]) -> Dict[str, List[dict]]:
    """{contêiner: [eventos em ordem de data]} na janela de cada contêiner (`janela_do_conteiner`)."""
    janelas = {c['numeroConteiner']: janela_do_conteiner(c) for c in cards if c.get('numeroConteiner')}
    janelas = {conteiner: janela for conteiner, janela in janelas.items() if janela}
    if not janelas:
        return {}
    desde, ate = min(j[0] for j in janelas.values()), max(j[1] for j in janelas.values())
    conexao = session.connection()
    eventos: Dict[str, List[dict]] = defaultdict(list)
    for tabela, (colunas, condicao) in CONSULTAS_EVENTOS.items():
        if not tabelas_opcionais.existe(session, tabela):
            continue
        # SQL: só os nomes fixos de CONSULTAS_EVENTOS entram no texto
        consulta = text(f'SELECT {colunas} FROM {tabela} WHERE numeroConteiner IN :conteineres '  # nosec B608
                        f'AND dataHoraOcorrencia BETWEEN :desde AND :ate {condicao}').bindparams(
            bindparam('conteineres', expanding=True))
        for linha in conexao.execute(consulta, {'conteineres': sorted(janelas), 'desde': desde, 'ate': ate}):
            conteiner = (linha[0] or '').strip()
            evento = _evento(tabela, linha)
            inicio, fim = janelas.get(conteiner, (None, None))
            if evento['data'] and inicio and inicio <= evento['data'] <= fim:
                eventos[conteiner].append(evento)
    nomes = busca_nomes_recintos(session, [e['recinto'] for lista in eventos.values() for e in lista])
    for lista in eventos.values():
        lista.sort(key=lambda e: e['data'])
        for evento in lista:
            evento['nomeRecinto'] = nomes.get(evento['recinto'])
    return dict(eventos)


def _cabecalho(session, conhecimento: Conhecimento) -> dict:
    """O card do CE sem contêiner: Mercante, família, navio, risco, CORAD, DTA e Fichas."""
    manifesto = busca_manifestos_navii(session, [conhecimento.manifestoCE]).get(conhecimento.manifestoCE, {})
    familia = busca_familias_navii(session, [conhecimento]).get(conhecimento.numeroCEmercante)
    resultado = ResultadoNavii(item=None, conhecimento=conhecimento, ciclo=None,
                               manifesto=manifesto.get('manifesto'), embarcacao=manifesto.get('embarcacao'),
                               data_prevista_atracacao=manifesto.get('dataPrevistaAtracacao'), familia=familia)
    return monta_cards_navii(session, [resultado])[0]


def _resumo(cabecalho: dict, conteineres: List[dict]) -> dict:
    estagios = Counter((c.get('estagio') or {}).get('rotulo') or ('Sem ciclo de vida' if not c['cicloDeVidaLocalizado']
                                                                  else 'Em monitoramento') for c in conteineres)
    return {'declarados': len(cabecalho.get('conteineres') or []), 'comCard': len(conteineres),
            'escaneados': sum(1 for c in conteineres if c.get('imagens_ids')),
            'estagios': sorted(estagios.items(), key=lambda item: -item[1])}


def monta_extrato(session, numero: Optional[str]) -> Optional[dict]:
    """Extrato do CE, ou None se o CE não está no Mercante."""
    numero = normaliza_ce(numero)
    conhecimento = session.query(Conhecimento).filter(Conhecimento.numeroCEmercante == numero).one_or_none() \
        if numero else None
    if conhecimento is None:
        return None
    cabecalho = _cabecalho(session, conhecimento)
    conteineres = monta_cards_navii(session, busca_ces_navii(session, 'efetiva', {'ce': [numero]},
                                                             limit=LIMITE_CONTEINERES))
    declarados = {c['codigoConteiner']: c for c in cabecalho.get('conteineres') or []}
    eventos = eventos_api_recintos(session, conteineres)
    for card in conteineres:
        card['declarado'] = declarados.get(card['numeroConteiner'])
        card['eventosApi'] = eventos.get(card['numeroConteiner'], [])
        card['janelaEventos'] = janela_do_conteiner(card)
    return {'ce': cabecalho, 'conteineres': conteineres, 'resumo': _resumo(cabecalho, conteineres),
            'exportacao': conhecimento.tipoTrafego == TIPO_TRAFEGO_EXPORTACAO,
            'limite': LIMITE_CONTEINERES}
