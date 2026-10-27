"""HISTÓRICO do health check no Mongo do virasana (decisões do usuário, 23/10/2026: serviço próprio no supervisor,
rodada diária às 7 h e as pedidas pela tela por um Supervisor, histórico de 180 dias).

Coleções (sem DDL, como os filtros salvos e a coleta de uso):
- `saude_rodadas`: uma por rodada — {inicio, fim, segundos, origem (agenda | tela | manual), usuario, geral,
  resultados: [{componente, estado, resumo, estado_desde, detalhes: [{texto, chave, desde}]}]}; TTL de RETENCAO_DIAS
  em `inicio`;
- `saude_pedidos`: rodada pedida pela tela — {pedido_em, usuario, estado (pendente | rodando | feito), rodada,
  feito_em}; TTL igual;
- `saude_servico`: o batimento do serviço (um documento) — {visto_em, rodando_desde, proxima}.

"Novo" ou "desde": a CHAVE de um achado é o texto sem os números que mudam de uma rodada para outra (quantidades,
horas, percentuais, datas), mantendo os códigos de 7 dígitos ou mais (recinto, contêiner, CE). O achado é "desde" a
rodada mais antiga da sequência ininterrupta de rodadas anteriores que o tiveram; sem nenhuma, é novo (desde = a
própria rodada). O estado do componente tem o mesmo "desde" (`estado_desde`).
"""
import re
from datetime import datetime, timedelta
from typing import List, Optional

from bson import ObjectId
from bson.errors import InvalidId

from virasana.saude.verificacao import Resultado, pior

RETENCAO_DIAS = 180
RODADAS, PEDIDOS, SERVICO = 'saude_rodadas', 'saude_pedidos', 'saude_servico'
ID_SERVICO = 'servico'
PENDENTE, RODANDO, FEITO = 'pendente', 'rodando', 'feito'
RODADAS_PARA_DESDE = 60          # rodadas anteriores lidas para achar o "desde" (2 meses de rodadas diárias)
RODADAS_NA_FAIXA = 14            # quadradinhos do histórico de cada componente no painel
NUMERO = re.compile(r'\d+(?:[.,]\d+)*')


def chave(texto: str) -> str:
    """O texto do achado sem os números que variam ('#'); códigos de 7+ dígitos ficam (identificam o achado)."""
    return NUMERO.sub(lambda m: m.group() if m.group().isdigit() and len(m.group()) >= 7 else '#', texto)


def colecoes(db):
    """(rodadas, pedidos, serviço), com os índices — o TTL apaga o que passa de RETENCAO_DIAS."""
    rodadas, pedidos = db[RODADAS], db[PEDIDOS]
    rodadas.create_index('inicio', expireAfterSeconds=RETENCAO_DIAS * 86400)
    pedidos.create_index('pedido_em', expireAfterSeconds=RETENCAO_DIAS * 86400)
    pedidos.create_index('estado')
    return rodadas, pedidos, db[SERVICO]


# ---------------------------------------------------------------------------------------------------- gravação
def grava_rodada(db, resultados: List[Resultado], inicio: datetime, fim: datetime, origem: str,
                 usuario: Optional[str] = None) -> ObjectId:
    rodadas, _, _ = colecoes(db)
    anteriores = list(rodadas.find({'inicio': {'$lt': inicio}}, {'inicio': 1, 'resultados': 1})
                      .sort('inicio', -1).limit(RODADAS_PARA_DESDE))
    documento = {'inicio': inicio, 'fim': fim, 'segundos': round((fim - inicio).total_seconds(), 1),
                 'origem': origem, 'usuario': usuario, 'geral': pior(r.estado for r in resultados),
                 'resultados': [_marca(r, anteriores, inicio) for r in resultados]}
    return rodadas.insert_one(documento).inserted_id


def _do_componente(rodada: dict, componente: str) -> Optional[dict]:
    return next((r for r in rodada.get('resultados', []) if r['componente'] == componente), None)


def _desde(anteriores: List[dict], inicio: datetime, presente) -> datetime:
    """Início da rodada mais antiga da sequência ininterrupta (das mais novas para trás) em que `presente` vale."""
    desde = inicio
    for rodada in anteriores:
        if not presente(rodada):
            break
        desde = rodada['inicio']
    return desde


def _marca(resultado: Resultado, anteriores: List[dict], inicio: datetime) -> dict:
    def tem_estado(rodada):
        anterior = _do_componente(rodada, resultado.componente)
        return anterior is not None and anterior['estado'] == resultado.estado

    def tem_achado(chave_do_achado):
        def presente(rodada):
            anterior = _do_componente(rodada, resultado.componente)
            return anterior is not None and any(d['chave'] == chave_do_achado for d in anterior.get('detalhes', []))
        return presente

    detalhes = [{'texto': texto, 'chave': chave(texto), 'desde': _desde(anteriores, inicio, tem_achado(chave(texto)))}
                for texto in resultado.detalhes]
    return {'componente': resultado.componente, 'estado': resultado.estado, 'resumo': resultado.resumo,
            'estado_desde': _desde(anteriores, inicio, tem_estado), 'detalhes': detalhes}


# ---------------------------------------------------------------------------------------------------- pedidos
def pede_rodada(db, usuario: str, agora: datetime) -> dict:
    """Pedido de rodada pela tela; havendo um pendente ou rodando, devolve ele (não empilha pedidos)."""
    _, pedidos, _ = colecoes(db)
    aberto = pedidos.find_one({'estado': {'$in': [PENDENTE, RODANDO]}}, sort=[('pedido_em', 1)])
    if aberto:
        return aberto
    documento = {'pedido_em': agora, 'usuario': usuario, 'estado': PENDENTE, 'rodada': None, 'feito_em': None}
    documento['_id'] = pedidos.insert_one(documento).inserted_id
    return documento


def pedidos_pendentes(db) -> List[dict]:
    return list(colecoes(db)[1].find({'estado': PENDENTE}).sort('pedido_em', 1))


def marca_pedidos(db, ids, estado: str, agora: datetime, rodada: Optional[ObjectId] = None):
    valores = {'estado': estado} if estado != FEITO else {'estado': estado, 'rodada': rodada, 'feito_em': agora}
    colecoes(db)[1].update_many({'_id': {'$in': list(ids)}}, {'$set': valores})


def batimento(db, agora: datetime, rodando_desde: Optional[datetime] = None, proxima: Optional[datetime] = None):
    """O serviço está vivo (a tela mostra há quanto tempo foi visto)."""
    db[SERVICO].update_one({'_id': ID_SERVICO}, {'$set': {'visto_em': agora, 'rodando_desde': rodando_desde,
                                                          'proxima': proxima}}, upsert=True)


# ---------------------------------------------------------------------------------------------------- leitura (tela)
def _iso(valor) -> Optional[str]:
    return valor.isoformat(timespec='seconds') if isinstance(valor, datetime) else valor


def _resultado_para_tela(resultado: dict, inicio: datetime) -> dict:
    return {'componente': resultado['componente'], 'estado': resultado['estado'], 'resumo': resultado['resumo'],
            'estado_desde': None if resultado.get('estado_desde') == inicio else _iso(resultado.get('estado_desde')),
            'detalhes': [{'texto': d['texto'], 'novo': d.get('desde') == inicio, 'desde': _iso(d.get('desde'))}
                         for d in resultado.get('detalhes', [])]}


def rodada_para_tela(rodada: dict) -> dict:
    return {'id': str(rodada['_id']), 'inicio': _iso(rodada['inicio']), 'fim': _iso(rodada.get('fim')),
            'segundos': rodada.get('segundos'), 'origem': rodada.get('origem'), 'usuario': rodada.get('usuario'),
            'geral': rodada.get('geral'),
            'resultados': [_resultado_para_tela(r, rodada['inicio']) for r in rodada.get('resultados', [])]}


def uma_rodada(db, rodada_id: str) -> Optional[dict]:
    try:
        rodada = colecoes(db)[0].find_one({'_id': ObjectId(rodada_id)})
    except (InvalidId, TypeError):
        return None
    return rodada_para_tela(rodada) if rodada else None


def lista_rodadas(db, antes: Optional[datetime] = None, limite: int = 30) -> List[dict]:
    filtro = {'inicio': {'$lt': antes}} if antes else {}
    return [{'id': str(r['_id']), 'inicio': _iso(r['inicio']), 'origem': r.get('origem'), 'usuario': r.get('usuario'),
             'geral': r.get('geral'), 'segundos': r.get('segundos')}
            for r in colecoes(db)[0].find(filtro, {'resultados': 0}).sort('inicio', -1).limit(limite)]


def painel(db, agora: datetime, parado_minutos: float) -> dict:
    """O que a tela mostra no topo e no painel: serviço, pedido em aberto, última rodada e a faixa de cada
    componente."""
    rodadas, pedidos, servico = colecoes(db)
    ultimas = list(rodadas.find({}, {'inicio': 1, 'geral': 1, 'resultados.componente': 1, 'resultados.estado': 1})
                   .sort('inicio', -1).limit(RODADAS_NA_FAIXA))
    ultima = rodadas.find_one({'_id': ultimas[0]['_id']}) if ultimas else None
    faixas = {}
    for rodada in reversed(ultimas):   # da mais antiga para a mais nova
        for resultado in rodada.get('resultados', []):
            faixas.setdefault(resultado['componente'], []).append(
                {'id': str(rodada['_id']), 'inicio': _iso(rodada['inicio']), 'estado': resultado['estado']})
    estado_servico = servico.find_one({'_id': ID_SERVICO}) or {}
    visto = estado_servico.get('visto_em')
    pedido = pedidos.find_one({'estado': {'$in': [PENDENTE, RODANDO]}}, sort=[('pedido_em', 1)])
    return {
        'agora': _iso(agora),
        'servico': {'visto_em': _iso(visto),
                    'parado': visto is None or agora - visto > timedelta(minutes=parado_minutos),
                    'rodando_desde': _iso(estado_servico.get('rodando_desde')),
                    'proxima': _iso(estado_servico.get('proxima'))},
        'pedido': {'estado': pedido['estado'], 'usuario': pedido.get('usuario'),
                   'pedido_em': _iso(pedido['pedido_em'])} if pedido else None,
        'ultima': rodada_para_tela(ultima) if ultima else None,
        'faixas': faixas,
    }
