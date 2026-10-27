# coding: utf-8
"""
Risco histórico por CE no NAVI-I (F4; ver virasana/risco_historico/CONTEXTO.md):

  - filtro "Risco histórico" (`risco`): só alto / médio ou alto / com algum sinal
    — verificado por candidato da janela (`_existe` de busca_navii, pela chave
    única de risco_historico_ce), como os outros filtros;
  - no card: faixa, pontos e a MEMÓRIA DE CÁLCULO (uma parcela por grupo: valor,
    fichas, CEs, vezes a média, peso), refeita com os pesos da versão com que o
    CE foi pontuado — o total é a soma das parcelas.

Sem as tabelas do risco no banco (homologação, testes antigos), o card fica sem
risco e o filtro responde com erro claro — o resto do NAVI-I não muda.
"""
from typing import Callable, Dict, Iterable, List

from sqlalchemy import select

from virasana.integracao.mercante.mercantealchemy import Item
from virasana.navii import tabelas_opcionais
from virasana.navii.fichas_ovr import URL_FICHA
from virasana.risco_historico import calculo, parametros
from virasana.risco_historico.sinais import de_json, pontuaveis
from virasana.risco_historico.tabelas import CES

FILTROS_RISCO = {'alto': ('alto',), 'medio': ('alto', 'medio'), 'sinal': ('alto', 'medio', 'baixo')}
ROTULOS_FILTRO = {'sinal': 'Com algum sinal', 'medio': 'Médio ou alto', 'alto': 'Só alto'}
FICHAS_NA_MEMORIA = 5


def tem_tabelas(session) -> bool:
    return tabelas_opcionais.existe(session, CES.name)


def criterio(session, valor: str, existe: Callable):
    """Condição do filtro `risco` para a query (Item, Conhecimento) do NAVI-I."""
    if not tem_tabelas(session):
        raise ValueError('Risco histórico indisponível neste banco (tabelas do risco por CE ainda não criadas).')
    return existe(CES, CES.c.numeroCEmercante == Item.numeroCEmercante, CES.c.faixa.in_(FILTROS_RISCO[valor]))


def _memoria(versao: parametros.Versao, sinais) -> List[dict]:
    _, parcelas = calculo.pontua(pontuaveis(sinais), versao.pesos)
    return [{
        'grupo': calculo.ROTULOS[calculo.GRUPO_DA_DIMENSAO[dimensao]], 'dimensao': calculo.ROTULOS[dimensao],
        'valor': valor, 'descricao': versao.descricoes.get((dimensao, valor), ''),
        'fichas': peso.fichas, 'importadores': peso.importadores, 'ces': peso.n,
        'pct': round(100 * peso.n / peso.total_n, 3) if peso.total_n else 0.0,
        'vezes': round(peso.vezes, 1), 'peso': peso.peso,
        'fichas_links': [{'id': id_, 'url': URL_FICHA.format(id_)}
                         for id_ in versao.fichas.get((dimensao, valor), [])[:FICHAS_NA_MEMORIA]],
    } for dimensao, valor, peso in parcelas]


def risco_dos_ces(session, numeros: Iterable[str]) -> Dict[str, dict]:
    """{CE: risco} dos CEs da página que estão em risco_historico_ce (os demais: não calculado)."""
    numeros = sorted({n for n in numeros if n})
    if not numeros or not tem_tabelas(session):
        return {}
    conexao = session.connection()
    resultado = {}
    for linha in conexao.execute(select(CES.c.numeroCEmercante, CES.c.sinais, CES.c.pontos, CES.c.faixa,
                                        CES.c.versao_id, CES.c.calculado_em)
                                 .where(CES.c.numeroCEmercante.in_(numeros))):
        pontos = float(linha.pontos) if linha.pontos is not None else None
        risco = {'pontos': pontos, 'faixa': linha.faixa, 'rotuloFaixa': calculo.ROTULOS_FAIXA.get(linha.faixa),
                 'versao': linha.versao_id, 'calculadoEm': linha.calculado_em, 'parcelas': [],
                 'recalculando': linha.versao_id is None}
        if linha.versao_id and pontos:
            versao = parametros.carrega_versao(conexao, linha.versao_id)
            risco['parcelas'] = _memoria(versao, de_json(linha.sinais))
            risco['versaoEm'] = versao.criado_em
        resultado[linha.numeroCEmercante.strip()] = risco
    return resultado
