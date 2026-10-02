# coding: utf-8
"""
Fichas OVR (sistema Fichas, repo bhadrasana2) ligadas a um CE ou a um contêiner — para o card do NAVI-I e o
detalhe da completude de escaneamentos (pedido do usuário, 02/10/2026). Somente leitura.

  - pelo CE (`ovr_ovrs.numeroCEmercante`): todas as fichas, abertas e encerradas;
  - pelo contêiner (`ovr_verificacoesfisicas.numerolote`, a RVF da ficha): só as ABERTAS — o número do
    contêiner se repete em outras operações, e uma ficha encerrada de outra operação só confundiria.

Ficha com `tipooperacao = 0` ("Mercadoria Abandonada") é uma FMA: o abandono informado pelo recinto, aberto
automaticamente pelo `bhadrasana/scripts/fma_update.py` do bhadrasana2.

As tabelas são declaradas aqui só com as colunas lidas (como `busca_navii.tabela_recintos`): importar os
models do bhadrasana carregaria o registry inteiro do OVR. `test_navii_fichas_ovr` confere as colunas e as
listas de tipos e fases contra os models.
"""
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, Table, VARCHAR, select

_metadata = MetaData()
tabela_ovrs = Table(
    'ovr_ovrs', _metadata,
    Column('id', BigInteger, primary_key=True),
    Column('numero', VARCHAR(10)),
    Column('ano', VARCHAR(4)),
    Column('tipooperacao', Integer),
    Column('numeroCEmercante', VARCHAR(15)),
    Column('fase', Integer),
    Column('datahora', DateTime),
)
tabela_rvfs = Table(
    'ovr_verificacoesfisicas', _metadata,
    Column('id', BigInteger, primary_key=True),
    Column('ovr_id', BigInteger),
    Column('numerolote', VARCHAR(20)),
)

# Cópias de bhadrasana.models.ovr.tipoOperacao / faseOVR (o teste confere)
TIPOS_OPERACAO = [
    'Mercadoria Abandonada',
    'Repressão mediante análise de risco na importação',
    'Repressão mediante análise de risco na exportação',
    'Pesquisa e seleção na importação',
    'Pesquisa e seleção na exportação',
    'Demanda externa',
    'Registro de operação de outros países',
    'Vigilância',
    'Registro de operação de outros órgãos',
]
FASES = ['Iniciada', 'Ativa', 'Suspensa', 'Concluída', 'Arquivada']
FASES_ABERTAS = (0, 1, 2)
MERCADORIA_ABANDONADA = 0
# Link para a outra aplicação: prefixo fixo do bhadrasana2 atrás do proxy (ver CLAUDE.md)
URL_FICHA = '/bhadrasana2/ovr?id={}'
TAMANHO_LOTE = 500


def _descricao(lista: List[str], indice: Optional[int]) -> Optional[str]:
    return lista[indice] if indice is not None and 0 <= indice < len(lista) else None


def _numero(numero: Optional[str], ano: Optional[str], tipooperacao: Optional[int], id_: int) -> str:
    """Como o Fichas mostra: FMA com 5 dígitos e o ano (ex.: 00123/2026); sem número, o id da ficha."""
    if not numero:
        return str(id_)
    if tipooperacao == MERCADORIA_ABANDONADA:
        try:
            numero = '{:05d}'.format(int(numero))
        except (TypeError, ValueError):
            pass
    return f'{numero}/{ano}' if ano else numero


def descreve(id_, numero, ano, tipooperacao, ce, fase, datahora, via: str) -> dict:
    """Uma ficha para a tela. `via`: 'CE' (o próprio CE), 'família' (outro CE da família) ou 'contêiner'."""
    return {
        'id': id_, 'numero': _numero(numero, ano, tipooperacao, id_),
        'tipo': _descricao(TIPOS_OPERACAO, tipooperacao), 'fase': _descricao(FASES, fase),
        'aberta': fase in FASES_ABERTAS, 'abandono': tipooperacao == MERCADORIA_ABANDONADA,
        'ce': ce, 'data': datahora, 'via': via, 'url': URL_FICHA.format(id_),
    }


def _chave(valor: Optional[str]) -> str:
    """CE/contêiner como chave: sem espaços nas pontas e em maiúsculas (o número do contêiner da RVF é digitado)."""
    return (valor or '').strip().upper()


def _lotes(valores: Iterable[str]) -> List[List[str]]:
    valores = sorted({v.strip() for v in valores if v and v.strip()})
    return [valores[i:i + TAMANHO_LOTE] for i in range(0, len(valores), TAMANHO_LOTE)]


_COLUNAS = (tabela_ovrs.c.id, tabela_ovrs.c.numero, tabela_ovrs.c.ano, tabela_ovrs.c.tipooperacao,
            tabela_ovrs.c.numeroCEmercante, tabela_ovrs.c.fase, tabela_ovrs.c.datahora)


def busca_fichas(session, ces: Iterable[str],
                 conteineres: Iterable[str] = ()) -> Tuple[Dict[str, list], Dict[str, list]]:
    """({CE: [linhas da ficha]}, {contêiner: [linhas das fichas ABERTAS pela RVF]}), em lotes, pelos índices de
    `numeroCEmercante` e `numerolote`. Linha = (id, numero, ano, tipooperacao, CE, fase, datahora)."""
    por_ce: Dict[str, list] = {}
    for lote in _lotes(ces):
        for linha in session.execute(select(*_COLUNAS).where(tabela_ovrs.c.numeroCEmercante.in_(lote))):
            por_ce.setdefault(_chave(linha[4]), []).append(tuple(linha))
    por_conteiner: Dict[str, list] = {}
    for lote in _lotes(conteineres):
        consulta = (select(tabela_rvfs.c.numerolote, *_COLUNAS).distinct()
                    .join(tabela_ovrs, tabela_ovrs.c.id == tabela_rvfs.c.ovr_id)
                    .where(tabela_rvfs.c.numerolote.in_(lote), tabela_ovrs.c.fase.in_(FASES_ABERTAS)))
        for conteiner, *linha in session.execute(consulta):
            por_conteiner.setdefault(_chave(conteiner), []).append(tuple(linha))
    return por_ce, por_conteiner


def fichas_do(ce: Optional[str], conteiner: Optional[str], por_ce: Dict[str, list], por_conteiner: Dict[str, list],
              familia: Iterable[str] = ()) -> List[dict]:
    """Fichas de um card/linha: as do CE, as dos outros CEs da família e as abertas pelo contêiner, sem repetir;
    abertas primeiro, depois as mais recentes."""
    vistas, fichas = set(), []
    origens = [(ce, 'CE', por_ce)] + [(outro, 'família', por_ce) for outro in sorted(set(familia) - {ce})]
    origens.append((conteiner, 'contêiner', por_conteiner))
    for chave, via, indice in origens:
        for linha in indice.get(_chave(chave), []) if chave else []:
            if linha[0] not in vistas:
                vistas.add(linha[0])
                fichas.append(descreve(*linha, via=via))
    fichas.sort(key=lambda f: f['data'] or datetime.min, reverse=True)
    return sorted(fichas, key=lambda f: not f['aberta'])   # estável: abertas primeiro, cada grupo por data
