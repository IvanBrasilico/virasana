# coding: utf-8
"""
Tabelas de referência locais do NAVI-I: código -> nome, para enriquecer a
apresentação (posição NCM, porto, país, navio).

São arquivos JSON nesta pasta, gerados de fontes públicas por
`atualiza_referencias.py` (ver lá as fontes e as limitações de cada uma) e
carregados sob demanda, uma vez por processo. É um "BD" provisório: serve só
para EXIBIR nomes. Código sem nome conhecido devolve None — quem chama mostra
apenas o código; nunca se inventa um nome.
"""
import io
import json
import os
import re
from functools import lru_cache
from typing import Optional

_AQUI = os.path.dirname(os.path.abspath(__file__))


@lru_cache(maxsize=None)
def _tabela(arquivo: str) -> dict:
    caminho = os.path.join(_AQUI, arquivo)
    if not os.path.exists(caminho):
        return {}
    with io.open(caminho, encoding='utf-8') as fp:
        return json.load(fp)


def _portos() -> dict:
    return _tabela('portos.json')


def nome_pais(sigla: Optional[str]) -> Optional[str]:
    """Nome em português do país pela sigla ISO de 2 letras ('CN' -> 'China')."""
    if not sigla:
        return None
    return _tabela('paises.json').get(sigla.strip().upper()[:2])


def nome_porto(codigo: Optional[str]) -> Optional[str]:
    """Nome do porto pelo código UN/LOCODE de 5 caracteres ('BRSSZ' -> 'Santos')."""
    if not codigo:
        return None
    codigo = codigo.strip().upper()
    return _tabela('portos_ajustes.json').get(codigo) or _portos().get(codigo)


def descreve_porto(codigo: Optional[str]) -> Optional[str]:
    """Texto de exibição de um porto: 'Santos, Brasil'.

    Sem nome de porto conhecido, devolve só o país ('China') — inclusive para
    os códigos genéricos terminados em ZZZ, que o Mercante usa para "porto não
    especificado" do país. Sem país conhecido, None.
    """
    if not codigo:
        return None
    porto, pais = nome_porto(codigo), nome_pais(codigo)
    if porto and pais and porto != pais:  # evita "Hong Kong, Hong Kong"
        return f'{porto}, {pais}'
    return porto or pais


def nome_navio(embarcacao: Optional[str]) -> Optional[str]:
    """Nome do navio pelo número IMO (7 dígitos). Cobertura parcial e nome
    possivelmente desatualizado (fonte: Wikidata). Inscrições nacionais de
    embarcações de navegação interior (10 dígitos) não têm fonte."""
    if not embarcacao:
        return None
    return _tabela('navios.json').get(embarcacao.strip())


def descricao_posicao_ncm(ncm: Optional[str]) -> Optional[str]:
    """Descrição oficial da POSIÇÃO (4 primeiros dígitos) de um código NCM, em
    qualquer formato ('84713012', '8471.30.12', '8471'). Se a posição não
    existir na tabela vigente, cai para a descrição do capítulo (2 dígitos)."""
    digitos = re.sub(r'\D', '', ncm or '')
    if len(digitos) < 2:
        return None
    tabela = _tabela('ncm.json')
    return tabela.get('posicoes', {}).get(digitos[:4]) or tabela.get('capitulos', {}).get(digitos[:2])
