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
import unicodedata
from functools import lru_cache
from typing import Iterable, List, Optional, Tuple

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


def _sem_acento(texto: str) -> str:
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode().lower()


def _sugere(opcoes: Iterable[Tuple[str, str]], termo: str, limite: int) -> List[dict]:
    """Primeiro os códigos que começam pelo termo, depois os nomes que o contêm
    (sem diferenciar acentos/maiúsculas)."""
    termo_codigo = re.sub(r'[\s.]', '', termo or '').upper()
    termo_nome = _sem_acento((termo or '').strip())
    if not termo_nome:
        return []
    por_codigo, por_nome = [], []
    for codigo, descricao in opcoes:
        if termo_codigo and codigo.startswith(termo_codigo):
            por_codigo.append({'codigo': codigo, 'descricao': descricao})
        elif termo_nome in _sem_acento(descricao):
            por_nome.append({'codigo': codigo, 'descricao': descricao})
    return (sorted(por_codigo, key=lambda s: (len(s['codigo']), s['codigo'])) + por_nome)[:limite]


def sugere_portos(termo: str, limite: int = 15) -> List[dict]:
    """Sugestões para os campos de porto: países (código de 2 letras = todos os
    portos do país) e portos das tabelas locais. [{'codigo', 'descricao'}]"""
    paises = [(sigla, f'{nome} (todos os portos)') for sigla, nome in _tabela('paises.json').items()]
    portos = {**_portos(), **_tabela('portos_ajustes.json')}
    return _sugere(paises + [(codigo, descreve_porto(codigo) or nome) for codigo, nome in portos.items()],
                   termo, limite)


def sugere_ncm(termo: str, limite: int = 15) -> List[dict]:
    """Sugestões para o campo NCM: capítulos (2 dígitos) e posições (4)."""
    tabela = _tabela('ncm.json')
    opcoes = list(tabela.get('capitulos', {}).items()) + list(tabela.get('posicoes', {}).items())
    return _sugere(opcoes, termo, limite)


def descricao_posicao_ncm(ncm: Optional[str]) -> Optional[str]:
    """Descrição oficial da POSIÇÃO (4 primeiros dígitos) de um código NCM, em
    qualquer formato ('84713012', '8471.30.12', '8471'). Se a posição não
    existir na tabela vigente, cai para a descrição do capítulo (2 dígitos)."""
    digitos = re.sub(r'\D', '', ncm or '')
    if len(digitos) < 2:
        return None
    tabela = _tabela('ncm.json')
    return tabela.get('posicoes', {}).get(digitos[:4]) or tabela.get('capitulos', {}).get(digitos[:2])
