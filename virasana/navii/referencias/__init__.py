# coding: utf-8
"""
Tabelas de referência locais do NAVI-I: código -> nome, para enriquecer a
apresentação (posição NCM, porto, país, navio).

São arquivos JSON nesta pasta, gerados de fontes públicas por
`atualiza_referencias.py` (ver lá as fontes e as limitações de cada uma) e
carregados sob demanda, uma vez por processo. Exceções, mantidas à mão (a fonte
está na chave "_fonte" de cada uma): `tipos_conteiner.json` (ISO 6346 — tamanho,
grupo e potencial OOG), `tipos_bl.json` (tipos de BL do Mercante) e `tipos_trafego.json` (tipos de tráfego do
filtro da tela, 23/10/2026). É um "BD" provisório: serve só
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


def _grupo_do_tipo(codigo: str, tabela: dict) -> Tuple[Optional[str], Optional[str], bool]:
    """(grupo, código de detalhe, tamanho no padrão ISO?) de um código já limpo: ISO (22G1), numérico antigo
    (2210) ou apelido (40HC — aí as duas primeiras posições são os pés, não o código de tamanho)."""
    if len(codigo) == 4 and codigo[2].isalpha() and codigo[3].isdigit():
        grupo = codigo[2]
        conhecido = grupo in tabela.get('grupos', {}) or grupo in tabela.get('nao_iso', {})
        return (grupo, codigo[2:], True) if conhecido else (None, None, False)
    if len(codigo) == 4 and codigo[2:].isdigit():
        return tabela.get('numericos', {}).get(codigo[2]), None, True
    return (tabela.get('apelidos', {}).get(codigo[-2:]) if len(codigo) >= 2 else None), None, False


def _tamanho(codigo: str, iso: bool, tabela: dict) -> Tuple[str, str]:
    """(texto curto, texto completo) do tamanho: pelo código ISO, ou pelos pés do apelido (40HC = 40' high cube)."""
    if iso:
        comprimento = tabela.get('comprimentos', {}).get(codigo[0])
        altura = tabela.get('alturas', {}).get(codigo[1], {}) if comprimento else {}
        return (' '.join(p for p in (comprimento, altura.get('curta')) if p),
                ', '.join(p for p in (comprimento, altura.get('descricao')) if p))
    pes = codigo[:-2]
    if pes not in ('10', '20', '40', '45'):
        return '', ''
    alto = ' high cube' if codigo[-2:] in ('HC', 'HQ') else ''
    return f"{pes}'{alto}", f"{pes}'{alto}"


def tipo_conteiner(codigo: Optional[str]) -> Optional[dict]:
    """Descrição do tipo de contêiner informado (ISO 6346 '45G1', numérico antigo '2210' ou apelido '40HC'):

        {'codigo': '45G1', 'grupo': 'G', 'curta': "40' high cube, uso geral (dry)",
         'descricao': "40', 9'6\\" de altura (high cube), uso geral (dry) — com respiros (ventilação passiva)",
         'oog': False, 'confirmado': True}

    `oog` = potencial carga fora de gabarito (open top, plataforma/flat rack); `confirmado` = grupo da norma
    (False para códigos fora dela, ex.: 'K'). None quando o código não é reconhecido.
    """
    tabela = _tabela('tipos_conteiner.json')
    limpo = re.sub(r'[^A-Z0-9]', '', str(codigo or '').upper())
    grupo, detalhe, iso = _grupo_do_tipo(limpo, tabela) if limpo else (None, None, False)
    if grupo is None:
        return None
    dados_grupo = tabela.get('grupos', {}).get(grupo) or tabela.get('nao_iso', {}).get(grupo)
    tamanho_curto, tamanho = _tamanho(limpo, iso, tabela)
    detalhe_texto = tabela.get('detalhes', {}).get(detalhe or '')
    return {
        'codigo': limpo, 'grupo': grupo,
        'curta': ', '.join(p for p in (tamanho_curto, dados_grupo['nome']) if p),
        'descricao': ', '.join(p for p in (tamanho, dados_grupo['nome']) if p)
        + (f' — {detalhe_texto}' if detalhe_texto else ''),
        'oog': bool(dados_grupo.get('oog')), 'confirmado': grupo in tabela.get('grupos', {}),
    }


def descreve_tipo_bl(codigo: Optional[str]) -> Optional[dict]:
    """Tipo de BL do Mercante ('12' -> {'codigo': '12', 'sigla': 'HBL', 'descricao': 'house (filhote...)'}).
    Código fora da tabela: sigla = o próprio código, descrição None."""
    if codigo is None or not str(codigo).strip():
        return None
    codigo = str(codigo).strip()
    dados = _tabela('tipos_bl.json').get(codigo)
    if not isinstance(dados, dict):
        return {'codigo': codigo, 'sigla': codigo, 'descricao': None}
    return {'codigo': codigo, 'sigla': dados.get('sigla') or codigo, 'descricao': dados.get('descricao')}


def tipos_bl() -> List[dict]:
    """Todos os tipos de BL conhecidos (para o filtro da tela), na ordem do código."""
    return [descreve_tipo_bl(codigo) for codigo in sorted(_tabela('tipos_bl.json')) if not codigo.startswith('_')]


def tipos_trafego() -> List[dict]:
    """Os tipos de tráfego do filtro da tela (Conhecimento.tipoTrafego; `tipos_trafego.json`), na ordem da tabela:
    [{'codigo': '5', 'nome': 'Importação', 'descricao': 'longo curso'}, ...]. A exportação não entra (o NAVI-I não a
    mostra)."""
    tabela = _tabela('tipos_trafego.json')
    return [{'codigo': codigo, 'nome': tabela[codigo]['nome'], 'descricao': tabela[codigo].get('descricao')}
            for codigo in tabela.get('_ordem', []) if codigo in tabela]


def descricao_posicao_ncm(ncm: Optional[str]) -> Optional[str]:
    """Descrição oficial da POSIÇÃO (4 primeiros dígitos) de um código NCM, em
    qualquer formato ('84713012', '8471.30.12', '8471'). Se a posição não
    existir na tabela vigente, cai para a descrição do capítulo (2 dígitos)."""
    digitos = re.sub(r'\D', '', ncm or '')
    if len(digitos) < 2:
        return None
    tabela = _tabela('ncm.json')
    return tabela.get('posicoes', {}).get(digitos[:4]) or tabela.get('capitulos', {}).get(digitos[:2])
