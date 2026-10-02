# coding: utf-8
"""
Dados do contêiner guardados no ciclo de vida (pedido do usuário, 02/10/2026): tipo ISO 6346 — com o
POTENCIAL OOG (open top e plataforma/flat rack podem ter carga fora de gabarito, que não passa nos gates
padrão nem no escâner) — e peso menos tara.

O tipo vem dos eventos da API (`tipoConteiner`) ou do Mercante (`Item.isoCode`). Grupos e descrições vêm
da tabela de referência `virasana/navii/referencias/tipos_conteiner.json` (a mesma das telas), que
reconhece o código ISO (22G1), o numérico antigo (2210) e apelidos (40HC, 40OT, 20FR); aqui ficam os
atalhos do ciclo de vida e o critério SQL de OOG.
"""
from typing import Optional

from sqlalchemy import func

from virasana.navii import referencias

_TABELA = referencias._tabela('tipos_conteiner.json')
GRUPOS = {letra: dados['nome'] for letra, dados in {**_TABELA['grupos'], **_TABELA['nao_iso']}.items()}

# Terceira posição do código que indica potencial OOG — ISO: U (open top), P (plataforma/flat rack);
# numérico antigo: 5x, 6x; apelidos OT, FR, FL. É o critério de banco (`expressao_oog`); o teste
# `test_tipo_iso_e_potencial_oog` confere que bate com `potencial_oog` nos códigos de 4 posições.
OOG_TERCEIRA_POSICAO = ('U', 'P', '5', '6', 'O', 'F')


def grupo_iso(tipo) -> Optional[str]:
    """Letra do grupo do tipo (G, R, U, P, T... ou K, fora da norma) ou None quando não dá para reconhecer."""
    info = referencias.tipo_conteiner(tipo)
    return info['grupo'] if info else None


def potencial_oog(tipo) -> bool:
    """Open top ou plataforma/flat rack: pode ser carga fora de gabarito (sem escaneamento possível)."""
    info = referencias.tipo_conteiner(tipo)
    return bool(info and info['oog'])


def expressao_oog(coluna):
    """Critério SQL "o tipo é potencial OOG" para uma coluna com o tipo do contêiner (NULL não é OOG)."""
    return func.upper(func.substr(coluna, 3, 1)).in_(OOG_TERCEIRA_POSICAO)


def peso_menos_tara(peso, tara) -> Optional[float]:
    """Peso bruto menos tara; None sem peso ou sem tara (balança que não pesou vem zerada)."""
    if not peso or not tara:
        return None
    return float(peso) - float(tara)
