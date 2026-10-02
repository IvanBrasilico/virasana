# coding: utf-8
"""
Dados do contêiner guardados no ciclo de vida (pedido do usuário, 02/10/2026): tipo ISO 6346 — com o
POTENCIAL OOG (open top e plataforma/flat rack podem ter carga fora de gabarito, que não passa nos gates
padrão nem no escâner) — e peso menos tara. Funções puras, sem banco.

O tipo vem dos eventos da API (`tipoConteiner`, 4 posições). Além do código ISO (22G1, 45U1, 42P1...) os
recintos mandam o código numérico antigo (2210, 2251...) e apelidos (40HC, 40OT, 20FR...): os três são
reconhecidos; o resto fica sem grupo.
"""
import re
from typing import Optional

GRUPOS = {'G': 'uso geral', 'V': 'ventilado', 'B': 'granel', 'S': 'especial', 'R': 'refrigerado',
          'H': 'isotérmico', 'U': 'open top', 'P': 'plataforma/flat rack', 'T': 'tanque', 'A': 'aéreo/superfície'}
GRUPOS_OOG = {'U', 'P'}
# código numérico antigo (ISO 6346:1984): 1º dígito do código de tipo (3ª posição)
_NUMERICO = {'0': 'G', '1': 'G', '2': 'V', '3': 'R', '4': 'H', '5': 'U', '6': 'P', '7': 'T', '8': 'B', '9': 'A'}
_APELIDOS = {'DC': 'G', 'DV': 'G', 'GP': 'G', 'HC': 'G', 'HQ': 'G', 'ST': 'G', 'OT': 'U', 'FR': 'P', 'FL': 'P',
             'PL': 'P', 'PF': 'P', 'RF': 'R', 'RH': 'R', 'RE': 'R', 'TK': 'T', 'TN': 'T', 'BK': 'B', 'VH': 'V'}


def grupo_iso(tipo) -> Optional[str]:
    """Letra do grupo do tipo (G, R, U, P, T...) ou None quando não dá para reconhecer."""
    codigo = re.sub(r'[^A-Z0-9]', '', str(tipo or '').upper())
    if len(codigo) != 4:
        return _APELIDOS.get(codigo[-2:]) if len(codigo) >= 2 else None
    if codigo[2].isalpha() and codigo[3].isdigit():
        return codigo[2] if codigo[2] in GRUPOS else None
    if codigo[2:].isdigit():
        return _NUMERICO.get(codigo[2])
    return _APELIDOS.get(codigo[2:])


def potencial_oog(tipo) -> bool:
    """Open top ou plataforma/flat rack: pode ser carga fora de gabarito (sem escaneamento possível)."""
    return grupo_iso(tipo) in GRUPOS_OOG


def peso_menos_tara(peso, tara) -> Optional[float]:
    """Peso bruto menos tara; None sem peso ou sem tara (balança que não pesou vem zerada)."""
    if not peso or not tara:
        return None
    return float(peso) - float(tara)
