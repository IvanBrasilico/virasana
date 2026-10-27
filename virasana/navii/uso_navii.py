# coding: utf-8
"""Registro do uso do NAVI-I no log do servidor, com selo fixo para filtrar por grep.

Uma linha por ação (abrir a tela, pesquisar, abrir o extrato), em `chave=valor`:

    NAVII_USO acao=pesquisa usuario=<id> janela=efetiva pagina=1 filtros={...} total=152 n_cards=20
              t_consulta=2.613 t_contagem=3.021 t_cards=0.412 t_total=6.08

A ação que levou `SEGUNDOS_LENTA` ou mais no servidor sai em WARNING e termina com `NAVII_LENTA`
(a mesma linha: o total de usos continua sendo o total de `NAVII_USO`). Exemplos:

    grep NAVII_USO <log>                                                # todo o uso
    grep NAVII_LENTA <log>                                              # só as lentas
    grep NAVII_USO <log> | grep -o 'usuario=[^ ]*' | sort | uniq -c     # quem usa
    grep NAVII_USO <log> | grep -o 'acao=[^ ]*' | sort | uniq -c        # como usa

Logger próprio (`virasana.navii.uso`), sem handler: a linha sai pelo handler raiz que o
`ajna_commons.flask.log` configura (o mesmo caminho das demais mensagens da app no servidor).
"""
import json
import logging

SELO = 'NAVII_USO'
SELO_LENTA = 'NAVII_LENTA'
SEGUNDOS_LENTA = 10  # mesmo limite do destaque dos tempos na tela

logger = logging.getLogger('virasana.navii.uso')


def _valor(valor) -> str:
    """Valor sem espaços soltos, para a linha continuar separável por espaço (texto com espaço vai entre aspas)."""
    if isinstance(valor, (dict, list, tuple)):
        return json.dumps(valor, ensure_ascii=False, separators=(',', ':'), sort_keys=True, default=str)
    if isinstance(valor, float):
        return f'{valor:.3f}'
    texto = str(valor)
    return json.dumps(texto, ensure_ascii=False) if not texto or any(c.isspace() for c in texto) else texto


def linha(acao: str, usuario, segundos=None, **campos) -> str:
    """Texto da linha de log (campos None ficam de fora)."""
    partes = [SELO, f'acao={acao}', f'usuario={_valor(usuario) if usuario else "-"}']
    partes += [f'{chave}={_valor(valor)}' for chave, valor in campos.items() if valor is not None]
    if segundos is not None:
        partes.append(f't_total={segundos:.2f}')
        if segundos >= SEGUNDOS_LENTA:
            partes.append(SELO_LENTA)
    return ' '.join(partes)


def registra(acao: str, usuario, segundos=None, **campos) -> None:
    """Grava a linha de uso; lenta (>= SEGUNDOS_LENTA) em WARNING. Nunca derruba a requisição."""
    try:
        lenta = segundos is not None and segundos >= SEGUNDOS_LENTA
        logger.log(logging.WARNING if lenta else logging.INFO, linha(acao, usuario, segundos, **campos))
    except Exception:  # log de uso é acessório: não derruba a tela
        logger.exception('NAVII_USO: falha ao registrar o uso')
