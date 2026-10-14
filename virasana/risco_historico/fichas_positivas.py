# coding: utf-8
"""
Fichas POSITIVAS do risco histórico por CE (ver CONTEXTO.md): as ocorrências do
Fichas que ensinam o risco.

Decisões do usuário (14/10/2026):
  - resultados que contam: Perdimento (2), Crédito (3), Multa RFB (6) e Multa de
    outro órgão (7). Apreensão fica de fora (quase toda de cocaína: perfil
    logístico, não do importador); Sanção e DARF também;
  - TG de alto valor (soma dos TGs da ficha >= R$ 100 mil) conta mesmo sem esses
    resultados, e pesa o dobro;
  - FMA (tipo de operação 0, Mercadoria Abandonada) fica de fora: abandono é outro
    risco. As fichas de exportação (tipos 2 e 4) também.

O valor do TG é o do Fichas: `ovr_tgovr.valor` (soma de quantidade x valor dos
itens, ver `atualiza_valortotal_tg`), somado por ficha.
"""
from datetime import datetime
from typing import List, Optional

from sqlalchemy import bindparam, text

RESULTADOS_POSITIVOS = (2, 3, 6, 7)        # Perdimento, Crédito, Multa RFB, Multa outro órgão
TG_ALTO_VALOR = 100000.0                   # R$
TIPOS_OPERACAO_EXCLUIDOS = (0, 2, 4)       # FMA (abandono); exportação
PESO_FICHA, PESO_TG_ALTO = 1.0, 2.0

SQL_POSITIVAS = """
SELECT o.id, o.numero, o.datahora, o.tipooperacao, o.numeroCEmercante, o.cnpj_fiscalizado, o.observacoes,
       (SELECT SUM(t.valor) FROM ovr_tgovr t WHERE t.ovr_id = o.id) AS valor_tg,
       CASE WHEN EXISTS (SELECT 1 FROM ovr_resultados r WHERE r.ovr_id = o.id AND r.tipo_resultado IN :resultados)
            THEN 1 ELSE 0 END AS com_resultado
FROM ovr_ovrs o
WHERE (o.tipooperacao IS NULL OR o.tipooperacao NOT IN :excluidos)
  AND (EXISTS (SELECT 1 FROM ovr_resultados r WHERE r.ovr_id = o.id AND r.tipo_resultado IN :resultados)
       OR (SELECT SUM(t.valor) FROM ovr_tgovr t WHERE t.ovr_id = o.id) >= :limiar)
  {filtro_data}
ORDER BY o.datahora, o.id
"""


def motivo(ficha: dict, limiar: float = TG_ALTO_VALOR) -> str:
    """'resultado', 'tg' (só pelo TG de alto valor) ou 'ambos'."""
    tg_alto = float(ficha.get('valor_tg') or 0) >= limiar
    if ficha.get('com_resultado'):
        return 'ambos' if tg_alto else 'resultado'
    return 'tg'


def peso(ficha: dict, limiar: float = TG_ALTO_VALOR) -> float:
    return PESO_TG_ALTO if float(ficha.get('valor_tg') or 0) >= limiar else PESO_FICHA


def busca(conexao, inicio: Optional[datetime] = None, fim: Optional[datetime] = None,
          limiar: float = TG_ALTO_VALOR) -> List[dict]:
    """Fichas positivas (data da ficha em [inicio, fim)), com `valor_tg`,
    `com_resultado`, `motivo` e `peso`. As chaves de ficha são as que
    `dados_risco_perdimentos.extrai(fichas=...)` espera."""
    filtro_data, params = '', {'resultados': list(RESULTADOS_POSITIVOS),
                               'excluidos': list(TIPOS_OPERACAO_EXCLUIDOS), 'limiar': limiar}
    if inicio:
        filtro_data += ' AND o.datahora >= :inicio'
        params['inicio'] = inicio
    if fim:
        filtro_data += ' AND o.datahora < :fim'
        params['fim'] = fim
    # SQL: só os filtros fixos acima entram no texto
    consulta = text(SQL_POSITIVAS.format(filtro_data=filtro_data)).bindparams(  # nosec B608
        bindparam('resultados', expanding=True), bindparam('excluidos', expanding=True))
    fichas = []
    for linha in conexao.execute(consulta, params):
        ficha = dict(linha._mapping)
        ficha['valor_tg'] = float(ficha['valor_tg'] or 0)
        ficha['motivo'] = motivo(ficha, limiar)
        ficha['peso'] = peso(ficha, limiar)
        fichas.append(ficha)
    return fichas
