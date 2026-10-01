# coding: utf-8
"""
Submenu "NAVII" do menu principal do virasana (`views.mynavbar`).

As telas de análise de risco ficam aqui, num lugar só, para não mexer mais no
menu legado. Cada entrada é (texto, endpoint Flask); o menu é montado com
`url_for`, então respeita o prefixo /virasana do Servidor. Um endpoint errado
derrubaria TODAS as páginas (o menu é renderizado em todas): o teste
`test_prefixo_proxy.TestMenuNavii` monta cada entrada.
"""
from flask_nav.elements import Link, Separator, Subgroup, View

ENTRADAS_NAVII = [
    ('NAVI-I — Análise de risco na importação', 'navii.index'),
    ('Entregas por CNPJ', 'entregas.index'),
    ('Transporte das apreensões', 'apreensoes.index'),
]

# Manual publicado no Apache, fora das apps (ver ajna_docs/docs2.0/publica_manual.sh)
URL_MANUAL = '/ajna/risco/'


def subgrupo_navii() -> Subgroup:
    return Subgroup(
        'NAVII',
        *[View(texto, endpoint) for texto, endpoint in ENTRADAS_NAVII],
        Separator(),
        Link('Manual (análise de risco)', URL_MANUAL),
    )
