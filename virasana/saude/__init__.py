"""HEALTH CHECK diário do NAVI (NAVI-I, ciclo de vida e o que os alimenta) — plano em `BACKLOG.md`, 23/10/2026.

Um relatório curto, OK / ATENÇÃO / FALHA por componente, que reaproveita o que cada parte já mede:
`verificacao.py` (resultado, contexto e relatório), `fontes.py` (carga do Mercante e da API Recintos),
`imagens.py` (imagens no MongoDB), `ciclo.py` (ciclo de vida e pesagens), `aplicacoes.py` (risco histórico, busca por
texto, NAVI-I e Fichas de FMA), `saude_navi.py` (linha de comando, à mão), `servico_saude.py` (programa `saude_navi` do
supervisor: rodada das 7 h e as pedidas pela tela), `historico.py` (histórico no Mongo, 180 dias) e `views_saude.py`
(tela `/saude/`, perfil Supervisor). Só leitura das bases medidas, consultas por índice.
"""
