# Backlog — NAVI-I, ciclo de vida, risco e busca (atualizado em 24/10/2026)

Junta as pendências que estavam espalhadas: `virasana/navii/CONTEXTO.md` ("Pendências" e "Próximos passos"),
`virasana/analytic_scripts/ciclo_de_vida/CICLO_DE_VIDA.md`, `virasana/risco_historico/CONTEXTO.md`,
`virasana/saude/CONTEXTO.md`, `virasana/analytic_scripts/declaracao/DIAGNOSTICO.md` e as notas das sessões. O
detalhe de cada item fica no documento de origem; aqui ficam o estado, a prioridade e a **decisão que falta**. Ao
fechar um item, mova-o para "Feito" com a data (uma linha; o histórico vai para o documento de origem).

**Critério de prioridade**, nesta ordem: (1) dado errado ou perdido em produção; (2) operação e saúde — saber cedo
quando algo quebra; (3) desempenho percebido; (4) funcionalidade nova. Ajustes que saírem da validação com os
usuários (em andamento) entram como itens novos.

## Feito (recente)

- **Revisita de imagens** publicada e `--completa-imagens 2026-09-01` rodado no servidor (23/10/2026): ciclos
  (inclusive concluídos) dos últimos 10 dias com escaneamento sem imagem voltam à revisita (até 1/4 do limite);
  detalhe no `CICLO_DE_VIDA.md` ("Imagens que faltam"). Medir no próximo acompanhamento: "com imagem" por semana
  (82–91% antes) e os 9.757 escaneamentos sem imagem com mais de 3 dias.
- **Pendentes do loop antigo**: `--reprocessa 100` rodado com a publicação da revisita (23/10/2026; eram 21, de
  91.397 em 02/10). Conferir no próximo acompanhamento (seção 6) que zerou; sobra aceitável = contêiner sem evento.
- **Cubagem no card do NAVI-I** publicada (23/10/2026): a do CE e a de cada contêiner, com ocupação e densidade
  (`navii/CONTEXTO.md`, item 23).
- **Health check v1** publicado e ativo (23/10/2026): serviço `saude_navi`, tela `/saude/` (perfil Supervisor),
  histórico "novo / desde" — desenho, limiares e 1ª rodada em `virasana/saude/CONTEXTO.md`. Refaz robusto e refaz
  de 22/10 concluídos no servidor (0 não refeitos, 0 violações).
- **Índices do NAVI-I** (conferido em 23/10/2026 nos `SHOW CREATE TABLE` dos diagnósticos de 16/10):
  `ix_ciclo_ce_conteiner` (criado em 30/09) e `ix_cdv_data_ce_conteiner` (02/10) existem; `ix_itens_ce_conteiner`
  **não** — e não faz falta: `ix_itens_chave (numeroCEmercante, numeroSequencialItemCarga)` já entra pelo CE, e um CE
  tem poucos itens. Fica fechado; o alerta de tempo do NAVI-I no health check (p95 > 10 s) é quem reabre.
- Nome NAVI no código novo (tela e relatório do health check, programa `saude_navi`) — 23/10/2026.
- **Busca por texto** (Meilisearch), aba `/navii/texto`, em produção desde 19/10/2026.
- Ciclo de vida: porto do desembarque (C1–C3), C6/C7/C8, corpus de 26 casos reais, auditoria da produção — 22/10.
- NAVI-I: modos de exibição, filtro de tráfego, atracação prevista, pesagens P1–P3, imagem pelo contêiner e data —
  20 a 22/10. Ciclo fora de Santos (MultiRio, Suape, Pecém) — 21/10. Gate `qualidade_navii.py --alterados` — 22/10.
- Validação com os usuários do que está no ar (Busca por texto, Estatísticas de uso, modos de exibição, pesagens,
  Fichas pelo NAVI-I): em andamento fora do backlog — os ajustes entram como itens novos.

## P0 — agora

1. **Cobrar os recintos** (fora do código; achados do health check de 23/10): MultiRio (7921303) e ICTSI (7921302)
   sem embarques/desembarques; BTP3 sem imagem. Acompanhar pela tela `/saude/` ("desde dd/mm").
2. **Calibrar o health check** com 1 a 2 semanas de rodadas (até ~06/11): limiares nas constantes do topo de cada
   módulo de `virasana/saude/`.

## P1 — próximas semanas

4. **Parser da carga do Mercante** (Pendência 5 do `navii/CONTEXTO.md`): `notify`/`cnpjNotify` (77%/53% dos CEs nos
   XMLs) chegam **vazios**; dados complementares do consignatário, observações e nome do embarcador com CNPJ se
   perdem. Os XMLs são apagados depois da carga: **cada dia sem a correção é dado perdido para sempre**. Afeta a
   Entregas por CNPJ e a Busca por texto. Precisa de `ALTER TABLE` (DBA). *Decisão: priorizar agora?*
6. **Contagem assíncrona no NAVI-I** (proposta de 02/10/2026): a contagem com NCM leva 18–20 s e segura a 1ª página;
   no health check, mediana de 6,1 s e p95 de 10,5 s só na 1ª página. Mostrar os cards já e a contagem quando
   chegar. *Decisão: aprovar.*

## P2 — depois

9. **Unidade/equipe do usuário**: porto de atracação e recintos padrão pela unidade (hoje `PORTO_ESCALA_PADRAO` fixo
   em BRSSZ). Passa a importar com o ciclo já rodando em MultiRio, Suape e Pecém.
10. **Risco histórico**: F2 (validação no tempo, adiada em 14/10), normalizar o nome do embarcador (vem com
    endereço), ordenação por risco no NAVI-I (medir o cursor composto), F5 (analista desativa parâmetro).
11. **Coleta de uso × galeria**: na galeria vários cards ficam "vistos" de uma vez. Gravar o modo de exibição na
    coleta para separar nas estatísticas? *Decisão.*
12. **Busca por texto**: CE alterado depois de indexado (hoje fica de fora); regra da data de referência (destino ×
    Santos).
13. **C4** (o CE declarado vence a única família plausível não declarada): só se a auditoria mostrar casos.
14. **C7 nos filtros por candidato**: contêiner + "só com desembarque" deixa de fora o BL único que não é o
    principal (atalho mantido por desempenho).
15. **AJNA → NAVI no legado** (`layout.html` e ~29 templates do virasana, ~37 do bhadrasana2, textos históricos):
    só com novo pedido (layouts compartilhados: planejar e confirmar). Identificadores técnicos não mudam sem pedido.
16b. **Achados do acompanhamento de 23/10 (20:12)**:
    - alerta "inspeções da API Recintos: evento mais novo de 03:26" (17 h sem inspeção nova) — a marca d'água está
      em dia (0 não lidos): a carga da API parou de trazer inspeções. Conferir se o health check do dia seguinte
      acusa (componente API Recintos) e, se continuar, o extrator de inspeções;
    - dos 104 abertos além de 93 dias sem Ficha de FMA, **90 são "saída duvidosa (CE de outra família) — revisar"**
      de Suape (86) e Pecém (4): não são abandono, são ciclos que a heurística não fechou. Examinar 2 ou 3 com
      `caso_ciclo.py` e decidir se viram caso do corpus;
    - Pecém com 27% dos escaneamentos com imagem (crônico conhecido, P3).
17. **Qualidade das pesagens por recinto** (achado do usuário, 24/10/2026, na revisão dos sinais de declaração):
    MULTILOG, ECOPORTO e outros com muita variação na pesagem e quase sem pesar a tara do conjunto; suspeita de
    pesagem da tara transmitida com o `numeroConteiner` da viagem (par pesagem × declarado falso). Afeta o
    diagnóstico da pesagem no NAVI-I e qualquer uso da pesagem como verdade. Diagnóstico por recinto e cobrança
    (sem exceção por recinto no código). Detalhe em `analytic_scripts/declaracao/CONTEXTO.md` (seção 3).
    *Decisão: quando diagnosticar.*

## P3 — estacionados (dependem de terceiros ou de decisão futura)

- **Sinais da informação declarada (itens 16–18) — ADIADO em 24/10/2026** (revisão do usuário: os selecionados
  eram coerentes na imagem e na pesagem). Voltar do começo no futuro, sem as Fichas como rótulo positivo; ideia do
  usuário: rede neural que estime peso e volume do contêiner pela imagem, gravados no banco (há o modelo `peso` do
  AJNA-PADMA no legado). Lições, dados e ideias: `virasana/analytic_scripts/declaracao/CONTEXTO.md`.

- ICTSI com D/E trocados: a destroca está pronta e **desligada** — o recinto corrige o envio.
- Escâneres sem imagem no Mongo (9101602, 9981403, 9801303, 0301304) e Itaguaí sem acesso/pesagem: integração.
- Filtros por sócios/empresa: sem fonte de dados.
- Itens sem contêiner, ciclos sem CE (casar por contêiner + data: risco de contêiner reutilizado), ponte
  recinto ↔ terminal (abandonada).
- Fichas pelo NAVI-I, fase 3 (histórico das encerradas); retirar a tela legada da completude (com aval).
- `tox.ini` do bhadrasana2 (instala o `ajna_commons` de 2019, que passa na frente do vivo).
- Migração Flask/Python e da apresentação para o gov.br: só quando pedidas.
- Health check, próximos passos possíveis (`saude/CONTEXTO.md`): inspeções × imagens por recinto, erros 5xx do
  NAVI-I, disco e programas do supervisor.
- **Tela de abandono e risco de abandono** (ideia do usuário, 23/10/2026 — quando houver histórico bastante de
  ciclos v2 e de FMAs). Acompanhar as cargas paradas e, se der, prever o abandono antes dos 90 dias. Perguntas para
  o diagnóstico: aos 30 ou 60 dias sem retirada já dá para prever o abandono? Aos 90 dias, a FMA foi mesmo emitida?
  Quanto tempo entre o prazo e a FMA, e entre a FMA e a retomada ou o perdimento? Que sinais pesam (importador, NCM,
  recinto, valor, risco histórico)? Fontes: ciclo de vida, Fichas de FMA (`fma_update.py`, `tipooperacao = 0`) e o
  risco histórico.

## Decisões em aberto (resumo)

1. Priorizar o parser do Mercante já (P1.4): é a única perda irreversível da lista.
2. Aprovar a contagem assíncrona (P1.6).
3. Quando diagnosticar a qualidade das pesagens por recinto (P2.17).
4. Gravar o modo de exibição na coleta de uso (P2.11).
5. Quando começar unidade/equipe do usuário (P2.9).
