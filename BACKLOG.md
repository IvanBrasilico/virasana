# Backlog — NAVI-I, ciclo de vida, risco e busca (atualizado em 22/10/2026)

Junta as pendências que estavam espalhadas: `virasana/navii/CONTEXTO.md` ("Pendências" e "Próximos passos"),
`virasana/analytic_scripts/ciclo_de_vida/CICLO_DE_VIDA.md`, `virasana/risco_historico/CONTEXTO.md` e as notas das
sessões. O detalhe de cada item continua no documento de origem; aqui ficam o estado, a prioridade e a **decisão que
falta**. Ao fechar um item, mova-o para "Feito" com a data.

**Critério de prioridade**, nesta ordem: (1) dado errado ou perdido em produção; (2) operação e saúde — saber cedo
quando algo quebra; (3) validação com os usuários do que já está no ar; (4) desempenho percebido; (5) funcionalidade
nova.

## Feito (recente)

- **Filtros em linguagem natural** → **Busca por texto** (Meilisearch), aba `/navii/texto`, em produção desde
  19/10/2026. É busca por palavras tolerante a erro de digitação, não semântica.
- Ciclo de vida: porto do desembarque (C1–C3, caso CAAU5545018), C6/C7/C8, corpus de 26 casos reais, invariantes e
  auditoria da produção (`audita_ciclos.py`) — 22/10/2026.
- NAVI-I: modos de exibição (detalhado / simplificado / galeria), filtro de tráfego, atracação prevista no card,
  pesagens P1–P3, imagem pelo contêiner e data — 20 a 22/10/2026.
- Ciclo de vida fora de Santos (MultiRio, Suape, Pecém) e pesagens em produção — 21/10/2026.
- Gate de qualidade: `qualidade_navii.py --alterados` e `tox -e navii` isolado — 22/10/2026.
- **Health check diário v1** (`virasana/saude/saude_navi.py`, seção abaixo) e **refaz robusto** — feitos no código em
  23/10/2026 (aprovados pelo usuário, com as checagens da carga da API Recintos e das imagens no MongoDB que ele
  pediu). **Serviço e tela** (F1 e F2, decisões do usuário de 23/10: serviço próprio no supervisor, rodada diária às
  7 h e as pedidas pela tela, só o perfil Supervisor vê e comanda, histórico de 180 dias) — feitos no código no mesmo
  dia; falta publicar e ativar (P0.3).
- Nome NAVI no código novo: tela e relatório do health check dizem NAVI; programa e script `saude_navi` — 23/10/2026.

## P0 — agora

1. ~~Terminar o refaz de 22/10~~ — feito no servidor: a 1ª rodada do health check (23/10, `--nucleo-desde
   2026-10-22`) achou só 2 ciclos de 20/10 por refazer e 0 violações nas invariantes. Histórico: o `--refaz-desde 2026-09-22` rodou da estação de trabalho e perdeu a rede: 24 de
   41 partes falharam (a parte 3 às 16:22, conexão reiniciada; as partes 19 a 41 às 17:27, rede inacessível).
   Só 85 mil dos 202.692 contêineres foram refeitos. O refaz anda do desembarque mais recente para o mais antigo, e
   **todos** os 118 ciclos com violação na 2ª auditoria (113 "CE principal com a carga saindo do porto" e 9 "TRA
   concluído sem a saída do seco"; 4 com as duas) estão entre 22/09 18:10 e 09/10 03:58 — exatamente as partes que falharam. Nada indica
   defeito do C6/C7/C8. Refazer **no servidor** (rede estável), em sequência, e auditar de novo — esperado: 0.
   Sobrando alguma, extrair com `caso_ciclo.py` e levar ao corpus.
2. ~~Refaz robusto~~ — feito (23/10): a parte que falha é repetida até 3 vezes (30 s e 2 min de espera,
   reconectando); no fim, o log lista o período de desembarque de cada parte não refeita com o comando para
   refazê-la, e o programa sai com código 1.
3. **Health check v1** — feito no código (23/10), 1ª rodada no servidor no mesmo dia, calibrado. Para fechar o P0:
   - [x] refazer os 2 ciclos de 20/10 que sobraram — a 2ª rodada (23/10 10:50) mediu 0 não refeitos;
   - [ ] publicar a calibração, o alerta de abandono só dos não tratados e a composição dos abertos além do prazo
     (23/10);
   - [x] **serviço e tela, no lugar do cron** — feitos no código (23/10): programa `saude_navi` do supervisor
     (`servico_saude.py`, `supervisor_saude.conf`), histórico no Mongo com "novo" / "desde dd/mm" (`historico.py`) e
     a tela `/saude/` (`views_saude.py`, `templates/saude.html`; menu NAVII "Saúde do sistema (supervisores)");
   - [ ] ativar: publicar (o virasana web ganha a tela e a entrada no menu), criar `/var/log/virasana/saude/`,
     incluir `supervisor_saude.conf` no supervisor e iniciar `saude_navi`; conferir a 1ª rodada pela tela;
   - fora do código: cobrar MultiRio e ICTSI (embarques/desembarques parados) e ver a BTP3 (sem imagem).
   Depois, com 1 a 2 semanas de relatórios, calibrar os limiares (constantes no topo de cada módulo de
   `virasana/saude/`).

## P1 — próximas semanas

4. **Parser da carga do Mercante** (Pendência 5 do `navii/CONTEXTO.md`): `notify`/`cnpjNotify` (77%/53% dos CEs nos
   XMLs) chegam **vazios**; dados complementares do consignatário, observações e nome do embarcador com CNPJ se
   perdem. Os XMLs são apagados depois da carga: **cada dia sem a correção é dado perdido para sempre**. Afeta a
   Entregas por CNPJ e a Busca por texto. Precisa de `ALTER TABLE` (DBA). *Decisão: priorizar agora?*
5. **Revisita de imagens** (proposta de 13/10/2026): ~27% dos "sem imagem" têm a imagem chegando depois ao Mongo, e o
   serviço não volta aos ciclos concluídos. Com a galeria, a falta de imagem ficou muito visível. Proposta: revisitar
   a cada 6 h os ciclos sem imagem dos últimos 10 dias + `--completa-imagens` à mão. *Decisão: aprovar.*
6. **Contagem assíncrona no NAVI-I** (proposta de 02/10/2026): a contagem com NCM leva 18–20 s e segura a 1ª página.
   Mostrar os cards já e a contagem quando chegar. *Decisão: aprovar.*
7. **Validação com os usuários do que está no ar**: Busca por texto (F3), Estatísticas de uso (F3 em validação → F4
   ajustes), modos de exibição, diagnóstico das pesagens (digitação × possível fraude), Abrir Ficha e Fichas em
   acompanhamento. Saída: lista de ajustes priorizada.
8. **Índices e custo das consultas**: medir fora do pico (`navii/diagnostico_sql.py --analyze`, também
   `--so-ciclo` com e sem `--sem-familia`) e decidir com o DBA `ix_ciclo_ce_conteiner` e `ix_itens_ce_conteiner`
   (sugeridos, não criados).

## P2 — depois

9. **Unidade/equipe do usuário**: porto de atracação e recintos padrão pela unidade (hoje `PORTO_ESCALA_PADRAO` fixo
   em BRSSZ). Passa a importar com o ciclo já rodando em MultiRio, Suape e Pecém.
10. **Risco histórico**: F2 (validação no tempo, adiada em 14/10), normalizar o nome do embarcador (vem com
    endereço), ordenação por risco no NAVI-I (medir o cursor composto), F5 (analista desativa parâmetro).
11. **Coleta de uso × galeria**: na galeria vários cards ficam "vistos" de uma vez (a definição é a mesma). Gravar o
    modo de exibição na coleta para separar nas estatísticas? *Decisão.*
12. **Busca por texto**: CE alterado depois de indexado (hoje fica de fora); regra da data de referência (destino ×
    Santos).
13. **C4** (o CE declarado vence a única família plausível não declarada): só se a auditoria mostrar casos.
14. **C7 nos filtros por candidato**: contêiner + "só com desembarque" deixa de fora o BL único que não é o
    principal (atalho mantido por desempenho).
14b. **AJNA → NAVI no legado** (o sistema foi rebatizado em 23/10/2026: NAVI-I, NAVI-E e Navi-Fichas). Decidido
    pelo usuário em 23/10: grafia em maiúsculas; o Fichas fica Navi-Fichas; o código novo já diz NAVI (tela
    `saude.html` e relatório do health check) e o programa/script do health check virou `saude_navi`. O legado
    (`virasana/templates/layout.html` e ~29 templates do virasana, ~37 do bhadrasana2, textos históricos dos
    documentos) fica como está por enquanto — só com novo pedido (layouts compartilhados: planejar e confirmar).
    Identificadores técnicos (repositórios, `ajna_commons`, `ajna_docs`, URL `/ajna/` do manual, programas do
    supervisor já em produção) não mudam sem pedido.
15. **Pendentes do loop antigo**: `servico_ciclo_de_vida.py --reprocessa N`, se a seção 6 do acompanhamento ainda
    mostrar pendentes.

### Ideias de sinais de risco — informação declarada inventada (pedido do usuário, 22/10/2026)

Antes de implementar, cada uma precisa de um **diagnóstico nos dados** (como os F0 dos outros trabalhos). Código:
`virasana/analytic_scripts/declaracao/` — `regras_declaracao.py` (regras puras) e `diagnostico_declaracao.py` (só
leitura). Plano do passo 0 aprovado em 23/10/2026: um script, janela de 30 dias pela janela de ID; unidade o contêiner
cheio (FCL) na família master/filhote (17: só BL e MBL); referências = fichas positivas, RVFs e pesagens.

**Como validar (usuário, 23/10/2026): as Fichas são evidência FRACA** — são poucas e foram escolhidas por outros
critérios (a análise de risco delas não usava estas técnicas): força nelas não prova fraude e falta de força não
descarta um sinal. A decisão junta: (1) o sinal ser uma inconsistência em si (dado impossível ou copiado); (2) taxa de
marcação útil (0,5% a 5%); (3) corroboração independente quando existe (pesagem medida, peso da RVF); (4) as
referências comparadas com o **controle da mesma época** (vizinhos pelo ID), nunca só com a população recente;
(5) revisão de amostras no NAVI-I (imagens); e, se passar, (6) um piloto no NAVI-I como "sinal em observação",
medindo as Fichas abertas pelo card.

**1ª rodada em produção (23/10/2026, `declaracao.log`/`.csv`, 297 mil CEs, 317 mil contêineres FCL):**
- dados: números do Mercante sempre com ponto e 3 casas ("25040.000"); NCM com só 4 dígitos em 97% das linhas;
  cubagem vazia em 14% dos itens e em números redondos (estimada); embalagem vazia (94% no item; no NCM, 52% vazia e
  33% "99"); `codigoEmpresaNavegacao` = CNPJ da agência (99,8%); tempo: população 2,5 min, referências ~11 min;
- **descartados** como sinal de risco (decisão do usuário): 16a/16b (cubagem acima da capacidade, peso acima da carga
  máxima — erro de digitação, sem força nas referências; ficam contados como qualidade do dado); 16c/16d (pares raros
  carga × tipo e × embalagem — carga fora de gabarito legítima; embalagem inútil); 17b (cidade no número do BL — nenhum
  prefixo bate com a origem em 90%: HLCU 40%, ONEY 46%, MEDU 0%);
- **17a** (prefixo raro) parecia forte (35,8× nas positivas), mas era distorção: 1.054 das 1.471 marcações de uma
  agência que numera com ~100 prefixos (CNPJ 53364616000108; nenhuma referência) e TODO acerto em CEs antigos (2021,
  sobretudo) de empresas cuja numeração mudou — o mapa é dos últimos 30 dias. Agora só vale em empresa de prefixo
  dominante (90%+) e é medido contra o controle da mesma época;
- **18**: só a densidade ACIMA da típica do NCM separou (2,4% marcados; positivas 10,4×, RVF 4,1×, falsa declaração
  8,9×); abaixo, nada. Mas os acertos são todos antigos (27 de 36 positivas de 2021) e a força cai no corte mais alto —
  pode ser época (cubagem declarada de outro jeito), daí o controle. Peso atípico (18a) acompanha a pesagem
  divergente (1,9×; 15,6× com NCM de 8 dígitos): erro de declaração do peso, não fraude.

**2ª versão (23/10/2026), falta rodar**: controle da mesma época (vizinhos de cada referência pelo ID, `-150/+100/+250`,
de outra família) com "vezes o controle"; 18 por direção (`18a+/-`, `18b+/-`); 17a com prefixo dominante; e o sinal
novo **16e — cubagem copiada** (pedido do usuário): no CE, 2+ contêineres cheios com a mesma mercadoria (mesmo conjunto
de NCMs) e a MESMA cubagem, mas pesos ou volumes diferentes (> 10%) — no mínimo, dado inventado. Rodar (raiz do
virasana, no servidor): `virasana-venv/bin/python virasana/analytic_scripts/declaracao/diagnostico_declaracao.py
--saida declaracao2.log --csv declaracao2.csv` (`--sem-controle` pula os vizinhos).

16. **Tipo do contêiner × cubagem × carga × embalagem declaradas.** Inconsistência entre o que o contêiner comporta e
    o que foi declarado (ex.: cubagem acima da capacidade do 20', embalagem que não combina com a mercadoria ou com o
    tipo — granel em dry, carga refrigerada fora de reefer) sugere declaração inventada. Dados: `itensresumo.isoCode`,
    `cubagemM3`, `codigoTipoEmbalagem`, `qtdeItens`, `indicadorUsoParcial`; `ncmitemresumo.codigoTipoEmbalagem`,
    `qtdeVolumes`, `itemEmbaladoMadeira`; `conhecimentosresumo.cubagem` e `descricao`; tamanho/tipo em
    `navii/referencias/tipos_conteiner.json` (faltam as capacidades de referência). O diagnóstico responde: a cubagem
    vem preenchida e com valor real (ou padrão/zero)? Qual a distribuição por tipo de contêiner? Que tabela de
    embalagem o Mercante usa?
17. **Número do BL × origem e companhia declaradas.** O `conhecimentosresumo.numConhecimento` costuma trazer o prefixo
    da companhia (código SCAC, ex.: MAEU, MEDU, COSU) e às vezes o porto/local de emissão; deveria bater com a
    empresa de navegação (`codigoEmpresaNavegacao`), o porto de origem (`portoOrigemCarga`) e o terminal de
    carregamento. Formato estranho para a companhia, ou origem no número diferente da declarada, pode indicar
    informação falsa. O diagnóstico responde: os padrões de numeração por companhia e por tipo de BL (no HBL de
    agente o formato é livre — provavelmente só o MBL/BL serve), a tabela SCAC × empresa e a taxa de divergência
    "normal".
18. **Peso declarado × volume × densidade esperada pelo NCM.** Nos contêineres com **um só NCM**, a densidade
    (peso bruto declarado ÷ cubagem) pode ser comparada com a densidade típica daquele NCM, aprendida da própria base
    (mediana e dispersão por NCM, como os pesos do risco histórico); com mais de um NCM não há a quantidade de cada
    um, então fica de fora (ou só uma faixa possível). Vale cruzar com o **peso medido** das pesagens (P1–P3): a
    pesagem confirma ou derruba o declarado. O diagnóstico responde: quantos contêineres têm um só NCM, com cubagem
    preenchida, quantas amostras por NCM (em que nível — 4, 6 ou 8 dígitos) e quão estreita é a densidade de cada um.

## P3 — estacionados (dependem de terceiros ou de decisão futura)

- ICTSI com D/E trocados: a destroca está pronta e **desligada** — o recinto corrige o envio.
- Escâneres sem imagem no Mongo do AJNA (9101602, 9981403, 9801303, 0301304) e Itaguaí sem acesso/pesagem:
  integração.
- Filtros por sócios/empresa: sem fonte de dados.
- Itens sem contêiner, ciclos sem CE (casar por contêiner + data: risco de contêiner reutilizado), ponte
  recinto ↔ terminal (abandonada).
- Fichas pelo NAVI-I, fase 3 (histórico das encerradas); retirar a tela legada da completude (com aval).
- `tox.ini` do bhadrasana2 (instala o `ajna_commons` de 2019, que passa na frente do vivo).
- Migração Flask/Python e da apresentação para o gov.br: só quando pedidas.
- **Tela de abandono e risco de abandono** (ideia do usuário, 23/10/2026 — para o futuro, quando houver histórico
  bastante de ciclos v2 e de FMAs para analisar). Acompanhar as cargas paradas e, se der, prever o abandono antes dos
  90 dias. Perguntas para o diagnóstico: aos 30 ou 60 dias sem retirada já dá para prever o abandono, ou muitas cargas
  chegam lá e são retiradas (e não vale acompanhar como risco)? Aos 90 dias, a FMA foi mesmo emitida? Quanto tempo
  entre o prazo e a FMA, e entre a FMA e a retomada ou o perdimento? Que sinais pesam (importador, NCM, recinto,
  valor, risco histórico)? Fontes: ciclo de vida (estada, sem saída), Fichas de FMA (`fma_update.py`, `tipooperacao
  = 0`, com o CE) e o risco histórico.

## Health check (v1, feito em 23/10/2026)

**Um relatório curto por dia**, OK / ATENÇÃO / FALHA por componente, reaproveitando o que cada parte já mede. Só
leitura; cada consulta SQL com limite de 120 s; uma verificação que quebra vira FALHA dela e as outras seguem.
Limiares provisórios (constantes no topo de cada módulo), a calibrar com as primeiras semanas.
- **Serviço** `saude_navi` (supervisor; `servico_saude.py`): acorda a cada minuto, grava o batimento e roda às 7 h
  (uma vez por dia; se subir depois das 7 h, roda na hora) ou quando há pedido da tela. Fica fora dos serviços que
  vigia. Erro fora das verificações vira uma rodada com FALHA "Health check".
- **Histórico** no Mongo do virasana (`historico.py`): rodadas, pedidos e batimento; TTL de 180 dias. Cada achado é
  "novo" ou "desde dd/mm" (chave = o texto sem os números que variam; códigos de 7+ dígitos ficam) — trata os
  crônicos sem exceção por recinto.
- **Tela** `/saude/` (só perfil Supervisor, a regra das estatísticas de uso): estado geral, última e próxima rodada,
  serviço ativo ou parado, "Rodar agora"; um bloco por componente (cor + ícone + texto) com o resumo, "nesse estado
  desde", a faixa das últimas 14 rodadas e os achados marcados; histórico das rodadas e o detalhe de cada uma.
- **Linha de comando** `saude_navi.py` para rodar à mão (0/1/2; `--so`, `--nucleo-desde`, `--grava`).

| Componente | Medida | Alerta | Módulo |
|---|---|---|---|
| Mercante | CE e item mais novos carregados (pela chave primária) | > 6 h atenção; > 24 h falha | `fontes.py` |
| API Recintos | por tipo de evento: posição do extrator (ponteiro ou evento mais novo); por recinto: últimas 24 h × as **mesmas 24 h da semana anterior**; rejeições da carga | extrator > 6 h: falha; recinto ativo (≥ 50) com < 10%: atenção (com o último evento e o ponteiro dele); queda geral < 50% sem recinto que a explique; rejeições ≥ 100 e > 3× | `fontes.py` |
| Imagens (MongoDB) | imagem mais nova (`uploadDate`, UTC); por recinto: últimas 24 h × mesmas 24 h da semana anterior; atraso (chegada − escaneamento); ponteiro '99' do extrator | > 6 h: falha; recinto ativo (≥ 20) com < 10%; queda geral < 50%; atraso mediano > 48 h | `imagens.py` |
| Ciclo — serviço | alertas do acompanhamento (14 dias) | "serviço parado"/sem marcas: falha; outros: atenção | `ciclo.py` |
| Ciclo — invariantes | `audita_ciclos.Auditoria` nos desembarques das últimas 48 h | > 0 | `ciclo.py` |
| Ciclo — refaz | ciclos dos últimos 30 dias processados antes de `--nucleo-desde` | > 0 (com o comando do refaz) | `ciclo.py` |
| Ciclo — cobertura | % com CE e % dos escaneados com imagem, desembarques de 9 a 2 dias atrás × a semana anterior | queda > 10 pontos | `ciclo.py` |
| Pesagens | % possível fraude e % digitação da pesagem principal, 7 dias × 21 anteriores | > 2× (com ≥ 20 pesagens) | `ciclo.py` |
| Risco histórico | marca da extração, IDs do Mercante por ler, fila da pontuação, faixas | marca > 6 h; > 100 mil IDs; fila > 50 mil | `aplicacoes.py` |
| Busca por texto | Meilisearch no ar; IDs do Mercante por indexar (precisa de `NAVII_MEILI_CHAVE_ESCRITA`) | fora: falha; > 20 mil: atenção; sem chave: não medido | `aplicacoes.py` |
| NAVI-I | tempo no servidor da 1ª página das pesquisas de 24 h (a coleta de uso grava) | p95 > 10 s ou > 5% lentas (com ≥ 20 pesquisas) | `aplicacoes.py` |
| Fichas de FMA | Ficha de FMA mais nova (`fma_update.py` do Fichas) | > 3 dias | `aplicacoes.py` |

Próximos passos possíveis: cruzar, por recinto, inspeções transmitidas × imagens recebidas (precisa do de-para do
`metadata.recinto` para o código do recinto); erros 5xx do NAVI-I (log do gunicorn); espaço em disco e programas do
supervisor; aba "Saúde" no menu NAVII lendo o último relatório.

**1ª rodada em produção (23/10/2026 10:27 — ATENÇÃO)**. Achados reais:
- API Recintos: **MultiRio (7921303) e ICTSI (7921302) pararam de transmitir embarques/desembarques** — 0 nas últimas
  24 h contra 3.051 e 550 na semana anterior; último transmitido há 25 h e 35 h; ponteiro da extração andando (o
  extrator pergunta, o recinto não manda). O MultiRio está no escopo do ciclo de vida desde 21/10. Cobrar os
  recintos.
- Imagens: **BTP3 parou de mandar imagem** (10 nas últimas 24 h contra 1.382) — e os escaneados com imagem caíram
  de 90% para 82% na cobertura semanal (abaixo do limite de 10 pontos, mas na mesma direção).
- Refaz: 2 ciclos de 20/10 processados antes da troca do núcleo — sobra da parte 3 do refaz de 22/10 (o serviço não
  revisita concluídos): `--refaz-desde 2026-10-20 --refaz-ate 2026-10-21`.
- NAVI-I: 40 pesquisas, mediana de 6,1 s e p95 de 10,5 s no servidor só na 1ª página — reforça a contagem
  assíncrona (P1.6).
- Invariantes: 12.492 ciclos das últimas 48 h, **0 violações** (C6/C7/C8 em produção).

Calibração feita no mesmo dia: fila da pontuação do risco (327 mil) só conta 6 h depois da última versão dos
parâmetros (a versão nova repontua todos os CEs); relógio do Mongo segundos adiantado ("há -0,0 h"); sem ponteiro
'99' das imagens no controle, o resumo não o cita.

Crônicos (alertas do acompanhamento que vão se repetir todo dia):
- **98 ciclos v2 abertos há mais de 93 dias** ("deviam ser abandono"; 99 na 2ª rodada, **nenhum** com Ficha de FMA —
  hipótese: os ciclos de MultiRio, Suape e Pecém criados pelo refaz desde 19–20/07, operadores que não transmitem a
  saída, fora dos recintos de onde o `fma_update.py` baixa FMAs (os com `cod_dte`); o acompanhamento passou a mostrar
  a composição deles para confirmar) — **ajustado em 23/10** (pedido do usuário): o
  abandono com Ficha de FMA (aberta pelo `fma_update.py` para o CE do ciclo ou outro CE da família) já está em
  tratamento; o acompanhamento mostra quantos têm e quantos não têm Ficha, e só alerta os sem. Se ainda sobrarem: o
  alerta conta pelo desembarque e o núcleo fecha pelo início da estada atual — ver se `processado_em` é recente
  (revisitados e mantidos abertos: estada mais nova) ou velho (não revisitados):
  `SELECT c.codigoRecintoDesembarque, c.fluxo, c.motivo, COUNT(*), MIN(c.data_desembarque), MAX(c.processado_em)
  FROM ciclodevida_ciclo_de_vida_importacao c WHERE c.situacao = 'aberto' AND c.versao_heuristica = 'v2' AND
  ((c.data_saida IS NULL AND c.data_entrada_seco IS NULL AND c.data_desembarque < NOW() - INTERVAL 93 DAY) OR
  c.data_entrada_seco < NOW() - INTERVAL 93 DAY) GROUP BY 1, 2, 3 ORDER BY 4 DESC;`
- **Pecém (3111401) com 21% dos escaneamentos com imagem**: conhecido (imagens com +3 h e escâneres que não mandam
  ao Mongo — "Fora de Santos e pesagens" no CICLO_DE_VIDA.md).
- *Decisão:* como tratar o crônico — (a) histórico das rodadas, marcando cada achado como "novo" ou "desde dd/mm" e
  só subindo o código de saída com achado novo ou pior; (b) lista de achados conhecidos com motivo e validade; (c)
  deixar como está.

## Decisões em aberto (resumo)

1. ~~Refazer as partes que falharam (P0.1)~~ — feito no servidor (a 1ª rodada do health check só achou 2 ciclos de
   20/10 por refazer, da parte 3).
2. Onde o health check aparece além do log diário: e-mail do cron, aba "Saúde" (P0.3); e como tratar os achados
   crônicos — histórico "novo / desde", lista de conhecidos ou nada (ver "1ª rodada em produção").
3. Priorizar o parser do Mercante já (P1.4): é a única perda irreversível da lista.
4. Aprovar a revisita de imagens (P1.5) e a contagem assíncrona (P1.6), propostas desde 13/10 e 02/10.
5. Gravar o modo de exibição na coleta de uso (P2.11).
6. Quando começar unidade/equipe do usuário (P2.9).
