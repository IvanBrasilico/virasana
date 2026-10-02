# Ciclo de Vida da importação — fluxos, revisão do código e plano

> Contexto durável (01/10/2026). Ler antes de mexer em `manager_ciclo_de_vida.py`,
> `ciclo_de_vida_helpers.py` ou `ciclo_de_vida.py`. Usado pelo NAVI-I (cards, filtros "com imagem",
> "entregue", recinto) e pela tela de Completude de escaneamentos.

## Para que serve

Amarrar cada **contêiner declarado num CE de importação** aos eventos da API Recintos da sua passagem
pelo porto — desembarque, escaneamento, saída, entrada/saída em recinto seco — e, principalmente, à
**imagem de escaneamento** (Mongo/GridFS). Um ciclo = (CE, contêiner, um desembarque).

## Fluxos (como o usuário descreveu)

```
A) Desembaraço no operador
   CE emitido (numeroCEmercante)
   → desembarque do contêiner no operador (EmbarqueDesembarque D; Item.codigoConteiner)
   → escaneamento no operador (InspecaoNaoInvasiva)
   → saída do operador (AcessoVeiculo S)                              → CONCLUÍDO

B) Remoção para recinto seco (TRA / zona secundária)
   CE emitido → desembarque no operador
   → saída do operador (AcessoVeiculo S)
   → entrada no TRA seco (AcessoVeiculo E, outro recinto)
   → escaneamento no TRA seco (InspecaoNaoInvasiva)
   → saída do TRA seco (AcessoVeiculo S)                              → CONCLUÍDO
```

Exceções conhecidas (ainda NÃO tratadas pelo código):

| Exceção | Como aparece nos eventos | Efeito hoje |
|---|---|---|
| **Escaneamento terceirizado** (o operador/TRA escaneia em outro recinto) | `InspecaoNaoInvasiva.codigoRecinto` ≠ recinto do ciclo | não é achado pela busca por recinto + janela |
| **Data do escaneamento errada** | `dataHoraOcorrencia` fora da janela desembarque→saída (±1 h) | não é achado |
| **Recinto informado errado** | idem ao terceirizado | não é achado |
| **Contêiner reaproveitado** (sai entregue e entra vazio num TRA/REDEX para estufagem de exportação) | acessos e escaneamentos do mesmo número de contêiner, de OUTRA operação, logo depois | risco de vincular escaneamento/entrada de outra operação; `vazio` (AcessoVeiculo.vazioConteiner, Inspecao.vazio) existe mas a confiabilidade é desconhecida |
| **Passagem** (tipoTrafego 9) | "saída" é um EmbarqueDesembarque **E** (embarque) no mesmo ou em outro navio | ciclo nunca conclui pela regra (só pelos 93 dias) |
| **Passagem com baldeação** | desembarque num operador, embarque em outro operador | idem |
| **Cabotagem** (tipoTrafego 3) | escaneamento nem sempre obrigatório | conta como "sem escaneamento" |
| **Permanência longa / abandono** | sem saída até 90 dias (depois, abandono — continua sem saída) | busca de saída só olha 72 h depois do desembarque |

## Algoritmo atual (resumo de `manager_ciclo_de_vida.__main__`, a cada 10 min)

1. `desembarques_novos` — cria um ciclo por desembarque novo (D) nos 3 operadores de Santos (`OPERADORES`).
2. `saidas_novas_operadores` — atalho: casa saídas dos operadores com ciclos.
3. `busca_escaneamentos_novos` — atalho: inspeções novas com CE informado → ciclo do mesmo CE + contêiner.
4. `processar_ciclos_pendentes` — percorre TODOS os ciclos não concluídos (saltos aleatórios de ID) e,
   para cada um, `completar_ciclo_de_vida`: CE (contêiner + escala prevista ±5 dias), saída
   (até 72 h), escaneamentos (recinto do ciclo + janela), imagens (Mongo, contêiner ±4 h), entrada
   seco (outro recinto, até 6 h após a saída), saída seco (até 30 dias), e `check_ciclo_concluido`.
5. `busca_escaneamentos_faltantes_por_ce` — inspeções pelo CE para ciclos sem escaneamento.

## Revisão do código — problemas encontrados

Correção (bugs):

1. **`saidas_novas_operadores` relê sempre as mesmas 2.000 saídas**: calcula `max_id_saida_operador`
   mas não filtra por ele (`order_by(id).limit(2000)` desde o início da tabela).
2. **…e grava `saida_id` sem `data_saida`**. Depois, `check_ciclo_concluido` faz
   `data_saida + timedelta(...)` com `None` → `TypeError`, e `get_entrada_seco` busca com data `None`.
   Esses ciclos ficam presos para sempre (e podem derrubar o loop). *Medição de 01/10: 0 casos — na
   prática o atalho não casa nada, porque só relê saídas antigas (bug 1); é código morto.*
3. **`.one_or_none()` com mais de um ciclo** para o mesmo contêiner/CE (retransmissões, desembarques
   repetidos) levanta `MultipleResultsFound` em `saidas_novas_operadores` e `busca_escaneamentos_novos`
   — sem `try`, a exceção derruba a iteração do loop.
4. **Saída só até 72 h** (`HORAS_PERMANENCIA_MOLHADO`) depois do desembarque: contêiner que fica mais
   tempo no operador (até 90 dias) nunca ganha saída → ciclo aberto até os 93 dias.
5. **Agendamento conta como saída/entrada**: as buscas de AcessoVeiculo não filtram `operacao = 'C'`
   (acesso efetivado) — um agendamento (G) pode virar a "saída"; também não tratam retificações.
   *Medição de 01/10: 0 casos — os operadores só transmitem acessos C e quase nenhuma retificação.
   Manter o filtro por segurança, sem prioridade.*
6. **Transit time de 6 h decide o caminho**: saída sem entrada em seco em 6 h → concluído como
   "entregue pelo operador"; se a entrada no seco vier depois (trânsito lento), o fluxo TRA é perdido.
   E a entrada no seco não olha se o contêiner entrou **vazio** (depósito de vazios/REDEX).
7. **`busca_escaneamentos_faltantes_por_ce` olha sempre os mesmos 1.000** ciclos mais recentes sem
   escaneamento; os mais antigos nunca são vistos. `session.merge` de objeto sem ID insere duplicatas
   se a inspeção já estiver em outro ciclo.
8. **`busca_desembarque_conteiner`** compara `>= data` e `<= data` (igualdade); `inicio`/`fim` não usados.
9. **CE escolhido pela escala ±5 dias, o mais antigo**, sem desempate quando há mais de um CE para o
   contêiner e ignorando o tráfego (passagem/cabotagem). O `numeroConhecimento` dos eventos poderia
   servir de desempate — mas é declarado, não 100% confiável (ver "Confiabilidade dos dados").
10. **Fuso horário**: a API grava a hora local (o fuso é descartado em `converte_datetime`); a busca no
    Mongo trata essa hora como UTC — a janela de ±4 h fica deslocada em ~3 h.
11. `func` só existe quando o arquivo roda como script (import dentro do `__main__`).

Custo (por que o loop leva horas):

- **Sem memória de tentativas**: cada ciclo pendente é revisitado a cada volta com as MESMAS consultas
  (CE, saída, 2 buscas de escaneamento, Mongo), mesmo sem nada novo. Não há "próxima tentativa em",
  nem número de tentativas, nem motivo de pendência.
- **Pendentes que nunca concluem** (passagem, baldeação, abandono, saída > 72 h, bug do `data_saida`)
  se acumulam e entram em todas as voltas.
- Commit a cada passo de cada ciclo.

## Confiabilidade dos dados (orientação do usuário, 01/10/2026)

| Dado | Confiança | Por quê |
|---|---|---|
| `numeroConteiner` e `dataHoraOcorrencia` dos eventos da API | **alta** | informados automaticamente / por OCR |
| `numeroConhecimento` (CE) e demais campos **declarados** nos eventos da API | **não é 100%** | digitados/declarados pelo recinto; servem para **corroborar** e desempatar, nunca como verdade única |
| Mongo `metadata.numeroinformado` e `metadata.dataescaneamento` | **usar** | dados da própria imagem |
| Mongo, demais campos de `metadata` (`carga.*`, `recinto`, `predictions.*`...) | **NÃO usar** | preenchidos pela heurística do `virasana_periodic`, que será **substituída** justamente pela heurística do ciclo de vida |

## Escaneamento: como achar sem pegar o de outra operação

A âncora é sempre **contêiner + tempo** (os dados confiáveis); o resto corrobora ou desconfia:

1. **Janela do fluxo, recortada pelos eventos do próprio contêiner**: desembarque → saída do operador
   (fluxo operador) ou entrada → saída do seco (TRA), e **nunca depois do primeiro evento que indica
   outra operação** do mesmo contêiner (novo desembarque, embarque, entrada/saída de outra passagem).
   Dentro da janela, a inspeção vale **em qualquer recinto** (resolve o escaneamento terceirizado e o
   recinto informado errado).
2. **Corroboração** (aumenta a confiança, registrada como tal): inspeção com o mesmo CE do ciclo;
   recinto da inspeção igual ao do ciclo.
3. **Desconfiança** (baixa a confiança ou exclui, a decidir com os números): inspeção com OUTRO CE,
   `vazio`/`vazioConteiner` informado, entrada no seco com o contêiner vazio.
4. **Data informada errada**: inspeção do contêiner um pouco fora da janela, sem outra operação no
   meio — candidato "fraco", com a defasagem registrada.

A imagem continua sendo achada no Mongo só por `numeroinformado` + `dataescaneamento` (com o fuso
corrigido), a partir da inspeção ligada.

Sinais de "outra operação" a medir antes de confiar: `vazio`/`vazioConteiner`, inspeção com outro CE,
novo desembarque ou embarque do contêiner. (A seção 11 do diagnóstico mostra os outros campos do Mongo
só como informação — eles não entram na heurística.)

## Primeira medição em produção (01/10/2026, desembarques de 03/06 a 01/10)

Log: `virasana/ciclo_de_vida_diagnostico_20261001_152637.log`.

- **339 mil ciclos, 227 mil pendentes (67%)**; 140 mil com 30 a 93 dias. Os pendentes são quase só
  "sem saída" (217 mil) e TRA aguardando a saída do seco (~11 mil): todo ciclo com saída e sem entrada
  no seco é fechado em 6 h.
- **Atraso na criação dos ciclos**: o ciclo mais novo (ID 339.370, quase o máximo) é de um desembarque
  de 26/09, e o diagnóstico rodou em 01/10 — nenhum pendente com menos de 3 dias. `desembarques_novos`
  só roda uma vez por volta (que leva horas) e pega no máximo 2.000.
- **52% dos ciclos sem CE** (177 mil). Os desembarques trazem CE declarado; nas amostras sem CE ligado,
  os declarados começam com 10, 13, 18 ou 00 (os ligados, com 15) — hipótese a verificar: CEs de
  passagem/transbordo emitidos fora, ausentes do `conhecimentosresumo` ou fora da janela de escala.
- **CE de outro tipo**: 7,7 mil ciclos de importação ligados a CE de **exportação** (tráfego 7) — o
  `get_conhecimento` não filtra o tráfego e pega o contêiner reaproveitado.
- **Passagem/transbordo**: 12,4 mil ciclos de tráfego 9, só 25 concluídos. 128 mil pendentes têm
  embarque posterior no MESMO operador e 38 mil em outro. Amostras: desembarque → embarque 4 dias depois,
  **mesmo CE declarado nos dois eventos e nenhum acesso de gate no meio** (o contêiner não saiu).
- **Saídas além de 72 h**: 88 mil pendentes têm saída (acesso C) no operador até 93 dias — mas, pelo
  princípio abaixo, só valem com validação de que é a mesma operação. A permanência medida (só < 3
  dias) é viés da janela atual.
- **Trânsito > 6 h**: ~1,1 mil contêineres cheios entraram em outro recinto 6 a 72 h depois da saída
  (TRA perdido) e ~1,2 mil **vazios** (devolução de vazio — outra operação, o risco descrito pelo usuário).
  428 ciclos têm como "entrada no seco" uma entrada com o contêiner vazio.
- **Entrada em OPERADOR tratada como TRA**: o 8931318 é o **ECOPORTO SANTOS, operador**
  (`ovr_recintos.e_operador`; pode registrar EmbarqueDesembarque) — correção do usuário, 01/10. Nas
  amostras, o contêiner sai do operador, entra e sai do Ecoporto em ~9 min e segue para o TRA (8931339,
  8931304); o algoritmo, que toma QUALQUER entrada em outro recinto como seco, fechou o "TRA" no
  Ecoporto. O Ecoporto também aparece inspecionando contêineres de outros operadores (escaneamento
  terceirizado). E a lista fixa `OPERADORES` do loop (8931356, 8931359, 8931404) **não inclui o
  Ecoporto**: desembarques lá não abrem ciclo. Usar `e_operador` em vez da lista fixa.
- **Scanner no gate do TRA**: a inspeção vem minutos ANTES da entrada no TRA (ex.: inspeção 17:04,
  entrada 17:14).
- **Recuperação pelo CE é pequena**: 3,4 mil dos 169 mil ciclos sem escaneamento têm inspeção com o
  próprio CE (2,6 mil em outro recinto). O grosso dos "sem escaneamento" é de pendentes sem saída,
  passagem e cabotagem (209 de 15,7 mil cabotagens têm escaneamento).
- **Ligações já feitas que desconfiam**: ~24,6 mil (14%) com inspeção declarando OUTRO CE (pode ser o
  master/filhote — `Conhecimento.numeroCEMaster` existe — a verificar) e ~32 mil com inspeção vazio = 1.
- **Inspeções de vazio são 30 a 45% nos operadores** (ex.: 8931356: 80 mil de 196 mil): janela larga
  só por contêiner + data pega vazio com facilidade.
- **Fuso confirmado**: imagem (Mongo) = inspeção (API) **+3 h** em 291 de 300 (8 com +0 h — ver por
  recinto). O Mongo está em UTC e a API em hora local.

## Segunda medição — CEs e operadores (01/10/2026, `diagnostico_ces_ciclo_de_vida.py`)

Log: `virasana/ciclo_de_vida_ces_20261001_160742.log` (desembarques de 02/08 a 01/10, amostras de 20 mil).

- **Hipótese do usuário confirmada — os eventos informam master e house**: nas 13,7 mil inspeções já
  ligadas, 75% declaram o mesmo CE do ciclo; das que declaram OUTRO CE, **~88% são da família**
  (filhote 1.594, master 345, irmãos 31); sem parentesco são poucas (12 sem relação, 33 do mesmo
  manifesto, 186 fora do Mercante). Não é, em geral, bug do algoritmo antigo.
- **Por que o filhote aparece tanto**: o contêiner consta nos itens do master E do filhote (6,8 mil dos
  20 mil); o `get_conhecimento` ordena por emissão e fica com o **master** (36% dos ciclos com CE), e o
  recinto declara o **house** na inspeção.
- **Impacto no NAVI-I**: o card é um `Item` (CE + contêiner) e o NAVI-I liga o ciclo pelo CE **exato**
  (`_mesmo_conteiner_do_ciclo`). Quando o ciclo guarda o master, o card do **filhote** — o do
  consignatário/importador real — não acha o ciclo nem a imagem (e vice-versa).
- **CE declarado nos eventos** (3 mil ciclos, 10 dias após o desembarque): ausente em ~50% dos
  desembarques e saídas e em ~9% das inspeções; quando presente, quase sempre igual ao do ciclo. Por
  ciclo: 66% só o CE do ciclo, 25% nenhum declarado, **5% trocam master x house**, 3% declaram CE sem
  parentesco/inexistente (os casos a desconfiar).
- **`vazio` sozinho não indica outra operação**: 322 inspeções ligadas com o MESMO CE e 186 com o
  filhote estão marcadas vazio (desunitização no recinto, ou flag errada) — sinal fraco.
- **Tráfego do CE do ciclo**: 6,4% são de **exportação** (1.289 de 20 mil) — escolha errada do
  algoritmo; 8,5% passagem; 5,9% cabotagem.
- **Ciclos sem CE (20 mil)**: 55% sem CE declarado no desembarque (só o Mercante por contêiner resolve);
  **~45% têm CE declarado que existe no Mercante com o contêiner nos itens** — metade com a escala a
  até 5 dias (o algoritmo deveria ter achado: o CE/itens chegam ao Mercante depois, o ciclo já
  concluiu ou não foi revisitado — os pendentes são visitados com saltos aleatórios) e ~40% com a
  escala prevista a **6–15 dias** (janela de ±5 dias apertada); 776 declaram CE fora do Mercante.
- **Prefixos 00/10/13/18/12...** são CEs que **existem no Mercante** (emitidos em outros portos —
  transbordo/cabotagem): a hipótese de que ficavam fora estava errada.
- **Operadores**: a API é nacional (Pecém, Rio, Suape, Itaguaí...). Em Santos, o loop tem Santos Brasil
  (8931356), BTP (8931359) e Embraport (8931404); falta o **Ecoporto (8931318)**, ~3,3 mil desembarques
  em 60 dias. Há códigos com muito movimento sem cadastro em `ovr_recintos` (9801303, 9981403, 9101602,
  0301304 — fora de Santos).

## Terceira medição — primeira rodada da sombra (01/10/2026, `sombra_ciclo_de_vida.py`)

Log: `virasana/ciclo_de_vida_sombra_20261001_171744.log` + `sombra.csv` (3.000 contêineres, 3.031 ciclos).

- **Amostra viciada — corrigida no script**: pegava os contêineres de maior id; 3.004 dos 3.031 ciclos
  eram de 30/09 e 01/10, que o loop antigo **ainda não tinha criado** (só 27 com ciclo atual). A
  comparação ficou inconclusiva. Agora a amostra é espalhada pelo período (hash do id, reprodutível) e o
  período termina 7 dias antes de hoje (`--ignorar-ultimos-dias`).
- **Volume x loop antigo**: ≥ 2,2 mil contêineres desembarcados por dia nos operadores de Santos (só no
  dia 30/09 da amostra); o loop cria no máximo 2.000 ciclos por volta e cada volta leva horas — o atraso
  é estrutural. A seção 1 agora mede: desembarques por dia x com ciclo e a data do último desembarque
  com ciclo.
- **Ecoporto**: 23 desembarques, nenhum com ciclo atual (confirma a falta na lista fixa do loop).
- **Passagem conclui**: 79 ciclos fechados por "embarque no mesmo operador" (o loop antigo nunca fecha);
  46 deles sem CE resolvido.
- **Ligações de inspeção**: 901 alta, 49 média, 2 baixa (de 952) — quase todas no recinto da etapa e com
  o CE declarado da família; 140 até 1 h **antes** do desembarque no mesmo recinto (relógio/scanner no
  cais) e 49 no gate do TRA.
- **CE**: 31% dos ciclos novos sem CE resolvido (941 de 3.031).
- **Peso x tara no desembarque**: peso - tara <= 0 em 1.924 de 3.031 — a balança parece não medir (peso
  zero?) ou o contêiner veio **vazio**. Hipótese a medir: boa parte dos desembarques é de contêiner
  vazio (o Brasil importa vazios para exportar cheios), sem CE de importação. A seção 8 agora separa
  peso zero de peso <= tara e cruza com o manifesto do Mercante (`conteinervazioresumo`: contêiner
  declarado vazio no manifesto, com escala perto do desembarque) e com o CE resolvido.
- **Mongo: o +3 h não vale para todos** — com a imagem esperada em inspeção + 3 h, só 76 de 300 ficaram a
  até 30 min; 206 a 2–6 h (imagem no mesmo relógio da API?). A medição antiga (+3 h em 291 de 300) foi
  feita nas imagens já ligadas pelo loop (janela ±4 h, `find_one`), outro período e só 3 operadores. A
  seção 9 agora mede a defasagem **por recinto**, pela imagem mais próxima numa janela de ±8 h.
- **Desempenho**: a rodada inteira (3 mil contêineres, todos os eventos, Mercante e 300 consultas ao
  Mongo) levou segundos — carregar por lote de contêineres é viável para o serviço por eventos.

## Quarta medição — segunda rodada da sombra (01/10/2026, amostra espalhada)

Log: `virasana/ciclo_de_vida_sombra_20261001_173024.log` + `sombra.csv` (desembarques de 25/08 a 24/09;
3.000 contêineres, 3.006 ciclos, **2.970 com ciclo atual**).

- **Cobertura do loop antigo**: 3 a 8 mil desembarques por dia nos operadores de Santos; o loop cria ciclo
  para praticamente todos os dos 3 operadores da lista, com **atraso estável de ~4 dias** (corrige a
  leitura da 3ª medição: ele acompanha o volume). Na amostra, os 36 sem ciclo são todos do **Ecoporto**.
- **Ganhos da heurística nova** (nos 2.970 com ciclo atual): ~1.450 pendentes passam a fechar — passagem
  702 (embarque no mesmo operador), entregue pelo operador 452 (saída depois das 72 h), baldeação 200,
  TRA 29, incompletos por fronteira 67; **760 saídas recuperadas** (764 a mais de 72 h do desembarque);
  **+683 escaneamentos** em 582 ciclos (19 perdidos); CE: 779 novos onde não havia, 435 trocados pelo
  filhote da mesma família, 876 iguais, **82 perdidos**, 3 diferentes.
- **Defeito do núcleo corrigido**: 70 ciclos ficavam "abertos no operador" porque o contêiner, depois de
  sair, **voltou ao mesmo operador do desembarque** (quase sempre vazio, dias depois — outra operação).
  Agora é fronteira: o ciclo fecha na saída. Também: saída duvidosa (CE de outra família) não fecha
  mais o ciclo por causa de uma entrada vazia depois.
- **Escaneamentos perdidos (19) = CE declarado "de outra família"**, vários no cais minutos depois do
  desembarque. Causa provável: a família não incluía os **irmãos** (filhotes do mesmo master sem o
  contêiner nos itens). Agora inclui; a sombra classifica a relação de cada conflito para confirmar.
- **Ecoporto como "seco" no loop antigo**: os 50 "concluído (saiu do seco)" que viram "entregue pelo
  operador" e os 19 que viram "TRA aberto" são, nas amostras, passagens de **minutos** pelo Ecoporto
  (BTP sai → Ecoporto entra e sai → TRA entra, tudo em ~40 min); o loop antigo tomou o Ecoporto como
  seco e perdeu o TRA real. A sombra agora mostra o recinto da entrada no seco antiga.
- **CEs perdidos (82)**: quase todos com prefixo 12/10 (emitidos em outros portos), em passagem e com
  mais de 2 t — provável transbordo; o motivo (fora do Mercante, contêiner fora dos itens, escala longe,
  exportação...) agora é medido.
- **Contêiner vazio**: 18% dos desembarques constam vazios no manifesto do Mercante (534 de 3.006), 87%
  deles sem CE — explicam metade dos ciclos sem CE. O **peso de manifesto − tara** do evento separa bem
  (vazio: ≤ tara ou < 500 kg em 80%; não vazio: > 2 t em 83%); a **balança** quase nunca pesa (sem peso
  ou zero em ~75%). Proposta: marcar o ciclo como **vazio** (manifesto + peso) e não cobrá-lo como
  pendência nem exigir escaneamento — decisão do usuário.
- **Mongo: defasagem fixa POR RECINTO** (resíduo ≤ 10 min em ~99%): **+3 h** — 8931304, 8931305
  (Transbrasa), 8931318 (Ecoporto), 8931339, 8931342 (Marimex), 8931356 (Santos Brasil), 8931359 (BTP),
  8933202 (Eudmarco), 8933206 (Deicmar Alemoa); **+0 h** — 8931309 (Movecta), 8931364 (Deicmar
  Outeirinhos), 8931404 (Embraport), 8933001 (Localfrio), 8933204 (Santos Brasil CLIA). Sem imagem em
  ±8 h: BTP 22%, Embraport 12%. A busca da imagem deve usar a defasagem do recinto com janela estreita
  (±15 min); proposta: o serviço recalibra a tabela sozinho (moda por recinto nas inspeções recentes).
- **Recintos que não informam a entrada** ("saída sem entrada"): 8931305 Transbrasa 26, 8931318 Ecoporto
  25, 8932794 9... — hoje a saída é ignorada e o ciclo fecha como entregue pelo operador. Possível regra
  (a decidir): saída num seco durante o trânsito, dentro do prazo, vale como TRA com entrada omitida
  (confiança média).
- **Desempenho**: ~1 min no total (consultas de até 3 s; Mongo 18 s para 600 inspeções).

## Quinta medição — terceira rodada da sombra (02/10/2026, núcleo com a volta ao operador e os irmãos)

Log: `virasana/ciclo_de_vida_sombra_20261002_090605.log` (desembarques de 26/08 a 25/09; 3.005 ciclos,
2.969 com ciclo atual).

- **Atraso do loop antigo**: desta vez 0 dias (alcançou) — varia de 0 a ~5 dias entre as medições.
- **Volta ao operador do desembarque**: 235 ciclos (8%) passam a fechar na saída (antes, uma parte ficava
  "aberta no operador"). É o padrão "importou cheio, devolveu o vazio no mesmo terminal".
- **CEs atuais "perdidos" (84)**: 83 são CEs de **exportação** que o loop antigo ligou a desembarques
  (quase todos em passagem: o CE do reembarque) — o descarte está certo. Observação a confirmar: os
  dígitos 5–6 do número do CE parecem indicar o tráfego (05 importação, 07 exportação, 03 cabotagem).
- **"Outra família" eram números fora do Mercante**: 44 saídas, 9 inspeções, 2 entradas e 2 embarques
  declaravam um número que nem existe no Mercante; os casos de fato "sem relação" são poucos (23
  embarques com CE de **exportação** — reembarque, outra operação). Ajuste: CE declarado fora do
  Mercante só não corrobora (confiança média); não derruba a ligação.
- **Escaneamento fora antes do TRA não é só no Ecoporto**: passagens por operador — Ecoporto 88, Santos
  Brasil 41, Embraport 31, BTP 3. O loop antigo grava essas passagens como "entrada no seco" (Embraport
  42, Santos Brasil 53, Ecoporto 86, BTP 9 casos removidos/trocados pela nova).
- **Irmãos na família**: 799 irmãos carregados; os escaneamentos perdidos caíram de 19 para 13.
- **Mongo**: a defasagem por recinto se manteve (+3 h / +0 h, resíduo ≤ 10 min).

## Sexta medição — quarta rodada da sombra (02/10/2026, entrada implícita e passagens rápidas)

Log: `virasana/ciclo_de_vida_sombra_20261002_095406.log` (mesma amostra da quinta medição).

- **Entrada implícita**: 25 entradas no seco deduzidas de inspeção/pesagem (5.131 pesagens carregadas);
  os alertas "saída em X sem entrada" da Transbrasa (26) e do Ecoporto (24) viraram "entrada não
  informada — deduzida"; sobram poucos sem indício (8932794: 7).
- **Escaneamentos perdidos: 13 → 4** — os 4 são inspeções em OUTROS portos (Rio, 0301304) depois do
  reembarque, que o loop antigo tinha ligado: não ligar está certo. Confiança baixa: 4 inspeções.
- **Passagens rápidas medidas** (entrada e saída em até 6 h, seguidas de outra entrada): Ecoporto 66
  (63 em até 30 min, **64 com inspeção**) e Santos Brasil 23 (todas com inspeção) — confirmam o
  escaneamento terceirizado. **MULTILOG não apareceu** nesta amostra (os 4 casos dela são outro
  padrão), nem outro recinto do Ecoporto.
- **Escala declarada no desembarque confere** com o manifesto do CE principal em 2.008 de 2.034 e com
  o do vazio em 423 de 539 — bom corroborador (também para achar o manifesto do vazio).
- **Tipo do contêiner**: 45G1 (1.297), 22G0, 45G0, 45R1...; open top 21 e plataforma 7 (potencial OOG,
  16 sem escaneamento); `22K1`/`22K2`/`22K0` (215 ciclos), fora da ISO 6346, são **tanques**
  (usuário: `22K1` = tanque pressurizado). Nos desembarques de 30 dias, potencial OOG = 0,9%.
- Consulta de pesagens: 5–8 s por lote (a mais lenta da sombra) — atenção no serviço por eventos.

## Decisões do usuário (02/10/2026)

- **Contêiner vazio**: também precisa ser escaneado, mas não tem CE — tem **manifesto**. Monitorar e
  completar o ciclo do vazio trocando o CE pelo manifesto (não é prioridade). Guardar no ciclo: **campo
  de manifesto e de vazio**, **peso, tara, peso − tara**, e o **tipo do contêiner** — plataforma e open
  top podem ser **OOG** (fora de gabarito: não passam nos gates padrão nem no escâner). **A interface deve
  mostrar isso e permitir excluir** (ex.: tirar os potenciais OOG da cobrança de escaneamento).
- **Entrada não informada**: se o recinto informa **pesagem ou escaneamento** e não informou a entrada
  antes, considerar que houve uma entrada nele naquele momento. Implementado no núcleo para o trânsito,
  quando o recinto informa depois a saída (fecha a estada); sem saída nem outro evento, a inspeção fica
  ligada como "em trânsito" e o ciclo fecha pelo prazo — senão ficaria aberto até o abandono.
- **Ecoporto / MULTILOG**: as passagens rápidas antes da entrada noutro TRA são o **escaneamento
  terceirizado**. A linha do tempo mostra esse escaneamento (marcado "escaneamento fora") antes da
  entrada no TRA; os **acessos** dessa passagem **não** entram no ciclo (omissões frequentes e poluiriam
  a estrutura). No núcleo: passagem por operador (já existia) e estada curta (até 3 h) num recinto não
  operador seguida de outra entrada em até 6 h. A sombra mede as passagens rápidas por recinto para
  confirmar MULTILOG e o outro recinto do Ecoporto.

## Princípio geral da heurística (usuário, 01/10/2026)

Contêiner + intervalo de datas **nunca basta** para dizer que um evento é do mesmo ciclo — sobretudo
ao alargar tolerâncias. A API tem erros e omissões: uma saída não informada pode fazer a "próxima saída"
encontrada, muitos dias depois, ser de OUTRA operação (contêiner já vazio, já em exportação...). Toda
ligação precisa de **validação adicional de que ainda é a mesma operação**.

## Plano v2 (APROVADO pelo usuário em 01/10/2026)

Decisões do usuário na aprovação: **escopo só Santos** (operadores de Santos via `ovr_recintos`:
`e_operador` + unidade — inclui o Ecoporto); com master e filhote, o **CE principal é o filhote** (o do
importador real), mas o ciclo fica ligado à família. Os ajustes de baixo risco no loop atual NÃO foram
aprovados explicitamente — não mexer no `manager_ciclo_de_vida.py` sem pedir.

**Ideia central: linha do tempo do contêiner + estadas.** Em vez de buscas soltas por janela, carregar
todos os eventos do contêiner (desembarque/embarque, acessos, inspeções — contêiner e hora são os dados
confiáveis) e percorrê-los em ordem com uma máquina de estados. A validação de "mesma operação" vem da
**continuidade**: um evento só entra no ciclo se não houver, antes dele, um evento-fronteira que abra
outra operação:

- fronteiras: novo desembarque; embarque; **entrada no mesmo operador** depois do desembarque sem saída
  registrada (prova de saída omitida); entrada com o contêiner **vazio** depois da saída (devolução);
- sinais secundários (para quando as omissões não deixam fronteira): vazio informado no evento
  candidato x carga cheia no desembarque (peso x tara do desembarque), declaração/CE declarados que
  corroboram ou não, plausibilidade do tempo (distribuição por recinto);
- cada ligação recebe **confiança (alta/média/baixa) e motivo**; só alta/média fecham o ciclo
  automaticamente; baixa fica registrada para análise.

**Fechamento por tipo**: operador (saída validada, sem entrada válida em seco depois); TRA (entrada
cheia válida em recinto **não operador** (`ovr_recintos.e_operador` falso; entrada em outro operador,
como o Ecoporto 8931318, é baldeação se houver embarque lá, ou passagem) — segue-se para a
próxima entrada; scanner minutos antes do gate aceito); passagem/transbordo (embarque no mesmo
operador sem gate no meio); baldeação (saída → entrada em outro operador → embarque); **saída omitida**
(fronteira sem saída: fecha como incompleto, não fica pendente para sempre); abandono (90 dias sem saída
nem fronteira); cabotagem segue o fluxo, com escaneamento marcado como não obrigatório.

**Escaneamento**: inspeção dentro da estada, em qualquer recinto; vazio numa estada cheia baixa a
confiança; CE declarado igual (ou master/filhote) corrobora. Imagem no Mongo por `numeroinformado` +
`dataescaneamento` com **+3 h** e janela estreita (medida depois da correção).

**CE — confirmado em 01/10 (segunda medição)**: a unidade é a **família do CE** (master + filhotes,
`Conhecimento.numeroCEMaster`). Candidatos: CEs cujo `Item` tem o contêiner (Mercante — confiável) e
com escala numa janela maior que ±5 dias (a medir; ~40% dos perdidos estão a 6–15 dias), **sem
exportação** para desembarque de importação. Confiança: alta quando o Mercante (contêiner nos itens) e
o CE declarado nos eventos apontam a mesma família; média só com o Mercante e candidato único;
declarado de outra família ou inexistente = desconfiança. O ciclo deve ficar ligado a **todos os CEs da
família que têm o contêiner** (proposta: tabela ciclo↔CE com papel master/filhote/simples, origem e
confiança), para o NAVI-I achar o ciclo pelo card do master E do filhote. Ciclos concluídos sem CE
voltam a ser tentados quando o Mercante receber o CE (o CE chega depois). Texto anterior: **Hipótese do usuário (01/10) a confirmar** com
`diagnostico_ces_ciclo_de_vida.py` (CONFIRMADA): os eventos informam master e house(s) de forma intercambiada — se
sim, a unidade de comparação passa a ser a **família do CE** (master + filhotes, via
`Conhecimento.numeroCEMaster`): CE declarado da mesma família corrobora a mesma operação; só CE sem
parentesco desconfia. O "OUTRO CE" das ligações atuais pode ser isso ou bug do algoritmo antigo; sem candidato, o ciclo segue (a operação física existe) com o CE
declarado guardado como informação.

**Operadores**: lista vinda de `ovr_recintos` (`e_operador` + unidade), não fixa — inclui o Ecoporto.

**Processamento orientado a eventos** (resolve as horas de loop): marca d'água do último id lido de
cada tabela de eventos; a cada volta, só os contêineres com evento novo têm a linha do tempo refeita
(uma consulta por tabela por lote de contêineres). Ciclo sem novidade não é reconsultado. Prazos
(abandono etc.) fecham por UPDATE em lote. Desembarques novos sem o teto de 2.000 por volta.

Etapas (cada uma testada, com aprovação antes de tocar o loop de produção):

1. **Diagnóstico v2** (curto, só leitura, por amostragem): por que 52% sem CE (CE declarado existe no
   Mercante? prefixos? escala?); OUTRO CE é master/filhote?; operadores fora da lista fixa do loop
   (`e_operador`) — seção 0 de `diagnostico_ces_ciclo_de_vida.py`; saídas tardias que passam/não passam na continuidade; passagem com continuidade; scanner
   x gate; fuso por recinto e janela da imagem após +3 h; peso x tara no desembarque; atraso de criação.
2. **Núcleo puro — FEITO (01/10/2026)**, sem banco, testado em `tests/test_linha_do_tempo.py`:
   - `linha_do_tempo.py`: `ciclos_do_conteiner(eventos, operadores_escopo, operadores, agora,
     familias)` — máquina de estados (operador → trânsito → outro operador / seco → fim), fronteiras,
     inspeções em espera durante o trânsito e a passagem por outro operador (scanner no gate do TRA),
     confiança por ligação, situação (aberto/concluído/incompleto/abandono) e motivo; parâmetros em
     `Parametros` (trânsito 48 h, gate 2 h, relógio 1 h, permanência típica 10 dias, abandono 93 dias);
   - `ces_do_ciclo.py`: `resolve_ces(desembarque, candidatos, declarados)` — família master/filhote,
     sem exportação, janela de escala de 15 dias, filhote como principal;
   - `heuristica_ciclo.py`: `processa_conteiner(...)` — as duas passagens (linha do tempo → CEs →
     linha do tempo corroborada pela família).
3. **Modo sombra — FEITO (01/10/2026; 2 rodadas em produção, ver 3ª e 4ª medições)**: `sombra_ciclo_de_vida.py` (somente leitura; testes em
   `tests/test_sombra_ciclo_de_vida.py`). Escopo = `ovr_recintos.e_operador` com `cod_unidade = '0817800'`
   (normalizado com zeros à esquerda); operadores da linha do tempo = todos os `e_operador` + recintos
   com embarque/desembarque nos eventos. Amostra contêineres espalhados pelo período (que termina 7 dias
   antes de hoje — atraso do loop antigo), carrega TODOS os eventos deles (desde o início do período − 2 dias; acessos só `operacao = 'C'`),
   os CEs do Mercante pelos itens (só dos anos do período — dígitos 3–4 do CE) com master, tráfego e
   escalas (preferindo a de BRSSZ), roda `processa_conteiner` e compara com o ciclo atual do mesmo
   `desembarque_id`. Relatório: volume e desembarques sem ciclo atual por operador; situação atual x
   nova; etapas (igual/nova/removida/diferente); escaneamentos ganhos/perdidos (os de confiança baixa não
   contam como ligados) e confiança/motivos; CE atual x principal novo (igual/mesma família/novo/
   perdido/diferente); saídas > 72 h; incompletos; alertas; vazio no Mercante x peso x tara x CE; defasagem do Mongo por recinto (`--mongo`);
   amostras das divergências; `--csv` com uma linha por ciclo.
   ```
   cd /home/ajna/apps/ajna/virasana
   virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py \
       --dias 30 --conteineres 3000 --mongo --csv sombra.csv
   ```
4. **DDL aditiva — PROPOSTA PRONTA (02/10/2026), não executada**: `ddl_ciclo_de_vida_v2.sql` (para o
   usuário/DBA; homologação primeiro). Colunas novas no ciclo (todas NULL — o loop antigo segue igual):
   situação, fluxo, motivo, confiança do CE, **vazio, numeroManifesto**, escala declarada,
   **tipoConteiner, oog, peso_balanca, peso_manifesto, tara, peso_menos_tara**, embarque, entrada no seco
   implícita (origem), versão da heurística e data do processamento; nos escaneamentos: confiança, motivo,
   recinto e "fora"; tabela **ciclo↔CE** (família, papel, principal) e tabela de **marcas d'água**.
   ADD COLUMN com `ALGORITHM=INSTANT`, índices `INPLACE, LOCK=NONE`; rollback no fim do arquivo.
   Depois (com o serviço gravando): telas mostram tipo/OOG/vazio e permitem excluir OOG da cobrança.
   **DDL aplicada em homologação em 02/10/2026** (pelo usuário).
5. **Serviço por eventos — ATIVADO em produção em 02/10/2026** (pelo usuário): `servico_ciclo_de_vida.py`, com
   `carga_ciclo.py` (carga compartilhada com a sombra), `gravacao_ciclo.py` (tabelas v2 no SQLAlchemy Core,
   conferidas contra a DDL nos testes) e `imagens_ciclo.py` (imagem no Mongo pela defasagem do recinto);
   testes em `tests/test_servico_ciclo_de_vida.py`; programas do supervisor em `supervisor_ciclo_de_vida.conf`
   (raiz do virasana). Ver "Serviço por eventos" abaixo. O loop atual fica para rollback. Acompanhamento:
   `acompanha_ciclo_de_vida.py` (ver "Acompanhamento em produção").
6. **Reprocessamento** dos pendentes do loop antigo: `servico_ciclo_de_vida.py --reprocessa N` (N
   contêineres por vez, os mais recentes primeiro; o log diz quantos pendentes antigos restam).

### Serviço por eventos (`servico_ciclo_de_vida.py`)

A cada volta: (1) lê os eventos NOVOS de `apirecintos_embarquedesembarque`, `apirecintos_acessosveiculo`
(só `operacao = 'C'`), `apirecintos_inspecoesnaoinvasivas` e `apirecintos_pesagensveiculo` pela marca
d'água de cada tabela (`ciclodevida_marca_dagua`; lotes de 20 mil ids; sem marca, começa nos eventos dos
últimos 7 dias ou de `--desde`); (2) junta os contêineres a revisitar — ciclos v2 abertos processados há
mais de 6 h (prazos: trânsito de 48 h, estada curta, abandono) e ciclos v2 dos últimos 30 dias sem CE e
sem ser vazio, há mais de 24 h (o CE chega depois ao Mercante); (3) por lotes de 500 contêineres, numa
transação cada: carrega todos os eventos dos últimos 100 dias, refaz a linha do tempo e os CEs e grava;
(4) avança as marcas. Com fila (lote cheio), a próxima volta começa logo; senão, espera 5 min.

Gravação (`gravacao_ciclo.py`), por `desembarque_id`:
- a linha que já existe — inclusive a do loop antigo — é **atualizada no mesmo ID** (as telas continuam
  achando o ciclo); a que não existe é inserida (ex.: Ecoporto, que o loop antigo não cobria);
- `numeroCEmercante` = CE principal (filhote); a família vai para `ciclodevida_ciclo_ce`;
- `concluido` = situação diferente de "aberto" (concluído, incompleto ou abandono); o detalhe fica em
  `situacao`/`fluxo`/`motivo`;
- ligações de confiança **baixa** (CE declarado de outra família) **não são gravadas** — nem etapa nem
  escaneamento;
- entrada no seco **implícita** (deduzida de inspeção/pesagem): `entrada_seco_id` NULL, com
  `data_entrada_seco`, `codigoRecintoArmazenagem` e `entrada_seco_implicita_de`;
- escaneamentos do ciclo substituídos pelos da v2 (mantém a imagem já achada; procura no Mongo só as que
  faltam), com `confianca`, `motivo`, `codigoRecinto` e `fora` (escaneamento terceirizado antes do TRA);
- `numeroManifesto` só para o contêiner vazio; `peso_menos_tara` pela balança e, sem ela, pelo manifesto;
- `versao_heuristica = 'v2'` e `processado_em`.

Limitação conhecida: se o loop antigo criou um ciclo para um desembarque depois RETIFICADO (outro id, mesma
data/recinto), essa linha antiga não é atualizada (a v2 usa a retificação). A medição mostrou retificações
quase inexistentes.

### Validação em homologação (antes da troca em produção)

```
cd /home/ajna/apps/ajna/virasana
SERVICO=virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py
virasana-venv/bin/python $SERVICO --homologacao --uma-vez --simula    # 1. mede: processa e desfaz
virasana-venv/bin/python $SERVICO --homologacao --uma-vez             # 2. uma volta gravando
virasana-venv/bin/python $SERVICO --homologacao --reprocessa 2000 --simula   # 3. reprocessamento (mede)
```

Depois de (2), conferir no banco de homologação: `SELECT situacao, fluxo, COUNT(*) FROM
ciclodevida_ciclo_de_vida_importacao WHERE versao_heuristica = 'v2' GROUP BY 1, 2`, os escaneamentos com
`confianca`/`imagem_id`, `ciclodevida_ciclo_ce` e `ciclodevida_marca_dagua`; e abrir o NAVI-I e a
completude de homologação. O banco de homologação precisa ter os eventos da API e o Mercante atualizados
para a volta fazer sentido.

### Ativação no Servidor (supervisor)

Hoje o loop antigo roda sob o supervisor como
`python virasana/analytic_scripts/ciclo_de_vida/manager_ciclo_de_vida.py` (variáveis de ambiente do
`ajna_commons`; não há `.conf` dele no repositório). O ponto de entrada **muda**: o serviço novo é
`servico_ciclo_de_vida.py`, com dois programas em `supervisor_ciclo_de_vida.conf` (raiz do virasana),
ambos `autostart=false`: `ciclo_de_vida_v2_hom` (`--homologacao`) e `ciclo_de_vida_v2` (produção).

1. Logs: `sudo mkdir -p /var/log/virasana/ciclo_de_vida && sudo chown ajna /var/log/virasana/ciclo_de_vida`.
2. Copiar/incluir o `.conf` na configuração do supervisor, `supervisorctl reread` e `supervisorctl update`.
3. Homologação (contínuo): `supervisorctl start ciclo_de_vida_v2_hom`; acompanhar `hom_access.log`.
4. Produção — **troca**: `supervisorctl stop <programa do loop antigo>` e só então
   `supervisorctl start ciclo_de_vida_v2`. Na primeira volta, sem marcas, ele lê os eventos dos últimos
   7 dias (ou `--desde`, no `command`). Depois, reprocessar os pendentes antigos aos poucos com
   `--reprocessa N` (à mão, fora do pico).
5. **Rollback**: `supervisorctl stop ciclo_de_vida_v2` e `start` do programa antigo — o
   `manager_ciclo_de_vida.py` não foi alterado. As colunas novas ficam como estão (o loop antigo as
   ignora). Os dois nunca devem rodar juntos gravando nas mesmas tabelas.

### Acompanhamento em produção (`acompanha_ciclo_de_vida.py`)

Relatório curto, só leitura, por índices (segundos) — rodar a qualquer hora e comparar com o anterior:

```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/acompanha_ciclo_de_vida.py [--dias 28] [--saida arq.log]
```

Seções: (1) serviço — marcas d'água, eventos não lidos (fila), última gravação, ciclos gravados na última
hora/dia e **sinais de outro gravador** (ciclo v2 com `last_modified > processado_em` ou escaneamento sem
`confianca` num ciclo v2 = o loop antigo ainda roda); (2) cobertura por semana de desembarque (v2 x antigos,
abertos, CE, vazio, entrada no seco e implícita, escaneamento, imagem, OOG); (3) situação x fluxo, motivos de
quem não concluiu, abertos por idade e sem revisita; (4) escaneamentos por recinto (imagem, fora, confiança)
e sem imagem por idade; (5) CE (confiança, sem CE por idade, papéis da família); (6) pendentes do loop
antigo por mês; (7) **alertas**. O que esperar com o serviço saudável: marcas gravadas há poucos minutos,
fila ~0, nenhum sinal de outro gravador, semanas recentes com ~100% v2, nenhum aberto com mais de 93 dias.

### Efeitos nas telas (completude e NAVI-I) — 02/10/2026

As telas continuam funcionando sem mudança (mesmas tabelas e IDs; colunas novas são ignoradas), mas os
números mudam à medida que os ciclos viram v2:
- **Completude**: entra o TRA com entrada implícita (recintos que não informam a entrada); o escaneamento
  terceirizado/fora passa a contar para o TRA; ligações de outra operação (confiança baixa) saem —
  pode AUMENTAR o "sem escaneamento" onde o loop antigo ligava inspeção errada; ciclos novos do Ecoporto.
  O fluxo "operador" ainda é inferido (`concluido` + saída + sem entrada no seco) e por isso também pega
  baldeação (saída e embarque em outro operador) — o mesmo que o loop antigo fazia após 48 h.
- **NAVI-I**: o CE do ciclo passa a ser o **filhote** (antes, ~36% das vezes o master). Filtros que dependem
  do ciclo (com imagem, recinto, concluído) e a janela "efetiva" ligam pelo CE exato: passam a achar o card
  do filhote e deixam de achar o do master (o card do master ainda mostra o ciclo pela família).
  "Concluído" passa a incluir passagem/baldeação (embarque) e incompleto.

Ajustes propostos (aguardando aprovação): completude usar `fluxo` dos ciclos v2; NAVI-I ligar filtros e
janela "efetiva" pela `ciclodevida_ciclo_ce` (pendência 3a do CONTEXTO.md); mostrar situação/motivo,
entrada implícita, escaneamento fora e confiança no card e no detalhe da completude; decidir se o filtro
"concluído" do NAVI-I significa "encerrado" ou "entregue" (situação concluído com fluxo operador/TRA).

Ajustes pontuais de baixo risco que podem vir antes (se aprovados): tirar o atraso dos desembarques
novos; corrigir o fuso da busca no Mongo; desligar o atalho `saidas_novas_operadores` (código morto).
