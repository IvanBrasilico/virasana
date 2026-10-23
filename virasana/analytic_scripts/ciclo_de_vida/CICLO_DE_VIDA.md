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

**Remoção a bordo (pedido do usuário, 02/10/2026):** desembarque -> embarque -> novo desembarque no MESMO
operador, o novo desembarque em até 24 h depois do embarque e sem acesso de gate desde o primeiro desembarque
(ex.: contêiner tirado do navio para remover outro e devolvido). Antes, o embarque fechava o ciclo como
passagem e o novo desembarque abria outro ciclo (os escaneamentos de antes do embarque ficavam no primeiro).
Agora o ciclo do primeiro desembarque continua (`Parametros.remocao_horas`), o novo desembarque fica em
`CicloCalculado.remocoes`, e a gravação APAGA o ciclo que existia para ele (do loop antigo ou da v2), com os
escaneamentos e os CEs. Embarque seguido de desembarque depois de 24 h, em outro operador ou com gate no
meio continua sendo outra operação.

**Fonte oficial do abandono (informado pelo usuário em 02/10/2026):** os recintos são obrigados a informar os
abandonos, e o AJNA consulta as **FMAs eletrônicas** (Fichas de Mercadoria Abandonada) na API da Janela
Única Portuária. O script `bhadrasana2/bhadrasana/scripts/fma_update.py` (cron diário) abre uma **Ficha OVR por
FMA**, com `tipooperacao = 0` ("Mercadoria Abandonada"), `numeroCEmercante`, `recinto_id` (= `Cod_Recinto` da
FMA, código DTE), `dataentrada` (entrada no recinto) e `datahora` (emissão). A ficha acompanha o procedimento,
que termina em **retomada** (o importador retira) ou **perdimento** (retirada para o depósito e destinação). A
v2 hoje só DEDUZ o abandono por prazo (93 dias sem saída). A FMA pode confirmar ou antecipar o abandono e dar
o desfecho, ligada pelo CE (a FMA não traz o contêiner). Antes de usar, conferir se `ovr_recintos.id`
coincide com `cod_dte` e onde o desfecho fica registrado na OVR. Não implementado.

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
A carga de cada lote recua até o desembarque mais antigo dos ciclos v2 ainda ABERTOS dos seus contêineres (limite 400 dias): o abandono conta do início da estada atual (ex.: entrada no seco), então um ciclo pode ficar aberto bem mais que 100 dias depois do desembarque — sem isso ele deixava de ser refeito e travava a fila de revisita (achado no acompanhamento de 02/10/2026, corrigido no mesmo dia).

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

### Etapa do risco histórico por CE (14/10/2026)

No fim de cada volta, o serviço roda a etapa do risco histórico por CE (`virasana/risco_historico/etapa.py`):
sinais dos CEs novos/alterados, parâmetros (no máximo 1x/h) e pontos — em transações próprias, com limite de
60 s. Qualquer erro dela (inclusive ao carregar o módulo) só vai ao log: o ciclo de vida segue. Desligar:
`--sem-risco` no `command` do supervisor. Em `--simula`, não roda. Exige a carga inicial
(`virasana/risco_historico/lotes.py --sinais`); sem ela, só avisa no log.

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

### Completude no operador: cabotagem (13/10/2026)

Consulta do usuário (entregues pelo operador, 14 dias): importação 10.551 ciclos, 3,3% sem escaneamento;
**cabotagem 3.206, 97,6% sem escaneamento** (88% de todos os sem escaneamento do fluxo); vazios e ciclos sem CE
~1%. É dado real (cabotagem não é escaneada), não erro da lógica do ciclo. A completude passou a deixar a
cabotagem (tráfego 3 no CE do ciclo) FORA da conta por padrão, mostrando quantos ficaram de fora, com a opção
"Incluir cabotagem". Sem cabotagem, o operador fica em ~2,4% sem escaneamento.

### Imagens que faltam (`diagnostico_imagens_ciclo.py`)

No acompanhamento de 02/10/2026 só BTP (76%) e Embraport (84%) ficaram abaixo de ~100% de escaneamentos com
imagem — como a sombra já tinha visto ("sem imagem em ±8 h: BTP 22%, Embraport 12%"). O diagnóstico (só lê)
mostra a falta por dia e recinto, os atributos da inspeção (OCR, contingência, retificação) com x sem imagem e
classifica uma amostra no Mongo: imagem que chegou DEPOIS (o ciclo não foi refeito), outra defasagem, só
longe, número parecido (OCR), nenhuma imagem deste contêiner ou nenhuma imagem no horário (escâner parado).

1ª rodada (13/10/2026, escaneamentos de 29/09 a 10/10): BTP 22–47% sem imagem por dia, Embraport 13–21%, Santos
Brasil ~1% — mas em 09/10 e 10/10 os três foram a 39–100%: a CARGA das imagens no Mongo parou/atrasou por volta
de 09/10. A amostra pegava os mais recentes (todos desse período) e saiu quase toda "nenhuma imagem no horário";
corrigido: sorteio no período inteiro e contagem de imagens do Mongo por dia. Contêiner vazio aparece mais entre
os sem imagem (BTP: 58% dos sem imagem, 40% dos com imagem). Imagens que chegam depois não são ligadas a
ciclos já encerrados (o serviço só procura a imagem ao refazer o contêiner) — ver proposta de revisita de imagens.

```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_imagens_ciclo.py --saida imagens.log
```

### Operador em papel de TRA (observação do usuário, 15/10/2026) — EM PRODUÇÃO desde 15/10/2026

Faltou na especificação inicial: um operador pode armazenar carga desembarcada em OUTRO operador, como TRA —
desembarca na ECOPORTO, sai, entra na DPW/EMBRAPORT, escaneia e fica lá (DPW como TRA); ou desembarca na BTP,
Santos Brasil ou DPW, sai, entra na ECOPORTO, escaneia e fica lá. O problema é a confusão com o **escaneamento
terceirizado**, já tratado (BTP → ECOPORTO só para escanear → TRA não operador da 0817800, onde ficam a entrada e a
saída do ciclo).

**Como o núcleo trata hoje** (confirmado no cenário de `tests/test_diagnostico_operador_tra.py`): toda estada noutro
operador é PASSAGEM (`_em_outro_operador`): a saída de lá volta ao trânsito, o escaneamento feito lá fica marcado
"escaneamento fora" e, sem nova entrada em 48 h, o ciclo fecha como **operador/concluído — "entregue pelo operador"**
(o do desembarque). Ainda dentro do outro operador: aberto "no operador X" até o abandono. Efeitos: a **completude do
operador** cobra o escaneamento do operador do DESEMBARQUE (e o conta como escaneado se o outro escaneou); a
**completude TRA** não vê o outro operador; o NAVI-I mostra "entregue" sem a estada de armazenagem.

**Diagnóstico** (`diagnostico_operador_tra.py`, só leitura): candidatos = ciclos com entrada CHEIA noutro operador de
Santos em até 30 dias do desembarque; para eles, o percurso estada por estada até a primeira fronteira, a classe de
cada estada (passagem ≤ 6 h + nova entrada; final > 6 h sem nova entrada = "TRA?"; em aberto; baldeação; ...), por
faixa de duração e par de operadores, como o ciclo gravado tratou, o efeito na completude, a duração de referência
dos TRA não operadores, DTA dos CEs e exemplos.

```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_operador_tra.py --saida operador_tra.log
```

Hipótese de regra (a decidir com os números): depois da saída do outro operador, **nova entrada cheia em até 6 h** =
passagem (como hoje); **sem nova entrada em 48 h** (ou ainda lá depois de N horas) = o outro operador foi o TRA —
`entrada_seco`/`saida_seco`/`codigoRecintoArmazenagem` = ele, fluxo `tra`, escaneamento de lá "no recinto da etapa"
(não "fora"). Pontos a medir: estadas longas seguidas de outra entrada (dois estágios?), a duração que separa as
duas classes, e como gravar enquanto o contêiner ainda está lá (TRA provisório, como o `pos_seco` dos não operadores).

**1ª medição (15/10/2026, `virasana/operador_tra.log`; desembarques de 08/09 a 08/10; 4.000 de 16.078 contêineres
candidatos — multiplicar por ~4 para o mês):**
- 10–14% dos ciclos da Santos Brasil, BTP e Embraport entram cheios noutro operador em até 30 dias (Ecoporto quase
  não desembarca: 639 ciclos). A consulta dos candidatos leva ~4 min.
- **A duração separa sozinha passagem de armazenagem**: passagens 99,8% em até 3 h (mediana 12 min); "final (TRA?)"
  todas com mais de 6 h (só 4 abaixo de 1 dia; mediana 6,9 dias — mesmo perfil dos TRA não operadores, 7,7 dias).
  Entre 3 h e 6 h: 2 casos. O corte de 6 h (`passagem_ate_entrada_horas`) serve.
- **Operador em papel de TRA existe e é frequente**: 317 estadas finais + 248 ainda lá + 23 recém-saídas na amostra
  (~1.270 por mês terminadas e ~1.000 em andamento). Pares: Santos Brasil → Embraport 152, Embraport → Santos Brasil
  66, BTP → Santos Brasil 48, BTP → Embraport 31, Santos Brasil → BTP 16; o **Ecoporto nunca é TRA** (4) — é passagem
  para escanear (446 + 318 + 138), e o escaneamento do Ecoporto vem com o código do TRA de destino (8931339, 8931304).
  A passagem pela Santos Brasil (8931356) escaneia para a Santos Brasil Logística (8933203). Já tratadas.
- **Como fica gravado**: 82% das finais como operador/concluído "entregue pelo operador" (o do desembarque), com o
  escaneamento do outro operador marcado "fora"; as em andamento como indefinido/aberto "no operador X" (estágio
  "Em trânsito"). O outro operador escaneia em ~100% (inspeção lá: 100% nas finais) — o efeito na completude é de
  ATRIBUIÇÃO (o operador do desembarque recebe o crédito; o outro não aparece como TRA), não de falta de escaneamento.
- **Baldeação parecida durante a estada**: 260 estadas terminam em embarque no outro operador depois de 1–10 dias
  (68% com inspeção lá) — a duração não separa da armazenagem enquanto o contêiner está lá. A seção 8b (tráfego e
  origem do CE do ciclo por classe) foi acrescentada para ver se o CE separa; rodar de novo.
- **"final curta" (191)**: passagem de minutos por outro operador (quase sempre o Ecoporto) sem nova entrada e quase
  sem inspeção (2%); gravadas como entregue pelo operador do desembarque SEM escaneamento (97 de 104 da v2) —
  destino desconhecido. Perguntado ao usuário.
- Ruído do diagnóstico (não do núcleo): entradas em operador depois de uma estada longa num seco (reuso para
  exportação) aparecem como "em aberto"/"baldeação" com o ciclo gravado tra/concluído — o percurso do diagnóstico não
  corta depois do seco; o núcleo, sim. Ciclos do loop antigo (antes de 02/10) ainda trazem o Ecoporto como seco.

**2ª rodada (15/10/2026, `virasana/operador_tra2.log`, mesma amostra + seção 8b)** — o CE do ciclo separa a
baldeação da armazenagem enquanto o contêiner está no outro operador:

| classe | estadas | sem CE | importação (5) | cabotagem (3) | CE emitido fora (≠ 15) |
|---|---|---|---|---|---|
| final (TRA?) | 318 | 26 | 292 | 0 | 0 |
| em aberto | 248 | 42 | 198 | 8 | 3 |
| baldeação | 260 | **150** | 88 | **22** | **31** |

Armazenagem = CE de importação emitido em Santos (92%); baldeação = sobretudo sem CE (vazio reposicionado), cabotagem
ou CE de outro porto. Tráfego 9 (passagem) não aparece em nenhuma classe. Também: 40 ciclos "final (TRA?)" estão
gravados como baldeação/incompleto/aberto — no trânsito, o núcleo aceita um EMBARQUE a qualquer tempo como baldeação
(sem prazo); com a regra A o ciclo termina na saída do outro operador e isso deixa de acontecer.

**Proposta (aguardando aprovação)**: (A) saída do outro operador depois de mais de 6 h = ele foi o TRA (entrada e
saída do seco = as dele, fluxo `tra`, escaneamento lá sem "fora"); até 6 h, passagem como hoje. (B) ainda lá há mais
de 6 h E o CE principal do ciclo é de importação (tráfego 5) emitido em Santos (15...) = TRA provisório (aberto "no
recinto seco"); sem CE, cabotagem ou CE de outro porto, continua "aberto no operador X" até a saída (A) ou o embarque
(baldeação). Na amostra: 198 dos 248 em andamento viram TRA provisório; baldeações que pareceriam TRA enquanto
esperam o navio: no máximo 57–88 (10–15% dos provisórios), corrigidas quando embarcam — a gravação limpa os campos do
seco quando o ciclo refeito não tem entrada no seco (`gravacao_ciclo._entrada_seco`). O tráfego vem da segunda
passagem de `heuristica_ciclo.processa_conteiner` (CE principal + `CandidatoCE.trafego`). (C) completude: a lista de
TRA passa a ter operadores — marcar "operador em papel de TRA". (D) reprocessar os ciclos concluídos recentes com
estada noutro operador (o serviço não revisita concluídos sem evento novo). Validar em homologação/`--simula` e com a
sombra antes de produção. Usuário (15/10): seguir o plano passo a passo, MAS explicar antes a "final curta".

**"Final curta" em investigação (`diagnostico_final_curta.py`)**: ~190 por 4.000 contêineres (~760/mês), 98% no
Ecoporto, quase todas em até 30 min; ~390/mês gravadas como entregue pelo operador do desembarque SEM escaneamento —
ordem de grandeza de METADE dos "sem escaneamento" da entrega pelo operador (consulta de 13/10: ~750 por 30 dias);
mais 53 do loop antigo com o Ecoporto como "seco" sem escaneamento (cobradas do Ecoporto na completude TRA). O
diagnóstico compara com as passagens que seguem para um TRA (controle) e testa: H1 escaneado sem inspeção
transmitida (imagem no Mongo no horário; inspeção com outro código); H2 desembaraçado no operador e entregue
(declaração DI/DUIMP na saída; devolução do vazio); H3 o destino leu o número errado (o MESMO caminhão, pela placa,
entra noutro recinto em até 6 h com número parecido); H4 destino fora da API (o caminhão some; agendamento G).
```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_final_curta.py --conteineres 20000 --mongo --saida final_curta.log
```

**1ª rodada (15/10/2026, `virasana/final_curta.log`, TODOS os 16.061 candidatos; desembarques 08/09–08/10)**: 761
casos (748 Ecoporto, 13 Santos Brasil); controle = 761 passagens pelos mesmos recintos.
- **Escaneados**: imagem no Mongo no horário da passagem em 99% (controle 98%). **Sem inspeção transmitida**: com o
  código da passagem 2% e com outro código 1% — no controle, 17% e **80%** (nas passagens, a inspeção do Ecoporto vem
  com o código do TRA de destino: quem transmite é o "cliente" do escâner). A 2ª rodada (seção 11b: horário da
  imagem menos a defasagem do recinto) diz se a imagem é do escâner do Ecoporto ou da saída do operador de origem.
- **Não é OCR nem TRA**: o MESMO caminhão (placa) entra noutro recinto em até 6 h SEM contêiner (61%, quase sempre de
  volta a um terminal: BTP, Santos Brasil, Embraport), com outro contêiner (10%) ou some da API (29%); 0 números
  parecidos. No controle, 100% dos caminhões entram no TRA com o próprio contêiner (o método funciona). O contêiner
  reaparece 12–19 dias depois (vazio, de volta ao terminal, REDEX/depósito) ou nunca (358) — entrega ao importador.
- Lotes: 424 CEs, 74 com 2+ contêineres (até 15 do mesmo CE em 1–2 dias). `tipoDeclaracao` dos acessos quase nunca
  vem preenchido (também no controle) e não há agendamento (G).
- **Leitura**: entrega pelo operador de origem com escaneamento terceirizado no Ecoporto, sem inspeção transmitida
  por ninguém. Gravados: 467 entregue pelo operador (a maioria SEM escaneamento — ~metade ou mais dos "sem
  escaneamento" da completude do operador), 175 do loop antigo (Ecoporto como "seco").
- **Falha do núcleo achada**: 114 (75 baldeação, 31 "aberto no operador X", 8 incompleto) seguem além da fronteira —
  em `_entrada_apos_saida`, entrada em OUTRO OPERADOR não passa pelo teste de vazio nem pelo prazo de trânsito de
  48 h, e o EMBARQUE em trânsito não tem prazo. Conferido no núcleo: passagem pelo Ecoporto + vazio entrando na
  Embraport 15 dias depois + embarque = "baldeação". Corrigir no passo 1 (vale para todo ciclo, não só a final curta).

Opções para a final curta (decisão do usuário): (1) ligar a imagem como escaneamento ("imagem do escâner do Ecoporto
sem inspeção transmitida", confiança média) + coluna de conformidade na completude — precisa de DDL aditiva
(`escaneamento_id` é NOT NULL); (2) só marcar no motivo e mostrar à parte na completude; (3) só relatório de
conformidade, sem mudar o ciclo.

**Passo 1 — núcleo (15/10/2026, autorizado pelo usuário; NÃO está no Servidor até o commit/deploy):**
`linha_do_tempo.py` — (a) fronteiras do trânsito: em `_entrada_apos_saida`, vazio e prazo de 48 h vêm ANTES do teste
de operador; `_embarque_em_transito`: embarque depois do prazo = outra operação (entregue pelo operador, com alerta);
(b) regra A: `_em_outro_operador` → `_fim_do_operador_tra` quando a estada passa de `estada_operador_tra_horas` (6 h)
e termina em saída (TRA concluído, motivo "saída do recinto seco (operador X em papel de TRA)") ou em nova entrada
(TRA incompleto, saída omitida); embarque continua baldeação; (c) regra B: `_fecha_por_prazo` — ainda no outro
operador há mais de 6 h E `importacao` = TRA provisório ("no recinto seco (operador X em papel de TRA)");
`ciclos_do_conteiner(..., importacao={id do desembarque: bool})` — por enquanto ninguém passa (passo 2: a heurística
calcula pelo CE principal). Testes: `TestOperadorEmPapelDeTRA`, `TestFronteirasDoTransito` (test_linha_do_tempo); o
teste da volta ao operador depois de outro operador foi dividido (estada curta = volta; 10 dias = TRA incompleto).
Sombra: seção 2b (fluxo gravado x novo, com "operador em papel de TRA") e colunas `fluxo_atual`/`operador_tra` no CSV —
é a simulação do passo 1 sobre os ciclos que o serviço já gravou:
```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py --dias 30 --conteineres 10000 --csv sombra_passo1.csv --saida sombra_passo1.log
```

**Simulação do passo 1 (15/10/2026, `virasana/sombra_passo1.log` + `.csv`; 10.017 ciclos de ~140 mil desembarques
de 08/09 a 08/10 — multiplicar por ~14 para o mês)**: nada muda em operador→operador (2.842), passagem (2.377) e
TRA (1.834 + 436 abertos). Mudanças:
- fronteiras: **247 "baldeações" viram entregue pelo operador** (78% das 318 gravadas: 125 entrada noutro operador
  fora do prazo, 102 entrada vazia, 20 embarque semanas depois) e 175 "indefinido/aberto" também; esses ciclos perdem
  as ligações de escaneamento da OUTRA operação (161 + 86 ciclos);
- regra A com entrada informada: Embraport 76, Santos Brasil 41, BTP 7 (gravados hoje como entrega pelo operador do
  desembarque);
- **a conferir: 121 "Ecoporto em papel de TRA" com a entrada DEDUZIDA da pesagem** (o Ecoporto não informa a entrada;
  já visto na 4ª/6ª medição) — mesmo perfil dos outros (cheio, CE de importação), mas sem a linha do tempo não dá para
  saber se é armazenagem real ou pesagem de uma passagem com a saída de outra hora. Ferramenta nova:
  `eventos_conteiner.py` (eventos crus, intervalos, pesos, imagens e o ciclo do núcleo atual por contêiner):
```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/eventos_conteiner.py --csv sombra_passo1.csv --filtro "8931318 em papel de TRA" --limite 20 --mongo --saida eventos_ecoporto.log
```

**Ecoporto em papel de TRA conferido (15/10/2026, `virasana/eventos_ecoporto.log`, 20 contêineres)**: é armazenagem
real. Padrão: saída do operador de origem com pesagem → 20 min a 3 h depois o Ecoporto PESA o mesmo caminhão
(mesmo peso ±100 kg, cheio) e inspeciona (com o código do Ecoporto; imagem no Mongo) — sem informar a ENTRADA → 3 a 30
dias depois (mediana ~11) pesa de novo e informa a saída. O Ecoporto tem dois modos: passagem para escanear (entrada e
saída informadas, inspeção NÃO transmitida, minutos — a "final curta") e armazenagem (entrada NÃO informada, pesagem,
inspeção e saída informadas, dias). A entrada deduzida da pesagem está certa; o passo 1 fica como está. Em 2 dos 20
há imagem na chegada sem inspeção transmitida (o mesmo problema da final curta). Melhoria possível: o peso igual ao
da saída do operador de origem corroboraria a entrada deduzida (hoje confiança média).

**Passo 2 — CE na heurística (15/10/2026)**: `ces_do_ciclo.importacao_de_santos(resolvidos, candidatos)` (CE principal
de tráfego 5 e começando com 15) e `heuristica_ciclo.processa_conteiner` passa `importacao` ao núcleo na segunda
passagem — ativa a regra B (TRA provisório) no serviço e na sombra. Testes em `TestCEsDoCiclo` e
`TestProcessaConteiner`. Simulação: a mesma sombra do passo 1 (esperado: parte dos "aberto no operador X" vira
"tra/aberto, operador em papel de TRA").

**Simulação do passo 2 (`virasana/sombra_passo2.log`)**: 47 "aberto no operador X" com CE de importação de Santos
viram TRA provisório (Embraport 23, Santos Brasil 19, BTP 5); sobram 18 "aberto no operador X"; nada mais muda.

**Passos 3 e 4 — para produção (15/10/2026, aprovado pelo usuário: "vamos colocar em produção")**:
- **Imagem sem inspeção (opção 1 da final curta)**: `linha_do_tempo` guarda as PASSAGENS (`CicloCalculado.passagens`:
  recinto, entrada, saída — por outro operador ou estada curta num seco); `imagens_ciclo.janelas_sem_inspecao` dá as
  janelas sem inspeção ligada (passagem: entrada − 15 min a saída + 15 min; chegada ao seco: 2 h antes a 24 h depois
  da entrada) e `imagens_sem_inspecao` acha a primeira imagem de cada uma (defasagem do recinto); a gravação grava
  `escaneamento_id` NEGATIVO (`id_da_imagem`, dos 15 últimos hex do ObjectId — estável), confiança média, motivo
  "imagem do escâner sem inspeção transmitida", `fora` nas passagens. Sem DDL. Sem o Mongo (`--sem-mongo`), as que
  já existem ficam; inspeção que chega depois na janela substitui a só-imagem. Não vale para a estada no operador
  do desembarque.
- **Completude**: coluna "Só imagem, sem inspeção" (contam como escaneados) e selo "operador em papel de TRA" na
  lista de recintos secos (operador = recinto que também tem desembarques); no detalhe, selo "sem inspeção na API".
- **Refazer o que já foi gravado**: `servico_ciclo_de_vida.py --refaz-desde DATA [--refaz-ate DATA]` refaz UMA VEZ
  os ciclos (abertos e concluídos, de qualquer versão — inclusive os do loop antigo) dos contêineres desembarcados
  no período, os mais recentes primeiro, em partes de 5.000 com o progresso no log; parte com erro vai ao log.
- **Acompanhamento**: seção 3 com "operador em papel de TRA" por recinto e situação; seção 4 com a coluna "só imagem
  (sem inspeção)" por recinto.

**Roteiro no Servidor** (`cd /home/ajna/apps/ajna/virasana`; `SERVICO=virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py`):
1. `git pull` — o serviço em execução continua com o código antigo na memória até reiniciar.
2. Medir sem gravar, um dia já processado: `virasana-venv/bin/python $SERVICO --refaz-desde 2026-10-13 --refaz-ate
   2026-10-14 --simula` — o log mostra quanto tempo leva e as estatísticas (ciclos, "escaneamentos só com imagem").
3. `sudo supervisorctl restart ciclo_de_vida_v2` — o serviço passa a usar o núcleo novo.
4. Refazer o que já foi gravado, por semana e fora do pico, do mais recente para trás (ex.: `--refaz-desde 2026-10-08
   --refaz-ate 2026-10-16`, depois `--refaz-desde 2026-10-01 --refaz-ate 2026-10-08`, ... até 2026-09-01).
5. Reiniciar a aplicação web do virasana (tela da completude).
6. Acompanhar: `acompanha_ciclo_de_vida.py` (seções 3, 4 e 7), a tela da completude e, depois do passo 4, a sombra
   (`--csv`): a seção 2b deve mostrar o fluxo gravado igual ao novo.
Rollback: voltar o código (`git checkout` do commit anterior), reiniciar o serviço e refazer o período; os
escaneamentos só com imagem saem sozinhos ao refazer (ou `DELETE ... WHERE escaneamento_id < 0`).

**Final curta — 2ª rodada (`virasana/final_curta2.log`, seção 11b)**: a imagem é do momento da passagem em 754 de 761
(99%; mediana 4 min depois da entrada no Ecoporto), nenhuma na saída do operador de origem — é o escâner do Ecoporto
(e o da Santos Brasil em 13 casos). H1 confirmada: escaneados, inspeção não transmitida por ninguém. Origem: BTP 469,
Santos Brasil 253, Embraport 38.

**Resultado em produção (15/10/2026, refaz de 01/09 a 16/10 — 211.943 contêineres; `virasana/acompanha_pos.log`,
`sombra_pos.log`, `operador_tra_pos.log`)**:
- Operador em papel de TRA: as 312 estadas "final (TRA?)" da amostra estão gravadas como `tra/concluido` com a
  armazenagem lá (antes, 82% como entrega pelo operador do desembarque); "entrega pelo op. do desembarque" zerou
  na seção 6. No período (acompanhamento, seção 3): concluídos Ecoporto 977, Embraport 631, Santos Brasil 322,
  BTP 33; abertos Embraport 434, Santos Brasil 405, BTP 47. O Ecoporto quase não aparece no diagnóstico (seção 2:
  4) porque lá a entrada da armazenagem não é informada (é deduzida da pesagem).
- Final curta: 180 das 183 com escaneamento "fora" (a imagem sem inspeção); antes, nenhum.
- Só imagem por recinto (acompanhamento, seção 4): Ecoporto 543 (passagens), **Multilog 8933201: 972 de 972** —
  o Multilog não transmite NENHUMA inspeção à API, mas as imagens chegam ao AJNA (era o recinto "faltando" na
  completude) —, Santos Brasil Logística 8933203: 20 de 20, demais secos 1 a 15.
- Sombra, seção 2b: fluxo gravado = novo, menos 18 `indefinido → operador` (prazo de 48 h vencido entre a última
  revisita e a sombra — o serviço fecha na revisita seguinte) e 19 "sem ciclo atual". Os 183 "perdeu" da seção 4
  eram os escaneamentos só com imagem (id negativo), que a sombra não recalcula — agora ficam fora da comparação
  (coluna `escaneamentos_so_imagem` e linha própria na seção 4).
- **Achado: 18 de 65 desembarques no Ecoporto (10 a 24/09) sem ciclo**, iguais antes e depois do refaz: o loop antigo
  não criava ciclo para o Ecoporto, o serviço só cria ao ver um evento novo do contêiner e o `--refaz-desde` só pegava
  contêiner que já tinha ciclo. Corrigido: `conteineres_do_periodo` inclui os contêineres com desembarque num
  operador do escopo no período (teste `test_refaz_cria_ciclo_de_desembarque_sem_ciclo`). **2º refaz (15/10, 20:57;
  `virasana/sombra_pos2.log`)**: 214.809 contêineres, 216.244 ciclos atualizados e **0 inseridos** — os 18
  continuam sem ciclo, porque esse refaz rodou com o código ANTERIOR (a correção foi comitada às 21:11). Os eventos
  desde 25/08 (`virasana/eventos_sem_ciclo.log`) não têm nada antes do desembarque no Ecoporto e o núcleo gera o
  ciclo normalmente (vazios com inspeção "vazio": 10 entregues pelo operador e 2 ainda lá; 2 cheios entregues pelo
  operador; 2 TRA — Transbrasa e Marimex); falta só refazer com o código novo. O 19º (FLNU6804578, Embraport) é borda da janela da SOMBRA: desembarque em 04/09
  (antes do início da carga da sombra, 06/09), remoção a bordo em 09–10/09 — gravado certo, no ciclo de 04/09.
  Dado do Ecoporto a observar: em 2 dos 18 (WSCU6007441, TGHU0486292, mesmo CE) o DESEMBARQUE foi informado ~21 h
  DEPOIS da pesagem, da inspeção e da saída — o ciclo fica "aberto no operador" (sem saída depois do desembarque) e
  só fecharia por abandono. No resto, a sombra bate: seção 2b sem diferenças além do tempo (1 TRA provisório que
  passou das 6 h), seção 4 sem "perdeu" (186 só imagem em 183 ciclos, fora da comparação), seção 10 sem divergências.
- Para olhar depois: 7 ciclos abertos no operador/seco há mais de 93 dias (alerta do acompanhamento; eram 1 em
  02/10) e BTP com 67% dos escaneamentos com imagem (76% em 02/10) — ver "Imagens que faltam".

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

Ajustes APROVADOS e feitos em 02/10/2026: completude usa `fluxo` dos ciclos v2 (reembarcados fora da
"entrega pelo operador"); NAVI-I liga filtros e janela "efetiva" pela `ciclodevida_ciclo_ce`
(`LIGACAO_FAMILIA`, desligável por `NAVII_LIGACAO_FAMILIA=0`; medir antes com `diagnostico_sql.py
--so-ciclo [--sem-familia]`); card e detalhe da completude mostram estágio/situação/motivo, entrada
implícita, escaneamento fora e confiança; filtro de **estágio** (`estagios_ciclo.py`) — "concluído" =
**entregue** (decisão do usuário), com os demais estágios como opção. Ver CONTEXTO.md do NAVI-I.

Ajustes pontuais de baixo risco que podem vir antes (se aprovados): tirar o atraso dos desembarques
novos; corrigir o fuso da busca no Mongo; desligar o atalho `saidas_novas_operadores` (código morto).

## Fora de Santos e pesagens (plano APROVADO pelo usuário em 20/10/2026)

**Problema 1 — portos fora de Santos sem ciclo e sem imagem.** Causa principal já conhecida: o escopo do v2
(decisão de 01/10/2026) é **só Santos** — `carga_ciclo.operadores()` abre ciclo apenas nos operadores
`e_operador` da unidade 0817800; o loop antigo também só cobria Santos. O card do NAVI-I só mostra a imagem que
vem do ciclo. Exemplo do usuário: CE 132605338073101 (destino BRRIO), GSLU2053602 — D 13/10 e E 15/10 no 8931356
(passagem em Santos, viagem 620N), "E" 16/10 no 7921302 (Rio) da MESMA viagem 620N, que parece um desembarque
informado como embarque; inspeções 18/10 05:00 no 7921302 (vazio = não) e imagens no Mongo às 08:00 (+3 h). O
7921302 também omite o contêiner nos acessos (a inspeção traz).

- **D1 — `diagnostico_ciclo_outros_portos.py`** (só leitura): eventos por recinto fora da 0817800 e qualidade
  (acessos sem contêiner, inspeções, pesagens), "E" que parece desembarque, efeito no NAVI-I (cards de CEs com
  destino fora de Santos: com ciclo, com imagem, imagem achável pelo fallback), defasagem imagem − inspeção por
  recinto, linhas do tempo de amostra. Depois, por unidade, decidir ampliar o escopo (a sombra pode ganhar
  `--unidade`).
- **D2 — fallback da imagem no card do NAVI-I** (aprovado): só nos cards sem imagem do ciclo; janela da operação
  do CE (previsão de atracação −2 a +30 dias, cortada no 1º evento do contêiner que indique outra operação);
  preferir a inspeção da API (cheia) e a imagem pela defasagem do recinto (`imagens_ciclo.escolhe_imagem`); sem
  inspeção, a imagem do Mongo só por `numeroinformado` + `dataescaneamento` (confiança menor); selo no card com a
  origem. O filtro "Somente com imagem" continua só pelo ciclo.

**Problema 2 — pesagens (PesagemVeiculo) no ciclo.** Mecânica parecida com o escaneamento (descarga → pesagem →
saída; entrada no seco → pesagem → saída); balança muitas vezes integrada ao gate. Peso do contêiner = conjunto
carregado − conjunto descarregado (mesmas placas); no RTG (guindaste, sem placa) o peso já é o do contêiner. Regras
do usuário: `taraConjunto` da API não é confiável — preferir uma pesagem recente do conjunto sem contêiner;
resolução das balanças rodoviárias 10/20/50/70 kg, RTG 50/100 kg — peso fora da divisão indica valor digitado,
não lido da balança como manda o SICA; `capturaAutoPeso` existe; erros de digitação comuns (12350 x 1235).
**Base de comparação (usuário, 20/10/2026): medido − tara do contêiner = `Item.pesoBruto` (Mercante).**

- **P0 — `diagnostico_pesagens.py`** (só leitura): preenchimento por recinto, resolução de cada balança, pesagem x
  gate, tara (par na mesma visita, pesagem vazia recente, `taraConjunto`), RTG, medido x declarado e padrões de
  digitação, conjunto com dois contêineres.
- **P1 — captura** (DDL aditiva pelo usuário/DBA): tabela `ciclodevida_pesagem` (cada pesagem ligada, tara e
  origem, peso do contêiner, captura automática, confiança, motivo) + resumo no ciclo (peso medido, diferença,
  diagnóstico).
- **P2 — diagnóstico da divergência** (função pura): coerente; digitação provável (×10ⁿ, dígito a mais/a menos,
  vizinhos trocados); pesagem digitada (fora da divisão/sem captura automática); dois contêineres no conjunto;
  divergência relevante (possível fraude); sem pesagem/tara confiável. Tolerâncias a partir do P0.
- **P3 — tela**: peso medido x declarado no card (diferença e selo), expansão com as pesagens brutas e o
  diagnóstico; filtros por diferença %, ordem de grandeza e diagnóstico (pelas colunas do ciclo).
- Riscos: a carga da API (bhadrasana2, `get_listaConteineresUld`/`get_listaSemirreboque`) grava só o 1º contêiner
  e o 1º semirreboque da lista — conjunto com dois contêineres de 20' (ou bitrem) põe o peso todo no 1º e o 2º fica
  sem pesagem (o P0 mede; corrigir de vez = gravar a lista inteira, decisão do usuário); as pesagens só entram nos
  ciclos que existem (Santos) até a decisão do D1.

Ordem aprovada: D1 e P0 juntos (diagnósticos para rodar em produção) e D2 em paralelo; P1–P3 depois dos números.

**Andamento (20/10/2026)**:
- **D2 feito** — `navii/imagem_fallback.py` (regra pura `janela`/`corta`/`escolhe` + `completa_cards`, uma consulta
  por tabela de eventos e uma ao Mongo por página), ligado em `views_navii._monta_cards_com_imagens` (abas de
  análise e de texto; falha só vai ao log). O card leva `imagemFallback` {origem, confiança, detalhe} e o painel de
  imagens mostra o selo (âmbar = pela inspeção, laranja = só a imagem). Testes `test_navii_imagem_fallback.py`
  (caso do usuário reproduzido: as duas imagens das 08:00 entram, as de outra operação não); E2E no navegador.
- **D1 e P0 escritos, falta rodar em produção** — `diagnostico_ciclo_outros_portos.py` (o D1 mede o fallback
  com o mesmo `imagem_fallback`) e `diagnostico_pesagens.py` (regras em `pesagens_ciclo.py`: divisão da balança,
  tara do contêiner, padrões de digitação, classificação prévia, par da tara). Testes:
  `test_diagnostico_ciclo_outros_portos.py`, `test_diagnostico_pesagens.py`, `test_pesagens_ciclo.py`.
  Mutantes 28/28 (um deles mostrou que na balança de 1 kg o peso fracionado também é digitado — regra corrigida).
  ```
  cd /home/ajna/apps/ajna/virasana
  D=virasana/analytic_scripts/ciclo_de_vida
  virasana-venv/bin/python $D/diagnostico_ciclo_outros_portos.py --saida outros_portos.log
  virasana-venv/bin/python $D/diagnostico_pesagens.py --saida pesagens.log
  ```

**Resultado do D1 (produção, 20/10/2026; eventos de 06 a 20/10, log `virasana/outros_portos.log`)**:
- **Cadastro**: fora da 0817800, só 5 operadores estão em `ovr_recintos` (Pecém 3111401, Suape 4931303, Rio ICTSI
  7921302 e MultiRio 7921303, Itaguaí 7961304). Terminais de grande volume NÃO estão cadastrados (9101602, 9981403,
  9801303 — 12 a 16 mil desembarques cada em 14 dias —, 0301302/0301304 e dezenas de recintos 8932xxx/69xxxxx).
- **Acessos sem contêiner** ~50% em todos (Santos 53%): normal (caminhão sem contêiner). Exceções: RIO_ICTSI
  **100%** (nunca informa o contêiner no acesso — confirma o usuário), 0301302, 6911101, 8932722, 0301402 (100%);
  Itaguaí não informa acesso nem pesagem (0 em 14 dias). Inspeções quase sempre com contêiner (6911101: 100% sem).
- **"E" que parece desembarque**: RIO_ICTSI 57% da amostra de embarques (sem entrada nem D antes, com inspeção
  depois), todos SEM CE declarado; 42% sem outros eventos (o acesso sem contêiner esconde a entrada). Exemplos:
  passagem por Santos (D e E no 8931356) e depois "E" no ICTSI com inspeções. MultiRio, Suape e Pecém se comportam
  como Santos (embarque com entrada no gate antes ou passagem).
- **NAVI-I (1.500 cards de CEs com destino fora de Santos)**: 7% com ciclo (o contêiner passou por Santos), ~0% com
  imagem do ciclo; o **fallback recupera 9%** no total — **BRRIO 85%** (ICTSI e MultiRio), Suape 4%, ARBUE 5%
  (passagem pelo Rio). 91% continuam sem imagem.
- **Por que o resto continua sem imagem**: as imagens desses escâneres NÃO estão no Mongo do AJNA — 9801303,
  9101602, 9981403 e 0301304: 100% das inspeções sem imagem em ±6 h. Com imagem: ICTSI e MultiRio 99%, Suape 100%,
  Itaguaí 100%, Pecém 57% (todos **+3 h**). Itapoá, Navegantes, Itajaí, Paranaguá, Manaus etc. não têm o que mostrar
  — não é falha da heurística.
- **Fallback conferido nos exemplos**: Rio pela inspeção no dia da chegada; Suape no dia da previsão; carga de
  passagem para o exterior (ESBCN, ILASH, ITGOA) pela inspeção na baldeação no Rio ou em Santos, 6 a 20 dias depois da
  previsão (dentro da janela de 30 dias). 6% dos cards sem previsão (destinos no exterior sem escala).

**Decisões depois do D1 (usuário, 20/10/2026)**:
1. **Feito** — `imagens_ciclo.DEFASAGEM_HORAS` ganhou a defasagem medida (+3 h) de 7921302, 7921303, 4931303,
   7961304 e 3111401 (antes tentavam +3 h e +0 h; a imagem a +0 h deixa de ser ligada a uma inspeção desses
   recintos). O máximo continua 3 h: as folgas da consulta ao Mongo não mudam.
2. **Medir antes de ampliar o escopo** a **MultiRio (7921303) e Suape (4931303)**, que se comportam como Santos.
   O Rio ICTSI (acesso sem contêiner, "E" que parece desembarque sem CE) e Itaguaí (sem acesso nem pesagem) ficam
   FORA. A sombra ganhou `--operadores` (escopo explícito no lugar dos operadores da 0817800; teste
   `TestOutroEscopo`; mutantes 4/4). Lá não há ciclo atual: as seções que comparam com ele saem vazias — valem a
   situação, as etapas, os CEs, os escaneamentos e as imagens (`--mongo`). As escalas ainda preferem BRSSZ
   (`carga_ciclo._escalas`): sem efeito na medição (janela de ±15 dias); ajustar ao porto do operador se o escopo
   for ampliado.
   ```
   cd /home/ajna/apps/ajna/virasana
   virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py \
       --operadores 7921303 4931303 --dias 30 --ignorar-ultimos-dias 2 --conteineres 3000 --mongo \
       --saida sombra_rio_suape.log
   ```
   Critério para ampliar (por operador): ciclos completos e incompletos no padrão de Santos, CE principal com
   confiança alta na maioria, imagens ligadas às inspeções. Se passar, o escopo vira parâmetro (unidade + lista de
   operadores) no serviço, com as escalas pelo porto do operador.

**Resultado da sombra MultiRio + Suape (produção, 20/10/2026; desembarques de 18/09 a 18/10; log
`virasana/sombra_rio_suape.log`)** — 3.016 ciclos (MultiRio 2.016, Suape 1.000; ~1.450 desembarques/dia no escopo,
+30% sobre Santos). Base de comparação: a sombra de Santos de 15/10 (`virasana/sombra_pos2.log`, 10.017 ciclos).

| | MultiRio + Suape | Santos |
|---|---|---|
| passagem (embarque no mesmo operador) | 50% | 24% |
| entregue pelo operador (dos quais voltaram ao mesmo operador) | 22,5% (52%) | 34% (21%) |
| TRA (recinto seco) | 1 ciclo | 23% |
| aberto no operador | 24% (16% da amostra com < 7 dias) | 11% (amostra sem < 7 dias) |
| incompletos / saída duvidosa | 1,2% / 1,0% | 2% / 0,04% |
| CE alta / média / baixa / sem CE | 31% / 30% / 1% / 38% | 60% / 8,5% / 0,5% / 31% |
| CEs declarados nos eventos por contêiner | 0,50 | 0,80 |
| escala declarada no desembarque NÃO confere com o manifesto do CE (não vazios) | 9,6% | 1,3% |
| ciclos com escaneamento | 12% (MultiRio 19%, Suape 3%) | 65% |

- **Imagens: critério atendido** — MultiRio +3 h em 381 de 383 inspeções (resíduo ≤ 10 min), Suape 28 de 29;
  a defasagem do item 1 está confirmada. 38 inspeções "em trânsito" (8,5%; Santos 1,4%), 34 delas no ICTSI:
  contêiner do MultiRio escaneado no ICTSI (escâner compartilhado no Caju?) — a confirmar.
- **Ciclos: no padrão de Santos, com o perfil de porto concentrador** — metade é passagem (transbordo e
  cabotagem), quase não há TRA, metade das entregas volta vazia ao mesmo terminal (a fronteira já trata);
  incompletos abaixo de Santos. O "aberto no operador" maior se explica em boa parte pela amostra mais recente
  (`--ignorar-ultimos-dias 2`); separar por idade no CSV.
- **CE: critério NÃO atendido** — confiança alta em 31%. Os eventos declaram menos CE (0,50 por contêiner) e a
  confiança média ("só o Mercante, uma família possível") é a regra. Mais grave: a escala declarada no desembarque
  não confere com o manifesto do CE escolhido em 9,6% dos não vazios (Santos 1,3%) — suspeita: o CE de OUTRA perna
  (a cabotagem ou o transbordo seguinte, com escala dentro dos ±15 dias) ganha do CE da chegada. Contribuem as
  duas regras fixas de Santos: `carga_ciclo._escalas` prefere as datas de BRSSZ, e `ces_do_ciclo.importacao_de_santos`
  exige o prefixo 15 no CE (pelos exemplos do D1, o CE emitido no Rio começa com 13 e o de Suape com 07; o
  estrangeiro, com 00).
- **Suape tem pouco ganho em imagem** — 3% dos ciclos com inspeção e 93% dos cards BRSUA sem imagem no D1: o
  terminal quase não transmite inspeção de importação. O ganho lá seria só o acompanhamento (entrega, passagem).
- ~~CE: critério NÃO atendido~~ — **CORRIGIDO pelo CSV** (`virasana/sombra_rio_suape.csv`, mesma rodada): a confiança
  média é a carga de PASSAGEM com CE emitido fora (prefixos 00, 12, 17, 18...), que nunca é declarado nos eventos
  do terminal — e a escala confere em 100% desses. Separando por tipo de CE:

  | | MultiRio | Suape |
  |---|---|---|
  | CE de importação local (Rio 13, Suape 07): ciclos / confiança alta | 332 / **69%** | 679 / **85%** |
  | ... fluxo passagem / com escaneamento | 0 / **78%** | 3 / **3,5%** |
  | CE emitido fora (transbordo, cabotagem): ciclos / passagem / média | 653 / 82% / 96% | 165 / 87% / 41% |
  | sem CE (passagem, 59% no MultiRio) / vazio no Mercante | 615 / 416 | 122 / 34 |
  | escala declarada não confere (não vazios com CE) | 4% | 14% (37 deles com CE alta: dado de escala do terminal) |
  | aberto, por idade do desembarque < 7 / 7–13 / 14–20 / 21+ dias | 80% / 40% / 15% / 6% | 68% / 22% / 7% / 4% |

  A hipótese da "outra perna" caiu; a preferência por BRSSZ não pesa (nenhum CE emitido em Santos no MultiRio, 6 em
  Suape); o prefixo 15 de `importacao_de_santos` só afeta o TRA provisório, que não existe nesses portos. As
  propostas (A) e (B) da rodada anterior ficam sem necessidade para ampliar. Pontos de atenção: no MultiRio, 27% da
  importação local com 21+ dias ainda "no operador" (Suape 10%) — conferir saídas não informadas em amostras;
  **Suape quase não transmite inspeção de importação** (3,5%; no D1 os cards BRSUA ficaram 93% sem imagem também no
  Mongo) — dado a cobrar do terminal, não da heurística.

**Diretriz do usuário (20/10/2026)**: a base recebe, fora de Santos, só os dados dos OPERADORES PORTUÁRIOS, e
nesses portos há poucos recintos secos — a carga quase sempre fica no operador. O ciclo "simplificado" (só a parte
do operador) deve cobrir a quase totalidade dos casos; o usuário vai pesquisar os demais recintos secos do Brasil
para começar a alimentar a base. O núcleo atual já é esse ciclo quando não há eventos de seco (MultiRio e Suape:
1 TRA em 3.016 ciclos).

**Eventos e imagens por operador fora de Santos** (D1, 14 dias; a verificação pedida pelo usuário — por operador,
se a base tem EmbarqueDesembarque, AcessoVeiculo, PesagemVeiculo, InspecaoNaoInvasiva e imagem no Mongo):

| operador (`e_operador`) | D / E | acessos (sem contêiner) | pesagens (sem contêiner) | inspeções | imagem no Mongo | ciclo simplificado |
|---|---|---|---|---|---|---|
| 7921303 MultiRio | 13.316 / 12.245 | 9.358 (60%) | 5.649 (71%) | 5.271 | 99% (+3 h) | **sim** (sombra OK) |
| 4931303 Suape | 6.209 / 7.431 | 14.936 (37%) | 278 (0%) | 1.133 | 100% (+3 h) | **sim** (sombra OK; pouca inspeção) |
| 3111401 Pecém | 7.977 / 6.562 | 21.274 (70%) | 9.602 (63%) | 5.034 | 57% (+3 h) | a medir na sombra |
| 7921302 Rio ICTSI | 4.486 / 3.787 | 11.935 (**100%**) | 12.555 (77%) | 13.395 | 99% (+3 h) | não: acesso sem contêiner (saída só pela placa) |
| 7961304 Itaguaí | 1.584 / 1.205 | **0** | **0** | 233 | 100% (+3 h) | não: sem acesso, só D/E e inspeção |
| Santos (referência) | 65.611 / 72.939 | 262.649 (53%) | 259.685 (49%) | 69.334 | — | — |

Fora do cadastro (`ovr_recintos` sem `e_operador`), com todos os eventos e volume de terminal: **9101602, 9981403,
9801303** (12 a 16 mil D cada) — as imagens deles NÃO estão no Mongo; e 0301304/0301302 (acessos, pesagens e
inspeções, sem D/E). Entrar no ciclo exige cadastrá-los (decisão do usuário, no Fichas).

**Resultado do P0 — pesagens (produção, 20/10/2026; pesagens de 06 a 20/10; log `virasana/pesagens.log`; amostra de
200 pesagens com contêiner nos 15 recintos de maior volume — MultiRio, Suape e ICTSI ficaram fora da amostra)**:
- **A pesagem é um evento de gate**: em ~95% há acesso efetivado da mesma placa no mesmo recinto em até 2 min (até
  10 min na Transbrasa; Pecém 18% sem acesso em 6 h). Captura automática ~100% nos terminais (Pecém 36%, Movecta 22%,
  Suape 0%). Placa em 100% — **nenhuma pesagem sem placa: o RTG não chega à base**.
- **Divisão**: 10 kg na maioria; 20 kg no MultiRio, Marimex, Localfrio CLIA, Fassina e outros; 1 kg no ICTSI e no
  Deicmar Outeirinhos. Peso **fora da divisão ≤ 1%** em todos: sinal fraco, fica como marca.
- **Tara do conjunto**: par na visita (mediana 14.940 kg; p10 11.950, p90 16.350) em 34–64% nos terminais;
  **`taraConjunto` da API − par: mediana 0, p10 −10, p90 +160 kg; > 500 kg só em 8%** — onde dá para comparar, ela
  é confiável. Sem usá-la, "sem tara" foi 34–83% da amostra.
- **Medido x declarado (prévia)**: coerente 5–48%, digitação 1–15%, divergência 1–14%; diferença p10 −100%,
  mediana 0, p90 +88%. As amostras mostram **quatro defeitos das regras provisórias** — o número de divergências
  está inflado:
  1. **contêiner vazio na pesagem** (bruto − conjunto ≈ tara do contêiner: devolução/retirada de vazio) virou
     "digitação ÷100" ou "sem dado" (ex.: 9.470 − 7.310 − 2.150 = 10 kg x 4.904 declarados);
  2. **pesagem sem o contêiner** (o caminhão chega para retirar, com o número já informado: bruto ≈ conjunto) —
     não é a pesagem carregada;
  3. **declarado = item de UM CE** (o emitido por último, até 120 dias antes): em carga consolidada cada filhote tem
     só a sua parte do peso do contêiner; e uma pesagem de EXPORTAÇÃO é comparada com a importação anterior do mesmo
     contêiner (ex.: 26.850 medidos x 4.801 declarados). Falta validar a mesma operação;
  4. **padrões de digitação com a tolerância de 5% explicam diferenças pequenas** (+8% e −7% viraram "dígitos
     vizinhos trocados"); declarado de 1 kg virou "÷100".
- A consulta das candidatas a tara por placa estourou o `max_statement_time` em 2 lotes (138 s) — no P1, buscar
  pela visita (contêiner + janela curta).
- Proposta de calibração (P0b) e desenho do P1: na resposta de 20/10/2026. O usuário aprovou usar a `taraConjunto`
  da API como 3ª fonte da tara (depois do par na visita e da pesagem vazia em 7 dias), com a origem marcada.

**P0b — calibração das pesagens: FEITA no código (20/10/2026), falta rodar** (`pesagens_ciclo.py` +
`diagnostico_pesagens.py`; testes `test_pesagens_ciclo.py` e `test_diagnostico_pesagens.py`; mutantes 26/26; gate OK):
- categorias novas: **contêiner vazio na pesagem** (|medido| ≤ 800 kg), **pesagem sem o contêiner no conjunto**
  (medido < −800 kg, ou a outra pesagem do mesmo contêiner na visita é mais pesada em 1 t), **diferença moderada**
  (5 a 20%), **declarado inválido** (< 100 kg); digitação só acima de 20% (P0: os padrões "explicavam" +8%, −7%);
- tara do conjunto: par na visita (agora também a pesagem do MESMO contêiner 1 t mais leve — o caminhão chegando para
  retirar já com o número) > vazia em 7 dias > `taraConjunto` da API (4 a 30 t), com a origem no exemplo;
- peso declarado do contêiner pela família de CEs: item do master; sem ele, soma dos filhotes;
- **mesma operação**: só compara pesagem com desembarque do contêiner até 60 dias antes (no operador do desembarque ou
  noutro recinto); sem desembarque antes = outra operação, não comparada (`--d-e-trocados 7921302` para o ICTSI);
- amostra: os 15 maiores + MultiRio, Suape, Pecém e ICTSI; consulta por placa em lotes de 100 (P0 estourou o tempo);
- quadros novos na seção 6: geral por operação e classificação, **por origem da tara** (confere a `taraConjunto`
  contra o par), origem do declarado e diferenças por operação.
```
cd /home/ajna/apps/ajna/virasana
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py --d-e-trocados 7921302 \
    --saida pesagens2.log
```
Com os números: fixar as tolerâncias e desenhar o P1 (tabela das pesagens do ciclo, ligadas à saída do operador ou à
entrada no seco, mesma placa do acesso).

**Resultado do P0b (produção, 20/10/2026; `virasana/pesagens2.log`; ~3,6 mil pesagens em 19 recintos, sem estouro de
tempo)**:
- **Tara pelo par: 93 a 100%** nos terminais e secos de Santos (P0: 34 a 64%) — o par com o MESMO contêiner (o
  caminhão chegando para retirar) resolveu. `taraConjunto` − par: mediana 0, p90 +180 kg, > 500 kg em 9% (2.422
  casos). **Suape: a `taraConjunto` é a tara do CONTÊINER** (~4,6 t, igual à informada) e a captura é 100% manual — as
  pesagens de Suape não servem para comparar. Pecém: "pares" de 52 a 61 t (bitrem ou carga) e "vazia" de 4,4 t.
- **Recintos secos** (Transbrasa, Movecta, Marimex, CLIAs da Santos Brasil e Deicmar) — a chegada da importação:
  **84 a 92% coerente, 1 a 5% de divergência**; diferença p10 −4%, mediana 0, p90 +9%. **A comparação funciona.**
- **Terminais "no operador do desembarque"**: só 52 a 63% coerente e 16 a 26% de divergência (Santos Brasil, BTP,
  Embraport), quase sempre medido MUITO acima do declarado (+99%, +208%) — mistura a retirada da importação com o
  contêiner VOLTANDO carregado para exportação (sem D/E entre os dois). Terminais "em outro recinto": 24 a 36%
  coerente (exportação/baldeação).
- Declarado (comparáveis): item do CE 1.062, **item do master 949**, item do filhote 160, soma de filhotes 2.

**P0c — calibração com esses números: FEITA no código (20/10/2026), falta rodar** (mutantes 35/35; gate OK):
- **operação pela DIREÇÃO do acesso da mesma placa** (até 30 min): no operador do desembarque, SAÍDA = retirada
  (compara), ENTRADA = o contêiner voltando (exportação, devolução: não é a importação), sem acesso = à parte; noutro
  recinto, **recinto seco** = chegada (compara) e outro **operador portuário** (recinto com embarque/desembarque) =
  exportação/baldeação. O quadro por origem da tara só usa as operações da importação;
- **tara do conjunto plausível entre 8 e 30 t para as três fontes** (afasta Suape, os 52–61 t e a "vazia" de 4,4 t);
- seção 4: `vazia em 7 dias − par` e a `taraConjunto` por recinto (fora da faixa).
```
cd /home/ajna/apps/ajna/virasana
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_pesagens.py --d-e-trocados 7921302 \
    --saida pesagens3.log
```
Esperado: a retirada no operador perto dos 85–90% coerentes dos secos. Se vier, P1 compara só a RETIRADA (saída do
operador) e a CHEGADA AO SECO do ciclo, com tolerância de 5%/300 kg e faixa moderada até 20%.

**Resultado do P0c (produção, 20/10/2026; `virasana/pesagens3.log`)** — comparáveis por operação: **chegada ao seco
88% coerente** (moderada 9%, divergência 2,6%; diferença p10 −3%, p90 +4%); **retirada no operador 71%** (moderada
11%, digitação 4%, divergência 13%; p90 +71%); volta ao operador do desembarque 23% (divergência 41% — confirma: é
exportação/devolução); outro operador portuário (exportação, baldeação) fora. Tara pelo par: 84% coerente nas
operações da importação. Suape: `taraConjunto` 100% fora de 8–30 t (é a tara do contêiner) — sai pela regra genérica.

**Diretriz do usuário (20/10/2026)**: publicar em homologação e testar na prática com os usuários, melhorando as
heurísticas depois com novos diagnósticos; **sem código por recinto** — o recinto deve transmitir como manda a
especificação; se não transmite, o sistema terá erros e a Receita cobra a correção do recinto. (Por isso a destroca
do ICTSI fica DESLIGADA: o certo é o ICTSI corrigir o envio.)

**P1–P3 — implementação das pesagens: FEITA no código (20/10/2026), falta a DDL e a ativação** (testes
`test_pesagens_ciclo.py`, `test_servico_ciclo_de_vida.py` (`TestPesagensDoCiclo`), `test_navii_pesagem.py`; mutantes
30/30; gate OK; E2E da tela no navegador):
- **P1 — captura no ciclo** (`pesagens_ciclo.linhas_do_ciclo`, chamada pelo serviço a cada lote): para cada ciclo, as
  pesagens do contêiner entre o desembarque e o fim do ciclo. A até 30 min da SAÍDA do operador do desembarque é a
  **retirada**; a até 30 min da ENTRADA no seco, a **chegada ao seco**; as outras ficam como dado bruto. Das da etapa,
  a CARREGADA (a mais pesada da visita) é comparada: tara do conjunto pelo par na visita (mesma placa e recinto, até
  12 h; sem contêiner ou com o mesmo contêiner 1 t mais leve — buscado pela placa, uma consulta por dia) ou pela
  `taraConjunto` (8 a 30 t); tara do contêiner do item do Mercante (senão a do desembarque, senão a típica); declarado
  = item do master ou soma dos filhotes da família de CEs do ciclo. Divisão da balança por recinto: pesagens de 7
  dias, recalculada a cada 24 h. Tabela nova `ciclodevida_pesagem` (`ddl_pesagens_ciclo.sql`), substituída a cada
  reprocessamento do ciclo; sem a tabela, o serviço avisa no log e segue sem pesagens.
- **P2 — diagnóstico**: classificação (coerente, moderada, digitação provável, divergência relevante = "possível
  fraude", dois contêineres, contêiner vazio, sem o contêiner, declarado inválido, sem dado) e o código
  `diagnostico`; a PRINCIPAL do ciclo (a primeira comparada com classificação) e a **confirmação** pela outra pesagem
  comparada do ciclo (os dois medidos dentro da tolerância).
- **P3 — tela** (`virasana/navii/pesagem_navii.py`): no card, selo "Pesagem: …" com a diferença, barras declarado x
  medido, diagnóstico e confirmação, e a expansão "Dados brutos" com todas as pesagens do contêiner no ciclo (bruto,
  taras e origens, medido, declarado e origem, diferença, marcas "peso digitado" e "fora da divisão"). Filtros:
  **Pesagem** (possível fraude, digitação provável, qualquer diferença, coerente, com pesagem comparada), **Diferença
  mínima (%)** (para mais ou para menos) e **Ordem de grandeza** (medido ≥ 10× ou ≤ 1/10) — sobre a principal; nos
  filtros salvos e nas estatísticas. Sem a tabela, o card fica sem o bloco e o filtro responde com erro claro.

Ativação em **homologação** (depois do deploy do virasana):
```
-- 1. no banco de HOMOLOGAÇÃO (DBA):
SOURCE virasana/analytic_scripts/ciclo_de_vida/ddl_pesagens_ciclo.sql;
```
```
cd /home/ajna/apps/ajna/virasana
S=virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py
# 2. reiniciar o serviço de homologação (ele confere a tabela ao iniciar): supervisorctl restart ciclo_de_vida_v2_hom
# 3. preencher: refazer os ciclos de um período (as pesagens vêm junto), primeiro medindo
virasana-venv/bin/python $S --homologacao --refaz-desde 2026-10-13 --simula
nohup virasana-venv/bin/python $S --homologacao --refaz-desde 2026-09-20 > refaz_pesagens_hom.log 2>&1 &
```
Conferir: `SELECT operacao, diagnostico, principal, COUNT(*) FROM ciclodevida_pesagem GROUP BY 1, 2, 3`; abrir o
NAVI-I de homologação e filtrar por "Possível fraude". Produção: ver a seção **"Roteiro de produção — pesagens e
escopo fora de Santos"** no fim deste arquivo (passos, checagens e rollback).

**Sombra de Pecém (produção, 20/10/2026; `virasana/sombra_pecem.log` + CSV)** — 3.007 ciclos: passagem 35%, entregue
pelo operador 31%, aberto 31%, incompletos 2,4%. CE de importação local (prefixo 04): 1.055 ciclos, confiança alta
só 19% (média 75%; os eventos declaram 0,31 CE por contêiner, contra 0,50 no Rio/Suape e 0,80 em Santos), escala
declarada não confere em 21%, escaneamento em 31%, aberto com 21+ dias 15%. **Imagem: 72% das inspeções sem imagem
no Mongo** (167 de 600 com imagem, todas a +3 h) — como nos terminais fora do cadastro, a integração das imagens
falta. Nada quebra; o ciclo dá o desembarque e o escaneamento.

**Prioridades do usuário para esses portos (20/10/2026)**: desembarque, imagem, escaneamento e pesagem, nesta ordem
— a saída vem depois.

**Ampliação do escopo — FEITA no código (20/10/2026), falta ativar**: `carga_ciclo.OPERADORES_FORA_DE_SANTOS` =
MultiRio 7921303, Suape 4931303 e Pecém 3111401 (lista explícita; só entra quem também é `e_operador`). O serviço e a
sombra usam o mesmo escopo. `servico_ciclo_de_vida.py --refaz-desde DATA --refaz-operadores ...` cria os ciclos do
período só desses operadores, sem refazer Santos. Testes `test_escopo_com_operadores_fora_de_santos` e
`test_refaz_so_os_operadores_novos`. Efeitos:
- os cards do NAVI-I desses portos passam a ter a imagem pelo ciclo; o fallback continua para o resto;
- a completude "no operador" ganha as linhas deles (Suape ~97% sem escaneamento: dado real);
- o risco histórico passa a ver os CEs deles;
- ~2 mil desembarques/dia a mais (+40% sobre Santos).
Ativação:
```
cd /home/ajna/apps/ajna/virasana
S=virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py
virasana-venv/bin/python $S --homologacao --refaz-desde 2026-10-13 --refaz-operadores 7921303 4931303 3111401 --simula
# depois do deploy e do restart do serviço no supervisor (o escopo novo vale a partir dele):
virasana-venv/bin/python $S --refaz-desde 2026-07-19 --refaz-operadores 7921303 4931303 3111401
```

**Rio ICTSI — D3 (`diagnostico_acesso_pela_placa.py`, escrito em 20/10/2026, falta rodar)**: o problema lá é o ACESSO
sem contêiner (100%) — a saída do operador não se liga ao ciclo — e parte dos desembarques informada como "E" (D1:
57% da amostra). O desembarque, a imagem (99%) e o escaneamento (13 mil inspeções em 14 dias) existem. Regra
proposta, pura em `acesso_pela_placa.py`: o acesso recebe o contêiner da pesagem/inspeção da MESMA placa, no MESMO
recinto, a até 5 min; dois contêineres na janela = ambíguo. O D3 mede no ICTSI a cobertura por janela e direção, a
**precisão num controle que informa o contêiner** (BTP: deduz ignorando o informado e confere), o que vem depois de
cada D (inspeção, pesagem, embarque, saída deduzida) e os "E que parecem desembarque". Testes
`test_acesso_pela_placa.py` e `test_diagnostico_acesso_pela_placa.py`; mutantes 16/16 (com o escopo). Se a precisão
for alta, a regra entra na carga SÓ para operadores marcados (ICTSI), sem efeito nos demais; o ICTSI entra no escopo
depois de uma sombra com ela.
```
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_acesso_pela_placa.py --saida acesso_placa.log
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py --operadores 7921302 \
    --dias 30 --ignorar-ultimos-dias 2 --conteineres 3000 --mongo --csv sombra_ictsi.csv --saida sombra_ictsi.log
```

**Resultado D3 + sombra do ICTSI (produção, 20/10/2026; `virasana/acesso_placa.log`, `sombra_ictsi.log/.csv`)**:
- **O ICTSI informa embarque e desembarque TROCADOS**: dos 4.465 "D" em 14 dias, 100% sem nada depois no recinto
  (nem inspeção, nem pesagem, nem E); dos 3.769 "E", nenhum com D antes e 60% com inspeção/pesagem depois. Na sombra,
  2.997 de 3.000 ciclos "abertos no operador"; a escala do "D" confere com o manifesto do CE em 17% (MultiRio 96%) e o
  CE de importação do Rio (prefixo 13) não aparece. Exemplos do D1: MNBU3924552 — E em Santos 15/10, "E" no ICTSI
  16/10 18:07 e inspeção 18:25. Causa provável: "E" lido como Entrada (no terminal) e "D" como Despacho. A comunicar
  ao terminal/Alfândega. **Incluir o ICTSI sem destrocar criaria ~4,5 mil ciclos falsos a cada 14 dias.**
- **Regra da placa**: inspeção do ICTSI (e do BTP) vem SEM placa — só a pesagem serve. Precisão no controle BTP
  96,7% (3.463 iguais, 118 diferentes). Cobertura no ICTSI: entradas 35%, **saídas só 4%** (a pesagem com contêiner é
  quase só na entrada); com 30 min sobe a 26% nas saídas, mas casaria a pesagem de entrada da mesma visita — não usar.
  Serve para limitar o ciclo (reentrada), não para achar a entrega.
- **Destroca — FEITA no código (20/10/2026), desligada**: `carga_ciclo.OPERADORES_D_E_TROCADOS` (VAZIO: produção
  igual), `tipo_movimento(tipo, recinto, trocados)` na carga e `filtro_desembarques(escopo, trocados)` nas consultas
  de desembarque (sombra: `conteineres`, `desembarques_por_dia`; serviço: `conteineres_do_periodo`). A sombra mede com
  `--d-e-trocados 7921302` e escreve na seção 0 os operadores destrocados. Testes `TestDETrocados` (sombra) e
  `test_refaz_com_d_e_trocados` (serviço); mutantes 10/10; gate OK. Quando o ICTSI corrigir a origem, o histórico
  continua trocado: a destroca vai precisar de uma data de corte.
  ```
  cd /home/ajna/apps/ajna/virasana
  S=virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py
  # ICTSI destrocado: esperado escala conferindo, CE resolvido, inspeção DEPOIS do desembarque, passagens reais
  virasana-venv/bin/python $S --operadores 7921302 --d-e-trocados 7921302 --dias 30 --ignorar-ultimos-dias 2 \
      --conteineres 3000 --mongo --csv sombra_ictsi2.csv --saida sombra_ictsi2.log
  # Santos com a destroca: a seção 2b (fluxo gravado x novo) mostra o efeito nos ciclos que passam pelo ICTSI
  virasana-venv/bin/python $S --d-e-trocados 7921302 --conteineres 10000 --saida sombra_santos_trocados.log
  ```
  Ativar o ICTSI = pôr 7921302 em `OPERADORES_D_E_TROCADOS` E em `OPERADORES_FORA_DE_SANTOS`, depois desses números.
  Sem a saída informada, o ciclo de importação do ICTSI fica "aberto no operador" (até 93 dias, revisitado a cada 6 h)
  — a regra da placa (35% das entradas) pode limitá-lo na reentrada; decidir com a sombra.

## Roteiro de produção — pesagens e escopo fora de Santos (21/10/2026)

O que entra com o deploy do virasana (commits até "Pesagens no ciclo de vida (P1–P3)"):
- **pesagens** (P1–P3): o serviço grava `ciclodevida_pesagem` SE a tabela existir quando ele inicia; o NAVI-I mostra
  o bloco e os filtros SE a tabela existir (sem ela, segue como hoje);
- **escopo fora de Santos**: `OPERADORES_FORA_DE_SANTOS` (MultiRio 7921303, Suape 4931303, Pecém 3111401) vale
  assim que o serviço reiniciar com o código novo — ciclos novos desses operadores a partir dos eventos novos;
- a destroca do ICTSI fica DESLIGADA (`OPERADORES_D_E_TROCADOS` vazio; premissa: o recinto corrige o envio).

### Passos (nesta ordem)

```
-- 0. antes (DBA, banco de PRODUÇÃO): a tabela ainda não existe
SHOW TABLES LIKE 'ciclodevida_pesagem';
-- 1. criar a tabela (aditiva; rollback no fim do arquivo)
SOURCE /home/ajna/apps/ajna/virasana/virasana/analytic_scripts/ciclo_de_vida/ddl_pesagens_ciclo.sql;
SHOW CREATE TABLE ciclodevida_pesagem;
```
```
# 2. deploy do virasana (git pull) e restart da aplicação web (gunicorn) — a tela do NAVI-I com o bloco e os filtros
# 3. restart do serviço do ciclo de vida: ele confere a tabela AO INICIAR
supervisorctl restart ciclo_de_vida_v2
tail -f /var/log/virasana/ciclo_de_vida/access.log
#    esperado: SEM o aviso "pesagens: tabela ciclodevida_pesagem ausente"; nas voltas, "pesagens gravadas" e
#    "pesagem principal: ..." nas estatísticas
cd /home/ajna/apps/ajna/virasana
S=virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py
# 4. preencher as pesagens dos ciclos já gravados (fora do pico; o serviço contínuo segue rodando — uma parte que
#    colidir com ele só vai ao log e as outras seguem). Primeiro 30 dias; se a tela agradar, recuar mais.
nohup virasana-venv/bin/python $S --refaz-desde 2026-09-21 > refaz_pesagens.log 2>&1 &
#    progresso: grep "refazer:" refaz_pesagens.log  (partes de 5.000 contêineres)
# 5. criar os ciclos mais antigos dos operadores novos (até 93 dias; os últimos 30 já vieram no passo 4, que inclui o
#    escopo novo; só eles — Santos não é refeito)
nohup virasana-venv/bin/python $S --refaz-desde 2026-07-20 --refaz-ate 2026-09-21 \
    --refaz-operadores 7921303 4931303 3111401 > refaz_operadores.log 2>&1 &
```

### Checagens

**A. Serviço** — log sem erro novo; `virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/acompanha_ciclo_de_vida.py
--saida acompanha_pos_pesagens.log` com marcas gravadas há poucos minutos, fila ~0 e nenhum sinal de outro gravador.

**B. Banco** (só leitura; a tabela é nova e pequena):
```sql
-- 1. distribuição geral: operação x diagnóstico x principal
SELECT operacao, diagnostico, principal, COUNT(*) FROM ciclodevida_pesagem GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC;

-- 2. no máximo UMA principal por ciclo (esperado: nenhuma linha)
SELECT ciclo_de_vida_id, COUNT(*) FROM ciclodevida_pesagem WHERE principal = 1 GROUP BY 1 HAVING COUNT(*) > 1;

-- 3. a principal por recinto e operação (comparar com o P0c: chegada ao seco ~88% coerente, retirada ~71%)
SELECT codigoRecinto, operacao, diagnostico, COUNT(*) FROM ciclodevida_pesagem WHERE principal = 1
GROUP BY 1, 2, 3 ORDER BY 1, 2, 4 DESC;

-- 4. cobertura: ciclos com saída nos últimos 7 dias x com pesagem principal, por operador do desembarque
SELECT c.codigoRecintoDesembarque, COUNT(*) AS ciclos, SUM(p.ID IS NOT NULL) AS com_pesagem
FROM ciclodevida_ciclo_de_vida_importacao c
LEFT JOIN ciclodevida_pesagem p ON p.ciclo_de_vida_id = c.ID AND p.principal = 1
WHERE c.versao_heuristica = 'v2' AND c.data_saida >= NOW() - INTERVAL 7 DAY GROUP BY 1 ORDER BY 2 DESC;

-- 5. origem da tara x diagnóstico (o par na visita deve dominar; a taraConjunto, só onde não há par)
SELECT origem_tara_conjunto, diagnostico, COUNT(*) FROM ciclodevida_pesagem WHERE principal = 1 GROUP BY 1, 2;

-- 6. confirmação pela outra pesagem do ciclo (divergência CONFIRMADA por duas pesagens = a mais forte)
SELECT diagnostico, confirmacao LIKE 'confirmada%' AS confirmada, COUNT(*) FROM ciclodevida_pesagem
WHERE principal = 1 AND confirmacao IS NOT NULL GROUP BY 1, 2;

-- 7. amostra de "possível fraude" para conferir no Fichas (Consulta Contêiner: as pesagens brutas)
SELECT c.numeroConteiner, p.numeroCEmercante, p.data, p.codigoRecinto, p.operacao, p.peso_bruto, p.tara_conjunto,
       p.origem_tara_conjunto, p.tara_conteiner, p.peso_medido, p.peso_declarado, p.origem_declarado,
       p.diferenca_pct, p.confirmacao
FROM ciclodevida_pesagem p JOIN ciclodevida_ciclo_de_vida_importacao c ON c.ID = p.ciclo_de_vida_id
WHERE p.principal = 1 AND p.diagnostico = 'divergencia' ORDER BY p.data DESC LIMIT 30;

-- 8. balanças: divisão e pesos fora dela (P0: <= 1%)
SELECT codigoRecinto, divisao, SUM(fora_da_divisao) AS fora, COUNT(*) FROM ciclodevida_pesagem GROUP BY 1, 2;

-- 9. escopo novo: ciclos dos operadores fora de Santos e a situação (depois do passo 5)
SELECT codigoRecintoDesembarque, situacao, fluxo, COUNT(*) FROM ciclodevida_ciclo_de_vida_importacao
WHERE codigoRecintoDesembarque IN ('7921303', '4931303', '3111401') GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC;
```

**C. Tela do NAVI-I** (produção):
1. janela "Desembarque efetivo" padrão: cards com o selo "Pesagem: …" e o bloco medido x declarado; abrir "Dados
   brutos" e conferir um card contra a Consulta Contêiner do Fichas;
2. filtro **Pesagem = Possível fraude**: a contagem bate com a consulta 7 no mesmo período/porto (pelo card);
3. **Diferença mínima** 50% e **Ordem de grandeza**; os chips e o × de cada chip;
4. salvar um filtro com os campos de pesagem e reabri-lo; a aba Estatísticas mostra os rótulos novos;
5. tempo de resposta com o filtro de pesagem (rodapé da tela) — é um acesso indexado por candidato
   (`ix_pesagem_principal`), deve ficar perto da busca sem ele;
6. depois do passo 5: cards com destino BRRIO/BRSUA/BRPEC passam a trazer a imagem pelo ciclo (sem a faixa âmbar/
   laranja do fallback) e a linha do tempo.

### Rollback

- **Pesagens**: `supervisorctl stop ciclo_de_vida_v2` → `DROP TABLE ciclodevida_pesagem;` → `supervisorctl start
  ciclo_de_vida_v2` (com o serviço RODANDO, sem a tabela, cada lote falharia ao gravar as pesagens). O NAVI-I percebe
  a falta da tabela em até 10 min e segue sem o bloco.
- **Escopo fora de Santos**: esvaziar `OPERADORES_FORA_DE_SANTOS` em `carga_ciclo.py`, deploy e restart; os ciclos
  já criados desses operadores ficam (inofensivos) ou podem ser apagados pelo `codigoRecintoDesembarque`.

## Caso CAAU5545018: CE de cabotagem saindo de Santos no ciclo do desembarque (21–22/10/2026, CORRIGIDO)

Relato do usuário: o desembarque de 07/10/2026 do CAAU5545018 ficou com o CE 072603341989805, de **cabotagem com
origem BRSSZ** (um embarque em Santos), quando deveria ser o CE 152605316468892 (importação), que tem dois contêineres
— o outro, FANU1174955, ficou com o CE certo.

**Ferramenta** — `caso_ciclo.py` (só leitura no banco): extrai tudo o que o ciclo de vida lê para os CEs e contêineres
(Mercante com masters/filhotes/declarados, manifestos e escalas, contêineres vazios, eventos da API sem CPF/nome e
com as placas trocadas por apelidos, recintos e o ciclo GRAVADO) para um JSON; reprocessa num SQLite em memória com o
código atual, explicando por desembarque os eventos ligados, os candidatos (tráfego, origem/destino, manifesto e o
porto de carregamento dele, datas das escalas e distância, se o manifesto é da escala declarada no desembarque), os
CEs declarados, a escolha e o motivo; e compara o serviço com o gravado.

```
C=virasana/analytic_scripts/ciclo_de_vida/caso_ciclo.py
virasana-venv/bin/python $C --ces 072603341989805 152605316468892 --desde 2026-08-01 --saida caso_caau.log \
    --json caso_caau.json
```

**Causa (caso_caau.log, reprocesso igual ao gravado)** — NÃO foi o embarque (a hipótese do cenário sintético):
1. o desembarque (Santos Brasil, escala 26000454544, navio 9605279, viagem MM637A) DECLARA o CE de importação, e a
   entrada, a inspeção e a saída do CLIA também (4 declarações);
2. o manifesto desse CE (1526501771094, ESBCN -> BRSSZ, navio 9605279, viagem MM637A, início 07/10) NÃO tem escala
   ligada no Mercante (`escalamanifestoresumo`) — os outros manifestos do mesmo navio estão na escala 26000454544. Para
   `resolve_ces` o CE fica "sem escala";
3. a cabotagem (BRSSZ -> BRSUA, manifesto carregado em BRSSZ) tem a escala de CARREGAMENTO em Santos em 15/10, a 8 dias
   do desembarque: é a ÚNICA família plausível;
4. `_escolhe_familia` com uma só família plausível a escolhe mesmo sem declaração nenhuma (confiança média) — o CE
   declarado só desempata entre plausíveis ou decide quando não há nenhuma plausível (é o que salvou o FANU1174955).

Teste de regressão com os dados reais: `test_caso_ciclo_caau.py` (`dados/caso_ciclo_CAAU5545018.json`); o do
CAAU5545018 está como `expectedFailure` até a correção.

**Porto do recinto** — o ciclo de vida não sabe o porto do desembarque (`carga_ciclo._escalas` usa as escalas de
Santos, também para MultiRio/Suape/Pecém). Fontes que já existem: `ovr_recintos.cod_carga` (código do terminal no
Mercante, ex.: BRSSZ016; os 5 primeiros caracteres são o porto — editável no admin do Fichas e já usado pelo
`conformidade_manager` contra `Manifesto.codigoTerminalDescarregamento`), o porto da escala declarada nos
embarques/desembarques e o porto/terminal do manifesto do CE declarado. Medição: `diagnostico_porto_desembarque.py`
(proposta de porto por recinto e divergências; quanto vale o CE declarado no desembarque; quantos ciclos gravados têm
o principal de SAÍDA do porto — CSV com todos).

```
D=virasana/analytic_scripts/ciclo_de_vida/diagnostico_porto_desembarque.py
virasana-venv/bin/python $D --dias 7 --saida porto_desembarque.log --csv porto_desembarque.csv
```

**Medição (porto_desembarque.log, 14 a 21/10/2026: 145 mil embarques/desembarques, 47.500 ciclos gravados)**:
- `ovr_recintos.cod_carga` está VAZIO em todos os recintos — não serve hoje;
- o porto da escala declarada no evento é 100% constante por recinto, nos 16 com eventos (Santos: Santos Brasil,
  BTP, Ecoporto e Embraport BRSSZ; MultiRio e ICTSI BRRIO; Itaguaí BRIGI; Suape BRSUA; Pecém BRPEC; e os de fora do
  ovr_recintos: Navegantes BRNVT, Paranaguá BRPNG, Itapoá BRIOA); o terminal do manifesto confirma (BRSSZ016 Santos
  Brasil, BRSSZ058 BTP, BRSSZ057 Ecoporto, BRSUA002 Suape, BRRIO003 MultiRio, BRIGI001 Itaguaí);
- CE declarado no desembarque: Santos Brasil 93%, Ecoporto 89%, Suape 92%, BTP 48%; Embraport, Pecém, ICTSI e
  MultiRio NÃO declaram. Quando declarado, quase sempre está no Mercante e com o contêiner nos itens;
- manifesto do declarado SEM escala ligada: 0,2% (114, todos em Suape) — em todos, o navio e o início da operação
  conferem com a escala declarada;
- ciclos gravados com o principal saindo do porto: 0 na semana (o caso CAAU é raro). "Principal ≠ declarado" (42% na
  Santos Brasil) mistura o master declarado com o filhote principal (decisão do usuário) — o diagnóstico passou a
  separar "declarado na família gravada" de "declarado FORA da família gravada" (para medir de novo).

**Decisões (usuário, 22/10/2026)**: o ciclo de importação acontece no porto de descarregamento do manifesto / destino
final; pode haver desembarque em porto intermediário (motivo extemporâneo ou pedido da fiscalização), com armazenagem
no operador ou baldeação (desembarca e logo embarca prosseguindo viagem). Testes com casos-limite REAIS no repositório,
anonimizando só CPF, CNPJ e nomes (`caso_ciclo.py` não extrai essas colunas; a placa fica — `--apelida-placas` opcional).
Porto do recinto: NÃO precisa de coluna nova em `ovr_recintos` — sai da escala declarada em cada evento.

**Correção implementada (22/10/2026)** — genérica, sem código por recinto:
- C1 porto do desembarque (`CargaCiclo.portos_dos_desembarques`): o da escala declarada no evento; sem ela, o mais
  visto no recinto no lote; desconhecido = as regras de antes (escalas de Santos). As datas de cada candidato são as
  das escalas NESSE porto (sem escala nele, todas) — corrige também MultiRio/Suape/Pecém, que usavam as de Santos;
- C2 o CE cuja carga SAI do porto do desembarque (origem = porto) não é candidato (`ces_do_ciclo._sem_saida`); ele é a
  PERNA SEGUINTE da visita (`CEsResolvidos.seguimento`): não entra no ciclo, mas o CE dele declarado no embarque não é
  "outra família" — a baldeação fecha como passagem (antes da correção do porto, o embarque com o CE da cabotagem
  deixava o ciclo do CE de importação "incompleto");
- C2b entre famílias plausíveis sem declaração que desempate, a de destino no porto vence a escala mais próxima (MÉDIA);
- C3 manifesto sem escala ligada: o início da operação no porto de descarregamento vale como escala
  (`CargaCiclo.escalas_com_porto`);
- C4 (o CE declarado vencer uma única família plausível não declarada) NÃO foi feito: com C1–C3 o caso real fica com o
  CE certo e confiança ALTA, e a medição de "declarado FORA da família" ainda vai dizer se precisa.
Caso real: `test_caso_ciclo_caau.py` (sem `expectedFailure`); regras: `test_ces_porto_desembarque.py` (inclui a
baldeação); sombra: rótulo "perna seguinte" na comparação dos CEs. Mutantes: 23/23 mortos.

**Validação e ativação (produção)** — a sombra é só leitura e já usa o código novo:
```
cd /home/ajna/apps/ajna/virasana
SO=virasana/analytic_scripts/ciclo_de_vida/sombra_ciclo_de_vida.py
# 1. Santos: seção 5 (CEs atual x novo: "perna seguinte", "diferente", "mesma família") e situação/fluxo
virasana-venv/bin/python $SO --dias 30 --conteineres 5000 --ignorar-ultimos-dias 0 --saida sombra_porto.log \
    --csv sombra_porto.csv
# 2. fora de Santos (as escalas passam a ser as do porto do operador)
virasana-venv/bin/python $SO --dias 30 --conteineres 3000 --ignorar-ultimos-dias 0 \
    --operadores 7921303 4931303 3111401 --saida sombra_porto_fora.log --csv sombra_porto_fora.csv
# 3. depois do deploy e do restart do ciclo_de_vida_v2, refazer os ciclos recentes (o do CAAU5545018 é de 07/10)
nohup virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/servico_ciclo_de_vida.py \
    --refaz-desde 2026-09-22 > refaz_porto.log 2>&1 &
# 4. medir de novo (agora com "declarado FORA da família gravada")
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/diagnostico_porto_desembarque.py --dias 7 \
    --saida porto_desembarque2.log --csv porto_desembarque2.csv
```

**Sombras da correção (sombra_porto*.log/csv, 22/10/2026, desembarques de 22/09 a 22/10)**:
- Santos + fora (5.000 contêineres, 5.135 ciclos): CE igual 3.442, nenhum 1.661; MUDOU em 32 (0,6%) — novo 23 (CE de
  passagem ou de importação de outra unidade, antes não achado: manifesto sem escala/escala do porto), perdido 5 (o
  gravado era a cabotagem que SAI do porto depois — a classe do CAAU; contêiner vazio no Mercante), diferente 3 (Pecém:
  duas famílias de importação com escala próxima, confiança baixa), perna seguinte 1;
- fora de Santos (3.000 contêineres, 3.047 ciclos): mudou em 26 (0,9%) — novo 19, diferente 4 (Pecém), perna seguinte
  2, perdido 1;
- fluxo: 9 indefinido -> operador (o prazo do trânsito passou desde a gravação) e 7 passagem -> incompleto (o embarque
  declara um CE de EXPORTAÇÃO de outro porto em baldeação). CORREÇÃO (23/10): os 7 vieram da própria correção — a perna
  seguinte incluía uma cabotagem ANTIGA do contêiner saindo de Santos; com a família não vazia, o CE de exportação do
  embarque virou "outra família". Resolvido pela C6 e pela janela da perna seguinte (abaixo);
- sinal para a próxima regra: vários CEs "novos" de passagem têm a escala declarada no desembarque que NÃO confere com
  o manifesto do CE (pode ser a perna seguinte) — a escala declarada como evidência (C5) fica para os casos reais.

## Casos reais de regressão (22/10/2026)

Pedido do usuário: o ciclo de vida tem lógica complexa e o dado tem problema de qualidade — testes com dados reais
(anonimizados: sem CPF, CNPJ ou nome) ou sintéticos com as regras mais sólidas, para não haver regressão. Avaliação e
desenho, em três camadas:

1. **Corpus de casos reais com gabarito** (`virasana/tests/test_casos_reais_ciclo.py`): cada caso é um JSON extraído
   por `caso_ciclo.py` (só as colunas que o ciclo lê) em `virasana/tests/dados/casos_ciclo/`, com o gabarito por
   desembarque (CE principal, fluxo, situação e, quando importa, a confiança) e o PORQUÊ do caso. O gabarito é revisado
   pelo usuário a partir do `.log` do caso (a explicação de cada escolha). Defeito conhecido entra como `pendente`: o
   gabarito é o certo e o teste exige que o código ainda erre — ao corrigir, o teste manda tirar o `pendente`. O teste
   também falha se houver JSON extraído fora do gabarito. Primeiro caso: CAAU5545018. Lista a extrair:
   `dados/casos_ciclo/casos.txt` (25 casos, 39 contêineres, escolhidos nas sombras: o que a correção mudou e uma regra
   estável por fluxo e por tratamento de dado ruim). Tamanho: ~50 kB por caso.
2. **Invariantes** (`invariantes_ciclo.py`): o que vale para QUALQUER ciclo, com dado bom ou ruim — principal nunca de
   exportação nem saindo do porto do desembarque, sempre com o contêiner nos itens e na família; família/confiança
   coerentes; etapas em ordem; TRA concluído com a saída do seco, passagem/baldeação com o embarque. Verificadas em
   todos os ciclos de todos os casos (inclusive os sem gabarito) — são as "regras mais sólidas" e não dependem de
   revisão. Os cenários sintéticos do serviço não violam nenhuma.
3. **Auditoria da produção** (`audita_ciclos.py`, só leitura): as mesmas invariantes nos ciclos gravados de um período;
   cada violação é um candidato natural a caso do corpus (o ciclo de melhoria: auditoria -> caso real -> correção ->
   gabarito). Os sintéticos continuam onde já estão (`test_linha_do_tempo`, `test_ces_porto_desembarque`...): uma regra
   por teste, legível; gerar sintéticos aleatórios (fuzz) com as invariantes fica como possibilidade futura.

```
cd /home/ajna/apps/ajna/virasana
C=virasana/analytic_scripts/ciclo_de_vida/caso_ciclo.py
# os casos da lista: um JSON e um .log por caso em casos_ciclo/
virasana-venv/bin/python $C --casos virasana/tests/dados/casos_ciclo/casos.txt --desde 2026-08-01 --pasta casos_ciclo
# auditoria das invariantes nos ciclos gravados (depois do refaz)
virasana-venv/bin/python virasana/analytic_scripts/ciclo_de_vida/audita_ciclos.py --dias 30 \
    --saida auditoria_ciclos.log --csv auditoria_ciclos.csv
```

**Corpus carregado (23/10/2026)**: os 25 casos de `casos.txt` (39 contêineres, 2,1 MB) em `virasana/tests/dados/casos_ciclo/`,
com o gabarito em `test_casos_reais_ciclo.py` (+ o CAAU5545018): 26 casos, 51 desembarques (2 pendentes), nenhuma
violação das invariantes.
O gabarito aceita um CONJUNTO de CEs no empate legítimo, e a marca `pendente` é por desembarque. Mutantes contra o corpus:
7/10 mortos (os vivos — mapa de porto do recinto, desempate pelo destino, passagem rápida em não operador — não aparecem
nos casos; os testes unitários os matam). O que os casos mostraram:
- **perna seguinte** (Suape, Pecém) e **cabotagem que sai depois** (BTP, Embraport): a correção acerta — no
  HAMU4699375 a cabotagem que sai de Santos é o CE certo na CHEGADA a Suape;
- **CE de passagem achado agora** (escala "não confere"): baldeação Buenos Aires -> Buenaventura com manifesto de
  baldeação emitido em Santos — o CE é o certo; a escala declarada como evidência (C5) NÃO serve e foi descartada;
- **porto intermediário** (Vitória via Santos e Rio; Navegantes via Suape e Santos): o CE de importação vale nas
  passagens intermediárias, como o usuário descreveu;
- **PENDENTE — exportação de outro porto em baldeação** (C6, `embarque_exportacao_outro`): exportação Vitória -> Benin/EUA
  desce em Santos e reembarca declarando o CE de exportação; sem importação o ciclo fica sem CE (certo), mas o CE de
  exportação declarado no embarque conta como "outra família" e o ciclo fica INCOMPLETO — o certo é passagem concluída.
  Proposta genérica: o CE de exportação com o contêiner nos itens e origem em OUTRO porto (carga em trânsito) corrobora
  os eventos do ciclo, como a perna seguinte (sem entrar no ciclo);
- **PROPOSTA — contêiner consolidado com vários BLs únicos** (C7, `confianca_baixa`, `perna_seguinte_suape`): o mesmo
  contêiner nos itens de vários CEs do MESMO manifesto (sem master): hoje cada CE é uma família, o empate dá confiança
  baixa e só um CE entra no ciclo — o card do NAVI-I dos OUTROS CEs fica sem o ciclo e sem as imagens. Proposta: CEs do
  mesmo manifesto com o mesmo contêiner são uma família (todos no ciclo; principal o declarado, senão o primeiro);
- **Pecém, duas importações Jedá -> Manaus** em manifestos diferentes, mesma distância de escala: empate legítimo,
  confiança baixa (o gabarito aceita qualquer um).

**Fidelidade do porto (23/10/2026)**: quando a escala declarada não está no Mercante, o porto vinha do lote (o mais
visto no recinto) — o caso isolado ficava sem porto. Agora o serviço calcula o porto de cada recinto pelas escalas dos
últimos 7 dias, uma vez por dia (`CargaCiclo.portos_dos_recintos`, como a divisão das balanças), a sombra também, e a
extração guarda o mapa no caso (`meta.portos_dos_recintos`; os 25 casos e o CAAU receberam o mapa medido em 22/10).
Ordem: escala do evento -> porto do recinto -> lote.

**Auditoria** (`audita_ciclos.py`): a 1ª execução em 30 dias passou de 1 h e caiu com "MySQL server has gone away" — a
junção da família dos ~200 mil ciclos era quadrática (0,4 s agora) e a conexão ficou parada. Agora roda um dia por vez,
numa conexão nova, com o progresso na tela.

**Auditoria em produção (auditoria_ciclos.log, 23/10/2026: 204.777 ciclos v2 de 30 dias, 167 violações)**:
- 154 "CE principal com a carga saindo do porto" (Embraport 91, Santos Brasil 22, BTP 19, Suape 12, Pecém 10) — a
  classe do CAAU, gravada ANTES da correção do porto e ainda não refeita (quase todas até 10/10; o próprio CAAU5545018
  está lá) — some com o refaz;
- 17 "TRA concluído sem a saída do seco" — defeito NOVO: a linha do tempo concluía o TRA com a saída do seco de
  confiança baixa (CE de outra família), e a gravação não grava ligação baixa. Correção C8 abaixo.

**C6, C7 e C8 (aprovados pelo usuário em 23/10/2026; implementados no mesmo dia)**:
- **C6** exportação de OUTRO porto em trânsito (origem noutro porto e escala no porto do desembarque, na janela) entra
  no `seguimento`: corrobora os eventos, fora do ciclo — a baldeação de exportação de Vitória fecha como passagem;
- **janela da perna seguinte**: o CE com a carga saindo do porto continua fora dos candidatos, mas só é perna seguinte
  com escala nesse porto na janela (a cabotagem antiga do contêiner é outra operação);
- **C7** contêiner consolidado: os BLs únicos do MESMO manifesto com o mesmo contêiner são UMA família (todos gravados
  em `ciclodevida_ciclo_ce`; principal: o declarado, senão o primeiro). No caso real de Suape, 75 BLs no mesmo
  contêiner. No NAVI-I, o card acha o ciclo pela família GRAVADA (`busca_ciclos_navii`, uma consulta por página) e o
  selo "sem desembarque" da página não usa o atalho do BL único; os filtros verificados por candidato mantêm o atalho
  (desempenho) — limitação conhecida: contêiner + "só com desembarque" deixa de fora o BL que não é o principal;
- **C8** a saída do seco duvidosa (confiança baixa) deixa o TRA ABERTO, "saída do seco duvidosa (CE de outra família) —
  revisar" — como a saída do operador.
Corpus: os 2 pendentes acertam, o caso de confiança baixa virou `conteiner_consolidado` (alta, família com os 2 CEs) e
Suape tem a família com os 75 CEs. Mutantes: 10/10.

**Ativação**: sombra (Santos e fora) -> deploy (virasana) e restart do `ciclo_de_vida_v2` e do gunicorn (NAVI-I) ->
refaz desde 2026-09-22 -> auditoria de novo (esperado: as 154 somem; os 17 viram aberto ou acham a saída do seco).

**Refaz e 2ª auditoria (22/10/2026)**: o `--refaz-desde 2026-09-22` rodou da estação de trabalho e perdeu a rede —
24 de 41 partes com erro (parte 3: conexão reiniciada às 16:22; partes 19 a 41: rede inacessível às 17:27); só 85 mil
dos 202.692 contêineres refeitos. Como o refaz vai do desembarque mais recente para o mais antigo, as partes 19–41 são
os desembarques de 22/09 a ~09/10. A 2ª auditoria (`auditoria_ciclos2.log`, 204.180 ciclos v2) achou 118 ciclos com violação
(113 "carga saindo do porto" e 9 "TRA sem a saída do seco"; 4 com as duas), **todos** entre 22/09 18:10 e 09/10 03:58 — as partes
que falharam; nenhuma depois. Refazer essa janela no servidor e auditar de novo (esperado: 0). Robustez do refaz
(repetir a parte, listar as que falharam com o comando para refazer) e health check: `BACKLOG.md` (raiz do virasana).

**Refaz robusto e health check (23/10/2026)**: uma parte do refaz que falha é repetida até 3 vezes (30 s e 2 min de
espera, reconectando); no fim, o log lista o período de desembarque de cada parte não refeita e o comando para
refazê-la, e o programa sai com código 1 (`audita_ciclos.py` também sai com 1 quando há violação). O health check
diário (`virasana/saude/saude_navi.py`, ver `BACKLOG.md`) acompanha o serviço (alertas deste acompanhamento), as
invariantes dos desembarques das últimas 48 h, a cobertura semana a semana, as pesagens e — com `--nucleo-desde` — os
ciclos ainda não refeitos depois de uma troca do núcleo.
