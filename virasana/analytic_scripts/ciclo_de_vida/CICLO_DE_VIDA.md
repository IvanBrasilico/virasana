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

## Princípio geral da heurística (usuário, 01/10/2026)

Contêiner + intervalo de datas **nunca basta** para dizer que um evento é do mesmo ciclo — sobretudo
ao alargar tolerâncias. A API tem erros e omissões: uma saída não informada pode fazer a "próxima saída"
encontrada, muitos dias depois, ser de OUTRA operação (contêiner já vazio, já em exportação...). Toda
ligação precisa de **validação adicional de que ainda é a mesma operação**.

## Plano v2 (proposto em 01/10/2026 — aguardando aprovação)

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

**CE**: candidatos do Mercante (contêiner + escala), **sem exportação** para desembarque; declarado
nos eventos só corrobora/desempata. **Hipótese do usuário (01/10) a confirmar** com
`diagnostico_ces_ciclo_de_vida.py`: os eventos informam master e house(s) de forma intercambiada — se
sim, a unidade de comparação passa a ser a **família do CE** (master + filhotes, via
`Conhecimento.numeroCEMaster`): CE declarado da mesma família corrobora a mesma operação; só CE sem
parentesco desconfia. O "OUTRO CE" das ligações atuais pode ser isso ou bug do algoritmo antigo; sem candidato, o ciclo segue (a operação física existe) com o CE
declarado guardado como informação.

**Processamento orientado a eventos** (resolve as horas de loop): marca d'água do último id lido de
cada tabela de eventos; a cada volta, só os contêineres com evento novo têm a linha do tempo refeita
(uma consulta por tabela por lote de contêineres). Ciclo sem novidade não é reconsultado. Prazos
(abandono etc.) fecham por UPDATE em lote. Desembarques novos sem o teto de 2.000 por volta.

Etapas (cada uma testada, com aprovação antes de tocar o loop de produção):

1. **Diagnóstico v2** (curto, só leitura, por amostragem): por que 52% sem CE (CE declarado existe no
   Mercante? prefixos? escala?); OUTRO CE é master/filhote?; operadores fora da lista fixa do loop
   (`e_operador`) — seção 0 de `diagnostico_ces_ciclo_de_vida.py`; saídas tardias que passam/não passam na continuidade; passagem com continuidade; scanner
   x gate; fuso por recinto e janela da imagem após +3 h; peso x tara no desembarque; atraso de criação.
2. **Núcleo puro** `linha_do_tempo.py`: recebe os eventos de um contêiner e devolve os ciclos com as
   ligações e a confiança — sem banco; testes com os casos reais das amostras.
3. **Modo sombra**: roda o núcleo sobre um período e grava num relatório/tabela à parte, comparando
   com o ciclo atual (conclusões, escaneamentos, divergências), sem alterar nada.
4. **DDL aditiva** (usuário/DBA): situação, fluxo, motivo, confiança e origem das ligações, marcas d'água.
5. **Troca do loop** para o processamento por eventos (o atual fica para rollback).
6. **Reprocessamento** dos pendentes em lotes, com relatório.

Ajustes pontuais de baixo risco que podem vir antes (se aprovados): tirar o atraso dos desembarques
novos; corrigir o fuso da busca no Mongo; desligar o atalho `saidas_novas_operadores` (código morto).
