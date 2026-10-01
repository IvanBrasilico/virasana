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
   Esses ciclos ficam presos para sempre (e podem derrubar o loop).
3. **`.one_or_none()` com mais de um ciclo** para o mesmo contêiner/CE (retransmissões, desembarques
   repetidos) levanta `MultipleResultsFound` em `saidas_novas_operadores` e `busca_escaneamentos_novos`
   — sem `try`, a exceção derruba a iteração do loop.
4. **Saída só até 72 h** (`HORAS_PERMANENCIA_MOLHADO`) depois do desembarque: contêiner que fica mais
   tempo no operador (até 90 dias) nunca ganha saída → ciclo aberto até os 93 dias.
5. **Agendamento conta como saída/entrada**: as buscas de AcessoVeiculo não filtram `operacao = 'C'`
   (acesso efetivado) — um agendamento (G) pode virar a "saída"; também não tratam retificações.
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

## Plano (proposto — ver resposta de 01/10/2026; nada implementado ainda)

0. **Medir** (`diagnostico_ciclo_de_vida.py`; o usuário está rodando em produção em 01/10/2026):
   estado dos pendentes por estágio e idade, tráfego, passagem/embarque, saídas depois de 72 h,
   `saida_id` sem `data_saida`, escaneamentos achados pelo CE (e em que recinto/defasagem — mede o
   terceirizado e a data errada), risco de outra operação, defasagem de horário imagem x inspeção.
1. **Correções seguras** dos bugs 1, 2, 3, 5, 7, 8, 11 (com testes em SQLite e simulação do loop).
2. **Situação + agenda no ciclo** (colunas novas, nulas — ALTER TABLE pelo DBA): `fluxo`
   (operador/TRA/passagem/baldeação/abandono), `situacao`/`motivo_conclusao`, `tentativas`,
   `proxima_tentativa`. O loop só visita quem está "vencido", com espera crescente; ciclos sem
   novidade param de ser reconsultados.
3. **Fechamento por tipo**: passagem/baldeação concluem no embarque (EmbarqueDesembarque E);
   cabotagem marcada como "escaneamento não obrigatório"; abandono após 90 dias.
4. **Escaneamento ancorado em contêiner + tempo** (ver "Escaneamento: como achar"): janela do fluxo
   recortada pelos eventos do próprio contêiner, em qualquer recinto; CE e recinto só corroboram;
   imagem no Mongo só por `numeroinformado` + `dataescaneamento`, com o fuso corrigido; registrar a
   **confiança/origem da ligação** em `..._escaneamentos`.
5. **CE do ciclo pelo Mercante** (Item.codigoConteiner + escala, como hoje); o `numeroConhecimento` do
   desembarque/inspeção só corrobora ou desempata entre CEs candidatos — divergência vira alerta,
   não troca automática.
6. Reprocessar os pendentes antigos com as regras novas (script único, em lotes, com relatório).
