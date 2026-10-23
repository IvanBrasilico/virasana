# Piloto do sinal "densidade acima da típica do NCM com cubagem estimada" — plano (rascunho para revisão)

> **SUSPENSO (24/10/2026), pela revisão do usuário:** nos CEs de Santos da `revisao5.csv` (todos de 20'), o peso
> confere com a pesagem e a imagem — a anomalia está na CUBAGEM declarada, não na mercadoria. O típico de
> densidade mistura 20' e 40' (muitos 40' declaram a capacidade, 68/76 m³, como cubagem), e 16 dos 30 da lista são
> de 20' (~17% dos contêineres). O sinal mede o jeito de declarar a cubagem; a associação com as fichas (10×) pode
> ser o perfil de quem declara assim. O selo "carga mais pesada que a mercadoria" induziria a erro. Ver BACKLOG.md.

Itens 16–18 do `BACKLOG.md`. Passo 0 (diagnóstico, 5 rodadas em produção, 23–24/10/2026) concluído; este é o plano
do piloto no NAVI-I. **Rascunho de 24/10/2026, aguardando a revisão do usuário** — nada aqui está aprovado.

## 1. O que se quer

Como analista de risco na importação, ver no card do NAVI-I, **antes da chegada**, quando a carga declarada é pesada
demais para a mercadoria e o volume declarados — com o motivo em números — e poder filtrar por isso, para escolher o
que examinar (imagem, descrição, histórico) e abrir a Ficha.

É um **piloto "em observação"**: não entra na pontuação do risco histórico, não bloqueia nada, não muda telas do
Fichas. Serve para medir se o sinal ajuda o analista (Fichas abertas pelo card e o resultado delas), já que as Fichas
antigas são evidência fraca (usuário, 23/10/2026).

## 2. O sinal (o que foi validado na 5ª rodada — `regras_declaracao.py`)

Num **contêiner cheio (FCL)**, na família de CEs (peso e cubagem do master; sem ele, a soma dos filhotes), com **um
NCM só** (NCM dos capítulos 01 a 97):

1. **peso comparável**: de 100 kg até 110% da carga máxima do tipo (fora disso é digitação — o 16b);
2. **densidade** = peso ÷ cubagem, com a cubagem até 110% da capacidade do tipo;
3. **típico do NCM**: mediana e MAD da densidade em escala log, aprendidos dos últimos 30 dias, no nível mais
   específico do NCM (8, 6 ou 4 dígitos) com 30+ contêineres (piso de dispersão de ~12%);
4. **marca** quando: densidade **3 ou mais desvios acima** do típico **e** cubagem **em m³ inteiro** (estimada) **e**
   cubagem **plausível** (10%+ da capacidade do tipo; abaixo disso é valor simbólico, como 1 m³).

Na 5ª rodada (30 dias, 158 mil CEs avaliáveis): **1,18% dos CEs** (1.867 em 30 dias; ~60 por dia). Contra o
controle da mesma época: fichas positivas **10,4×** (32 de 146), RVFs 6,0×, falsa declaração 6,3×, RVF com peso
divergente 6,4×. É a **combinação**: a cubagem redonda sozinha (56% dos contêineres) dá 1,3×; densidade acima com
cubagem com fração, 1,1×. Leitura: o peso é real, a cubagem foi inventada e a densidade não combina com o NCM
declarado — a mercadoria pode não ser a declarada.

## 3. Arquitetura (o caminho do risco histórico, que já está em produção)

Módulo novo **`virasana/declaracao/`** (o serviço do ciclo só chama), só aditivo:

- **Regras**: `regras_declaracao.py` passa para `virasana/declaracao/regras.py` (o diagnóstico importa de lá: uma
  regra só para o diagnóstico e a produção). Só as partes do sinal escolhido entram no caminho de produção.
- **`ddl_declaracao.sql`** (o DBA executa) e **`tabelas.py`** (Core; teste confere que batem):
  - `declaracao_conteiner`: **uma linha por (CE, contêiner)** de contêiner FCL de um NCM só — a família repete a
    medida em cada CE dela, para o NAVI-I achar pela chave do card. Colunas: `numeroCEmercante`, `codigoConteiner`
    (chave única), `familia`, `ncm`, `tipo` (ISO), `peso`, `cubagem`, `densidade`, `cubagem_redonda`,
    `cubagem_simbolica`, `ce_id` (ID no Mercante), `ce_alterado_em`, `criado_em`; e, da marcação, `chave_tipico`,
    `desvios`, `marcado`, `versao_id`, `calculado_em`. Índices: a chave, `criado_em`, `versao_id`, `(marcado, criado_em)`.
  - `declaracao_versao`: um conjunto de típicos (janela, constantes, contagens) — e `declaracao_tipico`: por versão
    e chave de NCM, `n`, mediana, MAD, p10, p90 (a memória de cálculo do motivo).
  - `declaracao_marca`: marcas d'água próprias.
- **Lotes** (`lotes.py`, como o do risco: `--carga`, `--tipicos`, `--marca`, `--etapa`, `--situacao`, `--simula`):
  - **extração**: CEs novos pela marca no ID do `conhecimentosresumo` e retificados pela `create_date` (a última
    alteração, indexada); para cada CE, a família completa (master e filhotes) e a linha de **todos** os CEs dela
    (filhote que chega depois refaz a família). Carga inicial: os últimos 30 dias (2,5 min na estação, 5ª rodada).
  - **típicos**: versão nova 1x/dia, das linhas da própria tabela (`criado_em` nos últimos 30 dias) — sem reler o
    Mercante; segundos.
  - **marcação**: as linhas da fila (`versao_id` vazia ou antiga), em lotes, com os típicos da versão atual.
- **Etapa no serviço do ciclo** (`etapa.py`, `EtapaDeclaracao`, como a `EtapaRisco`): no fim de cada volta —
  extração, típicos (no máximo 1x/dia), marcação; transações próprias; limite de 60 s; qualquer erro só vai ao log;
  `--sem-declaracao` desliga; `--simula` não roda.
- **Health check**: verificação nova (marca d'água em dia, fila da marcação, idade da versão dos típicos, taxa de
  marcados do dia fora da faixa esperada de 0,5% a 3%).

## 4. NAVI-I (`virasana/navii/declaracao_navii.py`, como o `risco_navii.py`)

- **Selo no card** (só quando marcado): "**Densidade acima da típica do NCM — em observação**" (nome a decidir).
- **Motivo** (recolhível, como a memória do risco): "25.400 kg em 23 m³ declarados = 1.105 kg/m³; típico do NCM 0402
  (leite em pó): 384 a 535 kg/m³ (1.182 contêineres, 30 dias) — 8,2 desvios. Cubagem em m³ inteiro: estimada pelo
  declarante." E, se houver, "outros N contêineres desta família também marcados" (os marcados vêm em embarques
  inteiros).
- **Filtro** "Densidade acima da típica (em observação)", verificado por candidato (`_existe` na chave única de
  `declaracao_conteiner` com `marcado`), como o filtro de risco; filtros salvos, chips e coleta de uso tratam o novo
  parâmetro. Medir com `navii/diagnostico_sql.py --analyze` (cenário novo).
- **Abrir Ficha**: com o selo, o motivo entra no resumo do risco (observações) e na memória interna da RVF
  (`ficha_navii.py`) — o analista leva o porquê, e as Fichas com o sinal ficam identificáveis depois.
- Sem as tabelas, card sem o selo e filtro com erro claro (como o risco). Testes: unidade, SQLite, prefixo `/virasana`
  (`test_prefixo_proxy.py`) e roteiro no navegador.

## 5. Medição do piloto

A coleta de uso já grava, por pesquisa, os cards enviados, vistos e examinados e as Fichas abertas pelo card. Um
script de acompanhamento (`acompanha_piloto.py`, só leitura) cruza com `declaracao_conteiner`:

- marcados por dia; cards marcados enviados, vistos e examinados; uso do filtro;
- **Fichas abertas por card visto, com e sem o selo** (o efeito do sinal na escolha do analista);
- depois, o **resultado** dessas Fichas (RVF, infrações, TG, perdimento), comparado com as Fichas abertas pelo card sem
  o selo — o retorno chega tarde (semanas a meses);
- concentração por importador (os embarques inteiros) e sobreposição com o risco histórico alto/médio.

## 6. Fases

| Fase | Entrega | Grava? | Visível? |
|---|---|---|---|
| P0 | Revisão dos 30 CEs (`revisao5.csv`) pelo usuário no NAVI-I; decisões da seção 8 | não | não |
| P1 | Módulo, DDL, extração, típicos, marcação, `lotes.py`; carga dos 30 dias à mão; conferir ~1,2% marcados e a lista igual ao diagnóstico | sim (tabelas novas) | não |
| P2 | Etapa no serviço do ciclo + verificação no health check | sim | não |
| P3 | NAVI-I: selo, motivo, filtro, Abrir Ficha com o motivo | não (tela) | **sim** |
| P4 | Acompanhamento (script); relatório mensal; decisão ao fim do piloto | não | não |

P1 e P2 não aparecem para ninguém — podem andar enquanto a revisão (P0) acontece; **P3 só depois da revisão**.

## 7. Riscos e cuidados

- **Produção**: módulo e tabelas novos, etapa isolada (erro só no log, flag para desligar); no NAVI-I, um selo e um
  filtro a mais, no padrão do risco. Nada no Fichas legado.
- **Embarques inteiros do mesmo importador**: dezenas de cards marcados de uma vez podem cansar o analista — o card
  mostra quantos da família estão marcados; medir a sobreposição com o risco histórico do importador.
- **Típico em 4 dígitos** (97% dos NCMs do Mercante): posições heterogêneas; o motivo mostra a faixa típica para o
  analista julgar. Refinamento possível: típico também pelo histórico do importador.
- **Deriva**: se os declarantes mudarem o jeito de declarar a cubagem, o sinal muda — o health check vigia a taxa do dia.
- **Retificação**: CE retificado volta à fila (pela `create_date`), como no risco.

## 8. Decisões para o usuário (com a recomendação)

1. **Nome do selo**: "Densidade acima da típica do NCM" (técnico) ou "Carga mais pesada que a mercadoria declarada"
   (o que o analista entende de cara)? *Recomendo o segundo, com o técnico no motivo.*
2. **Quem vê no piloto**: todos os usuários do NAVI-I, ou um grupo (perfil Supervisor ou uma lista de CPFs) no começo?
   *Recomendo todos, com o "em observação" — mais dados em menos tempo.*
3. **Fora da pontuação do risco histórico** durante o piloto. *Recomendo: sim; ao fim, decidir se vira dimensão.*
4. **Duração e critério**: *recomendo 60 dias; continuar se a taxa de Ficha por card visto com o selo for maior que sem
   o selo e os resultados que já chegaram não contrariarem.* O critério final é seu.
5. **Janela dos típicos**: 30 dias (a validada) ou 90 (mais estável)? *Recomendo 30, como foi validado.*
6. **Peso acima do típico (18a+)** fora do piloto (5,7× nas positivas, mas poucos casos) — só no motivo, quando houver.
   *Recomendo assim.*
7. **Mover as regras** para `virasana/declaracao/regras.py` (o diagnóstico passa a importar de lá). *Recomendo.*
