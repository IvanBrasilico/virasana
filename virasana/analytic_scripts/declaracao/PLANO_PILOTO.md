# Piloto do sinal "carga pesada para a mercadoria declarada" — plano (para revisão)

> **ADIADO (postponed) em 24/10/2026, decisão do usuário:** na revisão da `revisao7.csv`, imagem e pesagem dos
> selecionados eram coerentes com o declarado — as seleções não fazem sentido. A análise volta do começo no futuro,
> com outro método e outros rótulos (Fichas não servem de rótulo positivo). Lições e ideias em `CONTEXTO.md`.

Itens 16–18 do `BACKLOG.md`. **Reescrito em 23/10/2026**, depois da 6ª rodada do diagnóstico e do pedido do usuário
("vamos continuar com o piloto"). **Nada aqui está aprovado.**

> **Histórico:** a 1ª versão deste plano era do sinal da CUBAGEM (densidade acima da típica do NCM com cubagem em m³
> inteiro). Foi suspensa pela revisão do usuário (nos casos de Santos o peso confere com a pesagem e a imagem; a
> anomalia era a cubagem) e derrubada pela 6ª rodada: com o típico por tamanho do contêiner, as positivas caíram de
> 32 para 10 e as RVF de 151 para 73, metade delas bagagem (NCM 9797, RVF de rotina); sem a bagagem, densidade acima
> com peso normal não separa (0,9×). O que sobrou é o PESO — este plano. Detalhes em `DIAGNOSTICO.md`.

## 1. O que se quer

Como analista de risco na importação, ver no card do NAVI-I, **antes da chegada**, quando o contêiner é **muito mais
pesado do que a mercadoria declarada costuma pesar** naquele tamanho de contêiner — e a cubagem declarada não explica
o peso —, com o motivo em números, e poder filtrar por isso. Leitura: mais mercadoria do que a declarada, ou outra
mercadoria (calçados com 19 t num 40', quando o típico é de 4 a 9 t). O peso declarado é real — confere com a pesagem
(revisão do usuário); o que não confere é o peso com a mercadoria.

É um **piloto "em observação"**: não entra na pontuação do risco histórico, não bloqueia nada, não muda telas do
Fichas. Serve para medir se o sinal ajuda o analista (Fichas abertas pelo card e o resultado delas), já que as Fichas
antigas são evidência fraca (usuário, 23/10/2026).

## 2. O sinal (`regras_declaracao.py`; "18a+ e 18b+" no diagnóstico)

Num **contêiner cheio (FCL)**, na família de CEs (peso e cubagem do master; sem ele, a soma dos filhotes), com **um
NCM só** (capítulos 01 a 97; **sem a bagagem, NCM 9797**):

1. **peso comparável**: de 100 kg até 110% da carga máxima do tipo (fora disso é digitação — o 16b);
2. **típico do PESO por contêiner** do NCM **no mesmo tamanho** (20', 40', 45'): mediana e MAD em escala log,
   aprendidos dos últimos 30 dias, no nível mais específico do NCM (8, 6 ou 4 dígitos) com 30+ contêineres (piso de
   dispersão de ~12%);
3. **típico da DENSIDADE** (peso ÷ cubagem declarada, com a cubagem até 110% da capacidade), do mesmo jeito;
4. **marca** quando, **no mesmo contêiner**, o peso está **3 ou mais desvios acima** do típico **e** a densidade
   também (a cubagem declarada não cresce junto com o peso: não é só um contêiner mais cheio da mesma mercadoria
   leve);
5. **fora máquinas e veículos** (capítulos 84 e 87; proposta, decisão 6): NCM de 4 dígitos heterogêneo — uma máquina
   pesada e uma leve na mesma posição.

Na **7ª rodada** (23/10/2026; 30 dias, 158 mil CEs avaliáveis, sem a bagagem — que era 1/3 das referências: as
fichas positivas avaliáveis caem de 146 para 91 e as RVF de 1.840 para 1.260):

| | marcados na população | fichas positivas | RVF | contrafação | contrabando | falsa declaração |
|---|---|---|---|---|---|---|
| peso e densidade acima | 959 (0,61%) | 6 de 91 (11,4× o controle) | 20 de 1.260 (2,9×) | 3 de 37 | 4 de 35 | 2 de 16 |
| ... só os capítulos 84 e 87 | 571 | 0 | 1 (abaixo do controle) | 0 | 0 | 0 |
| **... sem 84 e 87 (o sinal proposto)** | **388 (0,25%; ~13/dia)** | **6** | **19 (~5× o controle)** | **3** | **4** | **2** |

**Poucos casos** — nas infrações esperadas (eletrônicos, 85; calçados, 64; vidro, 70; escovas e artigos do 96), mas
o piloto existe para medir. Os acertos vêm de importadores diferentes (fichas: 3; RVF: 12), mas os marcados
concentram em alguns (um importador com 82 CEs, 52% dos dele: embarques inteiros). Peso acima sem densidade acima não
acerta quase nada; densidade acima sem peso acima é o jeito de declarar a cubagem. Na pesagem, os marcados divergem
mais da balança (1,9% dos "divergência" contra 0,3% dos "coerente"): parte pode ser peso declarado errado — o card
já mostra a pesagem quando há.

## 3. P0 — 7ª rodada e revisão do usuário (antes de qualquer coisa visível)

**7ª rodada feita (23/10/2026):** a força se manteve sem a bagagem (seção 2). Falta a **revisão** da `revisao7.csv`
(um CE por posição do NCM, os com pesagem primeiro): o usuário confere o peso na balança e olha a imagem — **a carga
parece a mercadoria declarada?** (avaliação: suspeito / explicável / erro de dado / sem como avaliar). As 9 linhas de
84 e 87 podem ser puladas. Duas linhas (8703 e 6910) misturam contêineres do mesmo CE — defeito da 7ª versão (o
candidato era por CE), corrigido na 8ª: vale o contêiner e o motivo do 18a+.

A **8ª versão** do diagnóstico (candidato por contêiner; validação também sem 84 e 87) dá os números exatos da regra
final e a linha de base para conferir a carga do P1 — rodar quando der, não bloqueia:

    virasana-venv/Scripts/python.exe virasana/analytic_scripts/declaracao/diagnostico_declaracao.py --saida declaracao8.log --csv declaracao8.csv --revisao revisao8.csv

Segue para o P3 (visível) se a revisão achar uma boa parte "suspeito". Se disser "explicável" na maioria, fecha 16–18.

## 4. Arquitetura (o caminho do risco histórico, que já está em produção)

Módulo novo **`virasana/declaracao/`** (o serviço do ciclo só chama), só aditivo:

- **Regras**: `regras_declaracao.py` passa para `virasana/declaracao/regras.py` (o diagnóstico importa de lá: uma
  regra só para o diagnóstico e a produção). Só as partes do sinal escolhido entram no caminho de produção.
- **`ddl_declaracao.sql`** (o DBA executa) e **`tabelas.py`** (Core; teste confere que batem):
  - `declaracao_conteiner`: **uma linha por (CE, contêiner)** de contêiner FCL de um NCM só — a família repete a
    medida em cada CE dela, para o NAVI-I achar pela chave do card. Colunas: `numeroCEmercante`, `codigoConteiner`
    (chave única), `familia`, `ncm`, `tipo` (ISO), `tamanho`, `peso`, `cubagem`, `densidade`, `ce_id` (ID no
    Mercante), `ce_alterado_em`, `criado_em`; e, da marcação, `chave_peso`, `desvios_peso`, `chave_densidade`,
    `desvios_densidade`, `marcado`, `versao_id`, `calculado_em`. Índices: a chave, `criado_em`, `versao_id`,
    `(marcado, criado_em)`. **A tabela guarda as medidas, não a regra**: se a regra mudar (outro limiar, capítulos
    fora), só se refaz a marcação.
  - `declaracao_versao`: um conjunto de típicos (janela, constantes, contagens) — e `declaracao_tipico`: por versão,
    medida (peso ou densidade) e chave ('tamanho|NCM'), `n`, mediana, MAD, p10, p90 (a memória de cálculo do motivo).
  - `declaracao_marca`: marcas d'água próprias.
- **Lotes** (`lotes.py`, como o do risco: `--carga`, `--tipicos`, `--marca`, `--etapa`, `--situacao`, `--simula`):
  - **extração**: CEs novos pela marca no ID do `conhecimentosresumo` e retificados pela `create_date` (a última
    alteração, indexada); para cada CE, a família completa (master e filhotes) e a linha de **todos** os CEs dela
    (filhote que chega depois refaz a família). Carga inicial: os últimos 30 dias (~3 min na estação, 6ª rodada).
  - **típicos**: versão nova 1x/dia, das linhas da própria tabela (`criado_em` nos últimos 30 dias) — sem reler o
    Mercante; segundos.
  - **marcação**: as linhas da fila (`versao_id` vazia ou antiga), em lotes, com os típicos da versão atual.
- **Etapa no serviço do ciclo** (`etapa.py`, `EtapaDeclaracao`, como a `EtapaRisco`): no fim de cada volta —
  extração, típicos (no máximo 1x/dia), marcação; transações próprias; limite de 60 s; qualquer erro só vai ao log;
  `--sem-declaracao` desliga; `--simula` não roda.
- **Health check**: verificação nova (marca d'água em dia, fila da marcação, idade da versão dos típicos, taxa de
  marcados do dia fora da faixa esperada de 0,1% a 0,6%).

## 5. NAVI-I (`virasana/navii/declaracao_navii.py`, como o `risco_navii.py`)

O card já mostra o peso e a cubagem declarados (item 23 do `navii/CONTEXTO.md`, falta publicar).

- **Selo no card** (só quando marcado): "**Carga pesada para a mercadoria declarada — em observação**" (decisão 1).
- **Motivo** (recolhível, como a memória do risco): "19.220 kg num 40'; o NCM 6402 (calçados) num 40' pesa de 4.064 a
  8.918 kg (230 contêineres, 30 dias) — 3,5 desvios acima. Densidade 283 kg/m³ (típica: 69 a 136): a cubagem
  declarada não explica o peso." E, se houver, "outros N contêineres desta família também marcados" (os marcados vêm
  em embarques inteiros).
- **Filtro** "Carga pesada para a mercadoria (em observação)", verificado por candidato (`_existe` na chave única de
  `declaracao_conteiner` com `marcado`), como o filtro de risco; filtros salvos, chips e coleta de uso tratam o novo
  parâmetro. Medir com `navii/diagnostico_sql.py --analyze` (cenário novo).
- **Abrir Ficha**: com o selo, o motivo entra no resumo do risco (observações) e na memória interna da RVF
  (`ficha_navii.py`) — o analista leva o porquê, e as Fichas com o sinal ficam identificáveis depois.
- Sem as tabelas, card sem o selo e filtro com erro claro (como o risco). Testes: unidade, SQLite, prefixo `/virasana`
  (`test_prefixo_proxy.py`) e roteiro no navegador.

## 6. Medição do piloto

A coleta de uso já grava, por pesquisa, os cards enviados, vistos e examinados e as Fichas abertas pelo card. Um
script de acompanhamento (`acompanha_piloto.py`, só leitura) cruza com `declaracao_conteiner`:

- marcados por dia; cards marcados enviados, vistos e examinados; uso do filtro;
- **Fichas abertas por card visto, com e sem o selo** (o efeito do sinal na escolha do analista);
- depois, o **resultado** dessas Fichas (RVF, infrações, TG, perdimento), comparado com as Fichas abertas pelo card sem
  o selo — o retorno chega tarde (semanas a meses);
- **pesagem × declarado** dos marcados que passam pela balança (o peso declarado confere? se sim, a carga é mesmo
  pesada para a mercadoria);
- concentração por importador e capítulo, e sobreposição com o risco histórico alto/médio.

## 7. Fases

| Fase | Entrega | Grava? | Visível? |
|---|---|---|---|
| P0 | 7ª rodada (`declaracao7.log`, `revisao7.csv`) e revisão dos ~30 CEs pelo usuário; decisões da seção 9 | não | não |
| P1 | Módulo, DDL, extração, típicos, marcação, `lotes.py`; carga dos 30 dias à mão; conferir ~0,25% marcados e os mesmos CEs do diagnóstico (8ª versão) | sim (tabelas novas) | não |
| P2 | Etapa no serviço do ciclo + verificação no health check | sim | não |
| P3 | NAVI-I: selo, motivo, filtro, Abrir Ficha com o motivo | não (tela) | **sim** |
| P4 | Acompanhamento (script); relatório mensal; decisão ao fim do piloto | não | não |

P1 e P2 não aparecem para ninguém e guardam as medidas, não a regra — podem andar enquanto a revisão (P0) acontece
(decisão 8). **P3 só depois da revisão.**

## 8. Riscos e cuidados

- **Produção**: módulo e tabelas novos, etapa isolada (erro só no log, flag para desligar); no NAVI-I, um selo e um
  filtro a mais, no padrão do risco. Nada no Fichas legado.
- **Máquinas e veículos** (84, 87): 571 marcados e 1 acerto na 7ª rodada — fora do sinal (decisão 6). Outras
  posições heterogêneas podem aparecer no piloto: o acompanhamento mede marcados e acertos por capítulo.
- **Embarques inteiros do mesmo importador**: dezenas de cards marcados de uma vez — o card mostra quantos da família
  estão marcados; medir a sobreposição com o risco histórico do importador.
- **Típico em 4 dígitos** (97% dos NCMs do Mercante): posições heterogêneas; o motivo mostra a faixa típica para o
  analista julgar. Refinamento possível: típico também pelo histórico do importador.
- **Deriva**: o health check vigia a taxa de marcados do dia.
- **Retificação**: CE retificado volta à fila (pela `create_date`), como no risco.

## 9. Decisões para o usuário (com a recomendação)

1. **Nome do selo**: "Carga pesada para a mercadoria declarada" (o que o analista entende de cara) ou "Peso acima do
   típico do NCM" (técnico)? *Recomendo o primeiro, com o técnico no motivo.*
2. **Quem vê no piloto**: todos os usuários do NAVI-I, ou um grupo no começo? *Recomendo todos, com o "em
   observação" — mais dados em menos tempo.*
3. **Fora da pontuação do risco histórico** durante o piloto. *Recomendo: sim; ao fim, decidir se vira dimensão.*
4. **Duração e critério**: *recomendo 60 dias; continuar se a taxa de Ficha por card visto com o selo for maior que sem
   o selo e os resultados que já chegaram não contrariarem.* O critério final é seu.
5. **Janela dos típicos**: 30 dias (a validada) ou 90 (mais estável)? *Recomendo 30, como foi validado.*
6. **Capítulos 84 e 87 fora do sinal**: na 7ª rodada, 571 dos 959 marcados e 1 acerto de RVF (abaixo do controle);
   sem eles, ficam todos os acertos de fichas, contrafação e contrabando. *Recomendo excluir* (a tabela guarda as
   medidas deles: se o piloto mostrar outra coisa, só se refaz a marcação).
7. **Mover as regras** para `virasana/declaracao/regras.py` (o diagnóstico passa a importar de lá). *Recomendo.*
8. **Quando começar P1 e P2** (tabelas novas no banco, pelo DBA; nada visível)? A condição combinada — a força se
   manter sem a bagagem — foi cumprida na 7ª rodada. *Recomendo começar com a aprovação do plano, sem esperar a
   revisão inteira: as tabelas guardam as medidas, não a regra. O P3 (visível) espera a revisão.*
