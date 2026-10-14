# Risco histórico por CE — contexto e plano

Ler antes de retomar. A tela **Risco histórico das Fichas** (`/risco_historico/`, lógica em
`analytic_scripts/dados_risco_perdimentos.py`) hoje só extrai e mostra os sinais das Fichas com resultado.
Este plano transforma esse histórico num **risco calculado por CE**, exibido e filtrável no NAVI-I.

## História de usuário (14/10/2026)

> Como usuário do NAVI-I, na análise de risco na importação, quero **filtrar e ordenar** os CEs por um risco
> matemático calculado pelo histórico de ocorrências do Fichas — Fichas de importação com resultado
> Perdimento, Crédito, Multa RFB ou Multa de outro órgão, e especialmente as com TG de alto valor —, a partir
> de dados como importador, embarcador e rota, vendo a **memória de cálculo** de cada CE.

Observações do usuário:
1. Os parâmetros e pesos podem ser pré-calculados em lote; entram ~10 fichas novas por mês (pouco perto do
   número de CEs), então o recálculo não precisa ser frequente.
2. O risco é por CE: começa num ponto (data) e avança incrementalmente.
3. Precisa ser explicável: exibir a memória de cálculo.
4. Calculado o risco de cada CE, ele aparece no NAVI-I e pode ser filtrado.
5. Os lotes (parâmetros e pontuação) rodam no laço do serviço do ciclo de vida; exibição e filtro na tela de
   análise de risco (NAVI-I).

### Critérios de aceite

1. Todo CE (exceto exportação) **criado a partir da data inicial** (1 mês antes da ativação) tem risco calculado: pontos, faixa e versão
   dos parâmetros (ou "sem sinal"). CEs anteriores aparecem como "não calculado".
2. O card do NAVI-I mostra a faixa e os pontos; aberto, mostra a **memória de cálculo**: cada sinal que contou
   (dimensão, valor, fichas com resultado, CEs na população, quantas vezes a média, peso), as fichas (links
   para o Fichas), a versão e a data do cálculo. **O total é a soma das parcelas exibidas.**
3. O NAVI-I filtra por faixa de risco e ordena por risco, sem piorar as pesquisas que não usam esse filtro
   (medir com `navii/diagnostico_sql.py`).
4. Os parâmetros são recalculados em lote, com versão guardada; os CEs são re-pontuados aos poucos.
5. Falha no cálculo do risco **não para o ciclo de vida**.
6. Os parâmetros em uso (a "regra") podem ser consultados na tela Risco histórico.

## O que já se sabe (diagnóstico de produção de 01/10/2026)

- **Ligação ficha → CE**: CE na ficha em 88%, na RVF em 86%; 90% dos CEs estão no `conhecimentosresumo`.
- **Volume**: ~14,8 mil fichas; 1.521 com Apreensão ou Perdimento. Perdimentos por ano: ~570 (2020), 153 (2021),
  184 (2022), 158 (2023), 113 (2024), 103 (2025), 7 (2026, o resultado chega tarde). Crédito: 26; Multa RFB:
  15; Multa outro órgão: 9.
- **Apreensão fica de fora**: 224 das 226 fichas com apreensão são de cocaína — o perfil é logístico
  (contaminação do contêiner), não do importador.
- **Abandono**: `dados_risco_perdimentos` não olha `tipooperacao`; perdimento de FMA (tipo 0) se mistura aos
  demais. 430 fichas do universo passaram por "Aguardando comunicado de abandono – IN2160/23".
- **Concentração**: importador (raiz) 602 valores, 76 em 2+ fichas; embarcador 1.024 / 104; rota 195 / 100;
  posição NCM 535 / 323. Rota e NCM se repetem por natureza: sem denominador, frequência não é risco.
- **Índices do Mercante** (conferidos no F0, o banco tem mais que o ORM): `conhecimentosresumo` tem índice em
  `consignatario`, `embarcador` (prefixo 767), `dataEmissao`, `create_date`, portos, `tipoTrafego` e
  (`create_date`, origem, destino); `ncmitemresumo` em `numeroCEMercante`, `create_date` e `identificacaoNCM`.
  Tudo latin1.
- **Carga do Mercante** (`resume_mercante.py`): inclusão = linha nova (ID crescente); retificação = UPDATE na
  mesma linha (sem ID novo); NCMs em tabela própria, que pode chegar depois do CE.
- **`conhecimentosresumo.create_date` NÃO é a data de criação**: a coluna é `ON UPDATE current_timestamp()`
  (padrão do MariaDB para o 1º TIMESTAMP), então é a **última alteração**. `dataEmissao` é a emissão do BL na
  origem (semanas antes da chegada). Para "a partir de quando", usar o **ID**.

## Modelo (explicável por construção)

**Fichas positivas** (`fichas_positivas.py`): Fichas de importação — sem FMA (tipo 0) nem exportação (tipos 2 e
4), e com algum CE que não seja de exportação — com resultado Perdimento (2), Crédito (3), Multa RFB (6) ou
Multa outro órgão (7), **ou** com TG de alto valor (soma de `ovr_tgovr.valor` da ficha ≥ R$ 100 mil). Peso da
ficha: 1; com TG a partir de R$ 1 mi, 2.

**Sinais** — só o que todo CE novo tem antes da chegada (Mercante):

| Dimensão | Valor |
|---|---|
| Importador | raiz do CNPJ do consignatário |
| Embarcador | nome normalizado (`normaliza_embarcador`) |
| Rota | porto de origem > destino final |
| Baldeação | porto de carregamento do manifesto ≠ origem |
| País + NCM | país de origem + posição NCM declarada |
| NCM | posição NCM declarada |
| CNPJ citado | raiz de CNPJ citado nos textos de ficha positiva (sem o importador da própria ficha) = consignatário do CE. Vale com **uma** ficha só e tem teto maior: deve ser dos que mais chamam atenção |

Marca, infração, contêiner e NCM encontrada continuam só na tela de consulta (não existem num CE novo).

**Peso de cada valor** (dimensão *d*, valor *v*):
- *k* = soma dos pesos das fichas positivas com *v*; *K* = soma de todas.
- *b* = fatia dos CEs da população que têm *v* (*n*/*N*). A população são os CEs (sem exportação) com sinais
  gravados em `risco_historico_ce` nos últimos 90 dias — começa com o mês da carga inicial (~320 mil CEs no F0,
  suficiente) — sem reler o Mercante (no F0, a leitura das NCMs de um mês levou 110 s).
- fatia suavizada das positivas: *r* = (*k* + *m*·*b*) / (*K* + *m*) — com poucas fichas, puxa para a média.
- **vezes a média** *L* = *r* / *b*; **peso** = ln(*L*), se *k* ≥ mínimo de fichas e *L* > 1; senão 0. Teto por
  dimensão.

**Pontos do CE** = soma, por **grupo**, do maior peso entre os valores do CE: identidade (importador, embarcador,
CNPJ citado) conta cada uma; **mercadoria** = NCM ou país+NCM (o maior); **logística** = rota ou baldeação (o
maior). Regras: identidade com mínimo de 1 ficha e teto 5 (CNPJ citado: 8); contexto com mínimo de 3 fichas de 2+
importadores diferentes e teto 3. **Faixa** por percentil dos pontos na população recente (ex.: Alto = 1% maiores; Médio = até 5%).
Constantes (*m*, mínimo de fichas, teto, peso do TG, recência, faixas) calibradas no F2.

Exemplo de memória de cálculo (números ilustrativos):

```
Risco histórico: ALTO — 6,1 pontos (parâmetros v3, de 01/11/2026)
  Importador 12345678 (EMPRESA X)  3 fichas positivas; 0,02% dos CEs   ×40 → 3,7
  Embarcador NINGBO TOYS           2 fichas positivas; 0,05% dos CEs   ×11 → 2,4
  Rota CNNGB > BRSSZ              149 fichas; 18% dos CEs              ×1,1 → 0 (abaixo do mínimo)
```

## Arquitetura

Módulo próprio em `virasana/risco_historico/` (o serviço do ciclo, no F3, só chama):

- **`ddl_risco_historico.sql`** (o usuário/DBA executa) e **`tabelas.py`** (Core; teste confere que batem):
  - `risco_historico_ce`: um CE (não exportação) desde a marca inicial — `sinais` (JSON por dimensão, extraídos
    uma vez do Mercante), `ce_id`, `ce_alterado_em` (create_date lido), `ncm_id` (maior NCM usada), `criado_em`;
    no F3, `pontos`, `faixa`, `versao_id`, `calculado_em`. Índices em ce_id, criado_em, versao_id, (faixa, pontos).
  - `risco_historico_versao`: motivo, assinatura das fichas positivas, *K*, *N*, janela da população, constantes.
  - `risco_historico_parametro`: por versão, dimensão e valor — descrição, fichas, importadores, *k*, *n*,
    vezes a média, peso, motivo do zero, ids das fichas. Todos os valores do histórico (a memória de cálculo).
  - `risco_historico_marca`: marcas d'água próprias (não reaproveita `ciclodevida_marca_dagua`).
- **Lote 2a — `extracao.py`** (sinais): CEs novos pela marca no ID; alterados pela `create_date` (= última
  alteração), em páginas (create_date, ID), re-extraindo só se mudou desde `ce_alterado_em`; NCMs novas pela marca
  no ID do `ncmitemresumo`, re-extraindo só se a NCM é mais nova que `ncm_id`. Nada depende do relógio desta
  máquina. Marca inicial: menor ID dos CEs com `dataEmissao` nos últimos 30 dias (recusa faixa > 30 mil IDs/dia).
- **Lote 1 — `parametros.py`**: fichas positivas → `calculo.historico` → população da própria `risco_historico_ce`
  (`criado_em` nos últimos 90 dias) → pesos → nova versão. Só quando muda a assinatura das fichas positivas ou a
  última versão passa de 30 dias (ou `--forca`). Leva segundos: cabe no laço do ciclo (F3).
- **`lotes.py`**: linha de comando dos lotes (`--sinais`, `--parametros`, `--situacao`, `--simula`).
- **Lote 2b — `pontuacao.py` (F3)**: sinais × parâmetros da versão atual, em Python; versão nova re-pontua os
  CEs em lotes, **sem reler o Mercante**.
- **No serviço do ciclo (F3)**: uma etapa no fim de cada volta, em transação própria e `try/except` (erro só vai
  ao log), com limite de tempo e `--sem-risco` para desligar.
- **NAVI-I (F4)** (`busca_navii.py`, `navii.html`):
  - card: selo (faixa e pontos) e memória de cálculo recolhível, montada de `sinais` × `parametro` da versão
    do CE (busca em lote para os 20 cards da página; `parametros.pesos_da_versao`).
  - filtro "Risco histórico" (alto / médio ou mais / com sinal): a tabela de risco dirige a busca (índice em
    faixa), e a janela vira condição por candidato, como no filtro de contêiner.
  - ordenação por risco: cursor (pontos, Item.ID); medir antes de liberar sem filtro.
- **Tela Risco histórico**: página "Parâmetros do risco por CE" (`/risco_historico/parametros`, abas entre as
  duas páginas): versão, regras, pesos por grupo, fichas de cada valor; `?todos=1` mostra também os que não
  contam, com o motivo.

## Fases

| Fase | Entrega | Grava? |
|---|---|---|
| F0 | Diagnóstico (feito em 14/10/2026; resultado abaixo) | não |
| F1 | Tabelas (DDL) + extração incremental dos sinais + lote de parâmetros (versões) + linha de comando + página "Parâmetros do risco por CE" | sim (tabelas novas) |
| F2 | Validação no tempo: parâmetros das fichas até T, aplicados aos CEs de T a T+12 meses — quantas fichas positivas futuras cairiam em Alto/Médio e quantos CEs seriam marcados. Calibra as constantes. | não |
| F3 | Pontuação (pontos, faixa) e re-pontuação por versão; extração, parâmetros e pontuação como etapa do serviço do ciclo | sim |
| F4 | NAVI-I: selo e memória de cálculo no card; filtro por faixa; ordenação (se medida ok) | não (tela) |
| F5 (opcional) | Analista desativa um parâmetro; exportação `campo;valor;motivo` para a tela Risco do Fichas (`/importacsv` → `riscosativos`) | sim |

## Decisões (14/10/2026, plano aprovado)

1. FMA (tipo 0) **não** conta como positiva (abandono é outro risco).
2. TG de alto valor: **R$ 100 mil** (soma dos TGs da ficha).
3. Data inicial da pontuação: **1 mês** antes da ativação (não é preciso pontuar CEs muito antigos).
4. CNPJ citado em ficha positiva **é sinal** e deve pesar muito; CPF fica fora dos parâmetros (LGPD).
5. Ajustes depois do F0 (aprovados no mesmo dia): ver "Ajustes aprovados" abaixo.

## Andamento

**F0 (14/10/2026) — escrito, falta rodar em produção.** O diagnóstico já usa o código do cálculo:
- `risco_historico/fichas_positivas.py` — a definição das fichas positivas (constantes + `busca`).
- `risco_historico/sinais.py` — sinais de um CE (`sinais_do_ce`, os mesmos valores de `dados_risco_perdimentos`;
  teste garante) e da população (`sinais_do_periodo`: CEs por `create_date`, manifestos em lote, NCMs pela
  `create_date` delas — sem junção pelo CE —, `conta`).
- `risco_historico/calculo.py` — `historico` (K, k, fichas), `peso` (memória de cálculo `Peso`),
  `tabela_de_pesos`, `pontua`. Constantes provisórias: *m* = 10, mínimo 2 fichas (CNPJ citado: 1), teto 5
  (CNPJ citado: 8).
- `dados_risco_perdimentos.extrai(fichas=...)` aceita as fichas já buscadas.
- `analytic_scripts/diagnostico_risco_ce.py` — seções 1 a 6 (estrutura, tipo de operação × resultado, valor dos
  TGs, fichas positivas, população com prévia de pesos e pontos, incremento). Identidades nunca saem no log.
- Testes: `tests/test_risco_historico_ce.py` (em `qualidade_navii.py`).

### Resultado do F0 (log de produção de 14/10/2026)

- **FMA é a maior parte dos perdimentos**: 928 fichas (tipo 0), contra 322 da repressão na importação (tipo 1,
  R$ 1,04 bi). Multa RFB só aparece em exportação e vigilância.
- **Fichas positivas**: 489 (só resultado 73, só TG alto 123, ambos 293), K = 905; 440 com CE no Mercante.
  2019–2020 entram só pelo TG (sem resultado registrado).
- **TG**: das 486 fichas de importação (sem FMA) com TG, 416 (85%) passam de R$ 100 mil — quase toda positiva
  pesa 2, e o "especialmente" se perde. Acima de R$ 1 mi: 241.
- **População (30 dias)**: 317.708 CEs (sem exportação). Tempo: CEs 21 s, manifestos 2 s, **NCMs 110 s**,
  Python 5 s — ~28 min para 365 dias. Do histórico aparecem 84 importadores, 47 embarcadores e 6 CNPJs citados.
- **Prévia (constantes provisórias)**: 113 mil CEs (36%) com algum sinal; p90 = 2,9, p99 = 6,0, máx. 16,7. O
  contexto domina: NCM conta em 83 mil CEs, país+NCM em 61 mil, rota em 27 mil, baldeação em 14 mil; importador
  em 2.010, embarcador em 492, CNPJ citado em 397. NCM e país+NCM somam a mesma informação. Rota rara com 2
  fichas (provavelmente a mesma operação) chega ao teto. Os padrões de contexto fazem sentido (HK 8482
  rolamentos, US 9797 objetos de arte, lingerie, consoles, relógios, papel para reciclar).
- **Incremento**: ~13 mil CEs novos por dia (IDs; ~27% exportação), ~10% de alterações de CEs antigos. NCMs
  chegam depois do CE em parte (25 mil entre 1 e 7 dias, 4 mil entre 7 e 30); 25,6 mil CEs sem NCM. A consulta
  agrupada por dia em `create_date` levou 320 s (varredura): o lote 2 não deve agregar o Mercante, só ler por
  faixa de ID.

**Ajustes aprovados para o F1** (14/10/2026):
1. Início por ID: marca inicial = menor ID dos CEs com `dataEmissao` ≥ ativação − 30 dias (índice).
   Retificações de graça: CEs com `create_date` ≥ última volta (é a data da última alteração, indexada). NCMs pela
   marca em `ncmitemresumo.ID`.
2. População tirada da própria tabela de sinais (CEs pontuados; começa com o 1º mês, depois janela móvel de 90
   dias) — o lote 1 cai para segundos e cabe no laço do ciclo; a leitura pesada do Mercante (NCMs) fica só na
   carga inicial do lote 2.
3. Modelo: dimensões correlatas agrupadas (mercadoria = maior entre NCM e país+NCM; logística = maior entre rota
   e baldeação); contexto com mínimo de 3 fichas de 2+ importadores diferentes e teto 3; identidade com mínimo
   de 1 ficha e teto 5 (CNPJ citado: 8).
4. TG: R$ 100 mil continua como critério de entrada; peso 2 só a partir de R$ 1 mi.

**F1 (14/10/2026) — escrito e testado, falta ativar.** Ver "Arquitetura". Também: `calculo.py` com grupos e
regras por dimensão (`Regra`, `REGRAS`, `GRUPOS`; `Peso.motivo`), `fichas_positivas.TG_PESO_DOBRADO`,
`sinais.sinais_dos_ces`/`para_json`/`de_json`. Testes: `tests/test_risco_historico_lotes.py` e a página em
`test_risco_historico_tela.py`/`test_prefixo_proxy.py`. Ativação: executar a DDL; `lotes.py --sinais --simula`
(mede a marca inicial e uma volta); `lotes.py --sinais` (carga do mês); `lotes.py --parametros`; conferir a
página. Depois, F2.
