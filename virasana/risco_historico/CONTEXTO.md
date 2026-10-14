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
- **Índices do Mercante**: `consignatario`, `portoOrigemCarga`, `portoDestFinal`, `create_date` e
  `tipoTrafego` têm índice; `embarcador` (VARCHAR 800) e `dataEmissao` (VARCHAR 'AAAA-MM-DD') não. No
  `ncmitemresumo`, o ORM não indexa `numeroCEMercante` (a consulta por CE levou 20 s) — conferir no F0.
- **Carga do Mercante** (`resume_mercante.py`): inclusão = linha nova (ID crescente); retificação = UPDATE na
  mesma linha (sem ID novo); NCMs em tabela própria, que pode chegar depois do CE.

## Modelo (explicável por construção)

**Fichas positivas** (`fichas_positivas.py`): Fichas de importação — sem FMA (tipo 0) nem exportação (tipos 2 e
4), e com algum CE que não seja de exportação — com resultado Perdimento (2), Crédito (3), Multa RFB (6) ou
Multa outro órgão (7), **ou** com TG de alto valor (soma de `ovr_tgovr.valor` da ficha ≥ R$ 100 mil). Peso da
ficha: 1; com TG de alto valor, 2 (ajustável no F2).

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
- *b* = fatia dos CEs da população que têm *v* (*n*/*N*). A população vem do Mercante (CEs criados numa janela
  recente, sem exportação), independente dos CEs pontuados — a pontuação começa só 1 mês para trás, pouco para
  servir de denominador. Tamanho da janela: decidir com o tempo medido no F0.
- fatia suavizada das positivas: *r* = (*k* + *m*·*b*) / (*K* + *m*) — com poucas fichas, puxa para a média.
- **vezes a média** *L* = *r* / *b*; **peso** = ln(*L*), se *k* ≥ mínimo de fichas e *L* > 1; senão 0. Teto por
  dimensão.

**Pontos do CE** = soma, por dimensão, do maior peso entre os valores do CE (um CE com 5 NCMs conta só o NCM
mais pesado). **Faixa** por percentil dos pontos na população recente (ex.: Alto = 1% maiores; Médio = até 5%).
Constantes (*m*, mínimo de fichas, teto, peso do TG, recência, faixas) calibradas no F2.

Exemplo de memória de cálculo (números ilustrativos):

```
Risco histórico: ALTO — 6,1 pontos (parâmetros v3, de 01/11/2026)
  Importador 12345678 (EMPRESA X)  3 fichas positivas; 0,02% dos CEs   ×40 → 3,7
  Embarcador NINGBO TOYS           2 fichas positivas; 0,05% dos CEs   ×11 → 2,4
  Rota CNNGB > BRSSZ              149 fichas; 18% dos CEs              ×1,1 → 0 (abaixo do mínimo)
```

## Arquitetura

Módulo próprio em `virasana/risco_historico/` (o serviço do ciclo só chama):

- **`modelos.py`** — tabelas novas:
  - `risco_historico_versao`: id, criado_em, constantes, *K*, *N*, fichas usadas.
  - `risco_historico_parametro`: versao_id, dimensão, valor, descrição, *k*, fichas (ids), *n*, *L*, peso, ativo.
  - `risco_historico_ce`: numeroCEmercante (único), dataEmissao, **sinais** (JSON por dimensão, extraídos uma
    vez do Mercante), pontos, faixa, versao_id, sinais_em, calculado_em. Índices em (faixa, pontos) e versao_id.
  - marcas d'água próprias (não reaproveitar `ciclodevida_marca_dagua`).
- **Lote 1 — `parametros.py`**: fichas positivas → sinais dos seus CEs (reaproveita `dados_risco_perdimentos`)
  → população (*n*, *N*) → pesos → nova versão. Roda quando há ficha positiva nova (marca em
  `ovr_resultados`/`ovr_tgovr`) e passou o intervalo mínimo (ex.: 30 dias), ou por linha de comando.
- **Lote 2 — `pontuacao.py`**:
  - **sinais**: CEs novos pela marca d'água em `conhecimentosresumo.ID` (lotes de ~5 mil); NCMs que chegam
    depois pela marca em `ncmitemresumo.ID`; revisita dos CEs dos últimos 30 dias para pegar retificações.
  - **pontos**: sinais × parâmetros da versão atual, em Python (parâmetros cabem em memória). Versão nova
    re-pontua os CEs em lotes, **sem reler o Mercante**.
  - **início**: `--desde AAAA-MM-DD` acha o primeiro ID por `create_date` (indexado); depois só incrementa.
- **No serviço do ciclo**: uma etapa no fim de cada volta, em transação própria e `try/except` (erro só vai ao
  log), com limite de tempo e `--sem-risco` para desligar. O lote 1 só roda dentro do laço se o F0 mostrar
  que é rápido; senão, linha de comando mensal (cron).
- **NAVI-I** (`busca_navii.py`, `navii.html`):
  - card: selo (faixa e pontos) e memória de cálculo recolhível, montada de `sinais` × `parametro` da versão
    do CE (busca em lote para os 20 cards da página).
  - filtro "Risco histórico" (alto / médio ou mais / com sinal): a tabela de risco dirige a busca (índice em
    faixa), e a janela vira condição por candidato, como no filtro de contêiner.
  - ordenação por risco: cursor (pontos, Item.ID); medir antes de liberar sem filtro.
- **Tela Risco histórico**: aba "Parâmetros em uso" (versão, pesos, fichas de cada valor).

## Fases

| Fase | Entrega | Grava? |
|---|---|---|
| F0 | Diagnóstico: `tipooperacao` × resultado; distribuição do valor dos TGs (limiar); fichas positivas pela definição acima e quantas têm sinais; tempo das contagens de população (consignatário, portos, NCM, embarcador); índices de `ncmitemresumo`/`itensresumo`; CEs por dia; parcela das FMAs | não |
| F1 | Lote 1 (parâmetros) + tabelas + aba "Parâmetros em uso" | sim (tabelas novas) |
| F2 | Validação no tempo: parâmetros das fichas até T, aplicados aos CEs de T a T+12 meses — quantas fichas positivas futuras cairiam em Alto/Médio e quantos CEs seriam marcados. Calibra as constantes. | não |
| F3 | Lote 2 (sinais + pontos) com marcas d'água, revisita e re-pontuação; etapa no serviço do ciclo | sim |
| F4 | NAVI-I: selo e memória de cálculo no card; filtro por faixa; ordenação (se medida ok) | não (tela) |
| F5 (opcional) | Analista desativa um parâmetro; exportação `campo;valor;motivo` para a tela Risco do Fichas (`/importacsv` → `riscosativos`) | sim |

## Decisões (14/10/2026, plano aprovado)

1. FMA (tipo 0) **não** conta como positiva (abandono é outro risco).
2. TG de alto valor: **R$ 100 mil** (soma dos TGs da ficha).
3. Data inicial da pontuação: **1 mês** antes da ativação (não é preciso pontuar CEs muito antigos).
4. CNPJ citado em ficha positiva **é sinal** e deve pesar muito; CPF fica fora dos parâmetros (LGPD).

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
