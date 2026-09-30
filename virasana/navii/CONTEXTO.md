# NAVI-I — contexto e checkpoint de desenvolvimento

> Checkpoint de **30/09/2026**. Leia este arquivo antes de retomar o desenvolvimento do NAVI-I
> em uma nova sessão. Ele registra o que não dá para deduzir só lendo o código: decisões do
> usuário, o que foi medido no banco real, o que ficou pendente e por quê.
> Ao concluir um incremento, atualize a seção "Estado atual" e "Pendências".

## O que é

NAVI-I é a tela de **análise de risco na importação** para analistas da Vigilância Aduaneira
(Receita Federal, Porto de Santos). Vive dentro do repo `virasana` (não é repo separado):

| Arquivo | Papel |
|---|---|
| `virasana/navii/busca_navii.py` | Camada de dados: monta a query, conta, enriquece e monta os cards |
| `virasana/navii/views_navii.py` | Blueprint `bp_navii` (`/navii/` e `/navii/api/ces`) |
| `virasana/templates/navii.html` | Tela (design system gov.br, JS puro, rolagem infinita) |
| `virasana/navii/diagnostico_sql.py` | Script: SHOW CREATE TABLE + EXPLAIN/ANALYZE das buscas da tela |
| `virasana/tests/test_navii.py` | Testes unitários (sessão fake) |
| `virasana/tests/test_navii_busca_sqlite.py` | Testes de integração: queries reais em SQLite, cenário simulado |

Rodar os testes (a partir da raiz do `virasana`, venv `virasana-venv`, Python 3.9):

```bash
python -m pytest virasana/tests/test_navii.py virasana/tests/test_navii_busca_sqlite.py --disable-warnings
```

Os demais arquivos de `virasana/tests` falham neste ambiente por motivos alheios ao NAVI-I
(`ajna_commons` fora do path na coleta, Mongo). Não use a suíte inteira como critério.

## Modelo de dados (o que cada tabela representa)

```
escalasresumo (Escala)            atracação de um navio em UM porto (portoDaEscala); datas em VARCHAR 'YYYY-MM-DD'
   └─ escalamanifestoresumo       N:N — um manifesto tem ~3,8 escalas (uma por porto onde o navio atraca)
manifestosresumo (Manifesto)      viagem: portoCarregamento, portoDescarregamento, codigoTerminalDescarregamento
   └─ conhecimentosresumo (CE)    Conhecimento.manifestoCE -> Manifesto.numero; tipoTrafego; embarcador/consignatario
        └─ itensresumo (Item)     contêiner declarado no CE (numeroCEmercante + codigoConteiner)
             └─ ncmitemresumo     NCMs declarados por contêiner
ciclodevida_ciclo_de_vida_importacao   "Ciclo de vida": eventos da API Recintos por (CE, contêiner):
                                       desembarque, saída, entrada/saída em recinto seco, concluido
   └─ ..._escaneamentos                escaneamentos do ciclo, com imagem_id (GridFS)
ovr_recintos (bhadrasana2)        cadastro de recintos: nome, cod_siscomex, cod_carga, cod_unidade
laudo_empresas / laudo_ncm        (bhadrasana2) CNPJ -> nome; NCM -> descrição oficial
Enumerado                         tabela-dicionário compartilhada; ver códigos abaixo
```

Códigos da tabela `Enumerado` (coluna `ID`):
- `tipoTrafego`: 1 Interior, 3 Cabotagem, **5 lci (importação)**, **7 lce (exportação)**, 9 Passagem, A BCN, B bce
- `tipoItemCarga`: 1 Contêiner, 2 Carga Solta, 3 Granel, 4 Veículos
- `tipoBLConhecimento`: 10 BL, 11 MBL, 12 HBL, 15 MHBL

`mercantealchemy.TIPOS_TRAFEGO` não tem os códigos 1, A e B (aparecem como "--" no card). É model
compartilhado com o bhadrasana2 — não alterar sem combinar.

Banco real (medido em 30/09/2026, produção `dbmercante`): **MariaDB 10.11**, latin1.
`ncmitemresumo` ~152 M linhas, `itensresumo` ~51 M, `conhecimentosresumo` ~22 M,
`escalamanifestoresumo` ~3,8 M, `manifestosresumo` ~1 M, ciclo de vida ~284 mil, escalas ~254 mil.
Qualquer busca que não parta de um índice seletivo é inviável.

## O que as amostras reais mostraram (30/09/2026)

Amostras fornecidas pelo usuário: 1000 escalas e 1000 manifestos mais recentes, `ovr_recintos` e
`ovr_setores` completas (dados públicos).

**Confirmado**
- `Escala.portoDaEscala` é o código de 5 letras (`BRSSZ`, `BRPNG`, `BRMAO`...). O filtro padrão funciona.
- `dataPrevistaAtracacao` é sempre `YYYY-MM-DD`. `dataEfetivaPrimeiraAtracacao` vem **vazia (`''`,
  não NULL)** enquanto o navio não atraca (950 de 1000).
- `Manifesto.codigoTerminalDescarregamento` é `<porto><3 dígitos>` (ex.: `BRSSZ016`) e casa com
  `ovr_recintos.cod_carga`. Vem **vazio em ~52% dos manifestos** (exportação, passagem).
- Exportação é ~34% dos manifestos recentes (tráfego 7); passagem 14%; `B` (bce) 14%; cabotagem 5%;
  interior 5%; `A` (BCN) <1%. Manifestos com destino `BRSSZ`: tráfegos 5, B e 3.

**Descobertas que mudam o desenho**
- **`ovr_recintos` tem duas linhas por terminal portuário**, sem chave entre elas: a linha do
  *recinto* (com `cod_siscomex`, ex.: `70 SANTOS BRASIL ... 8931356`) e a linha do *operador*
  (nome "OP - ...", com `cod_carga`, ex.: `997 OP - SANTOS/BRASIL ... BRSSZ016`). Só 4 linhas têm
  `cod_carga`: BRSSZ009 (Embraport), BRSSZ016 (Santos Brasil), BRSSZ057 (Ecoporto), BRSSZ058 (BTP).
  Consequência: a ponte `cod_siscomex -> cod_carga` **na mesma linha não devolve nada** na base
  real. Hoje o filtro Recinto alcança contêineres sem ciclo só se o usuário digitar o código do
  terminal (`BRSSZ016`). O nome do terminal no card (busca por `cod_carga`) funciona.
  Pares prováveis (por nome, **a confirmar com o usuário**): 8931404↔BRSSZ009, 8931356↔BRSSZ016,
  8931318↔BRSSZ057, 8931359↔BRSSZ058.
- Há `cod_carga = ''` (vazio) em `ovr_recintos` — tratado no código (senão o recinto casaria com
  todo manifesto de terminal em branco).
- `Escala.embarcacao` é o **número IMO/inscrição** do navio (numérico em 94%), não o nome. O card
  mostra "Embarcação <número>". Não há nome de navio nas tabelas conhecidas.
- A API Recintos já cobre outras unidades (Pecém, Suape, Rio, Itaguaí têm `cod_siscomex`). Logo o
  ciclo de vida pode trazer desembarques de fora de Santos, e o ramo "desembarque efetivo" da
  janela **não** é filtrado por porto.
- Unidade x porto: não existe tabela ligando a unidade (`cod_unidade` `0817800`, `sigla_unidade`
  `ALFSTS`) ao porto (`BRSSZ`). Dá para derivar pelo prefixo do `cod_carga` dos recintos da unidade.
  `ovr_setores.cod_unidade` tem sujeira (espaço à esquerda, `testes`, códigos de 6 dígitos).
- Dados a revisar em `ovr_recintos`: `8931333 TEAG` e `8931354 TEG` têm o mesmo `cod_dte` (8931354);
  5 recintos recentes (FASSINA, DINAMO 1/2, REDEX III, SERRA E MARQUES) estão sem `cod_unidade`.

## Decisões do usuário (não reabrir sem motivo)

1. **Ciclo de vida é opcional.** O ciclo falha com frequência em localizar os eventos e, na
   previsão de atracação, ainda nem existe. O card aparece mesmo só com o CE declarado, sem
   desembarque e sem imagem — e a tela deixa isso explícito (selo "Sem ciclo de vida" + aviso).
2. **Unidade do card e da contagem = contêiner declarado** (`Item`), não o CE.
3. **Janela "Desembarque efetivo"**: usa `ciclo.data_desembarque`; se o contêiner não tem
   desembarque localizado, cai para `Escala.dataPrevistaAtracacao` (foi escolha explícita do
   usuário; a alternativa `dataEfetivaPrimeiraAtracacao` foi descartada).
4. **Filtro Recinto** casa com o recinto do ciclo (desembarque/armazenagem) **ou** com o terminal
   de descarregamento do Manifesto (direto, ou via `ovr_recintos.cod_siscomex -> cod_carga`).
5. **Imagem não é exigida por padrão**; há o checkbox "Somente com imagem de escaneamento".
6. **Tráfego: excluir apenas exportação** (`tipoTrafego = '7'`). Cabotagem e passagem devem
   aparecer, mesmo sem imagem: interessa analisar o risco e, se for o caso, mandar descer e escanear.
7. **Porto**: a escala que vale é a do porto do usuário. Por ora é um campo na tela
   ("Porto de atracação (escala)", filtro `porto_escala`, padrão `BRSSZ`; vazio = todos).
8. **Contagem total** do filtro exibida junto com os cards carregados.
9. **Forma de trabalhar**: mudanças pequenas e incrementais sobre o código existente ("tidy"),
   sem reescrever do zero; mexer o mínimo na tela, que o usuário considera boa; muitos testes com
   dados simulados; plano antes de mudanças não triviais. Produção não pode quebrar.

## Como a busca funciona (desenho atual)

- `monta_query_navii` monta `Item JOIN Conhecimento` + janela + filtros, **sem executar** (exceto
  a resolução do recinto em `ovr_recintos`). `busca_ces_navii` pagina (`Item.ID DESC`, cursor =
  último `Item.ID`), `conta_conteineres_navii` conta, `monta_cards_navii` enriquece em lote
  (ciclos, manifesto/escala, itens/NCMs, empresas, recintos) só para a página.
- Janela prevista: `Conhecimento.manifestoCE IN (manifestos com escala no período [e no porto])`.
- Janela efetiva: `UNION` de (itens com desembarque no período) e (itens pela escala prevista
  **sem** desembarque localizado), como tabela derivada de `Item.ID`.
- Filtros em outras tabelas são sempre `IN (subquery)`, nunca `JOIN`: não multiplicam linhas,
  dispensam `DISTINCT` e mantêm a contagem exata.
- `ovr_recintos` é lida por `tabela_recintos` (uma `Table` leve local). **Não** importar
  `bhadrasana.models.ovr.Recinto` para consultar: o registry do OVR não configura sozinho
  (relationship com RVF) e quebra na primeira query.

Armadilhas já encontradas (têm teste de regressão):
- JOIN com escalas duplica o contêiner (um manifesto tem várias escalas) → inflava a contagem e
  encurtava as páginas. Por isso o semijoin.
- `NOT IN`/`IN` com subquery aninhada em `manifestosresumo` fazia varredura completa (994 mil
  linhas); os terminais agora vão como lista literal.
- SQLite: `BIGINT PRIMARY KEY` não autoincrementa (`EscalaManifesto` precisa de `ID` explícito nos testes).
- SQLAlchemy 1.4 não renderiza `datetime` com `literal_binds` no dialeto MySQL; o diagnóstico
  compila com parâmetros e monta o SQL legível à parte.

## Estado atual

Feito e testado (64 testes passando) — commit `8b5e140` + alterações posteriores ainda **não
commitadas** em 30/09/2026 (semijoin da janela, terminais literais, exclusão de exportação,
filtro `porto_escala`, `--analyze` para MariaDB, amostras extras no diagnóstico, este arquivo):

- ciclo de vida opcional + selo/aviso na tela + nome do recinto na linha do tempo;
- contagem total; checkbox de imagem; filtro de porto de atracação (padrão BRSSZ);
- exclusão de exportação;
- script de diagnóstico rodado uma vez em produção (só EXPLAIN, sem `--amostras`/`--analyze`).

O que o EXPLAIN de produção mostrou:
- Janela prevista: bom plano (parte do índice de `dataPrevistaAtracacao`).
- Janela efetiva: ~97 mil candidatos em 11 dias (todos os portos, todos os tráfegos), tabela
  derivada + filesort refeitos a cada página e na contagem. O filtro de porto e a exclusão de
  exportação devem reduzir muito — **ainda não medido**.
- Estimativa de 1 linha de ciclo com desembarque entre 15 e 25/09/2026: suspeita de que o ciclo
  de vida não está populado para datas recentes — **não confirmado**.

## Pendências

Dependem de dados reais (rodar `python virasana/navii/diagnostico_sql.py --amostras`, e
`--analyze` em homologação/fora do pico):

1. ~~Formato de `portoDaEscala` e das datas da escala~~ — confirmados pelas amostras.
2. **Ponte recinto (cod_siscomex) ↔ terminal (cod_carga)** — não existe na base (ver "O que as
   amostras reais mostraram"). Decidir com o usuário: preencher `cod_carga` nas linhas de recinto
   (atenção: `conformidade_manager.get_completude_mercante` faz JOIN por `cod_carga` e passaria a
   duplicar), criar uma coluna/tabela de vínculo, ou um mapa fixo no NAVI-I.
3. **Código do recinto no ciclo de vida = `cod_siscomex`?** Compatível com as amostras (7 dígitos,
   `89xxxxx`), mas ainda não vimos linhas do ciclo de vida. Conferir no diagnóstico `--amostras`.
4. **Frescor do ciclo de vida** e **tempos reais** das buscas (`ANALYZE FORMAT=JSON`).
5. **Filtrar o ramo "desembarque efetivo" pelo porto/unidade** (recintos da unidade do usuário),
   já que a API Recintos cobre outros portos.

Índices sugeridos, **não criados** (decisão do usuário/DBA, medir antes):

```sql
ALTER TABLE ciclodevida_ciclo_de_vida_importacao
  ADD INDEX ix_ciclo_ce_conteiner (numeroCEmercante, numeroConteiner, data_desembarque);
ALTER TABLE itensresumo
  ADD INDEX ix_itens_ce_conteiner (numeroCEmercante, codigoConteiner), ALGORITHM=INPLACE, LOCK=NONE;
```

Índices redundantes (prefixo de outro): `ix_itensresumo_numeroCEmercante`,
`ix_ncmitemresumo_numeroCEMercante` (em `ncmitemresumo` os índices ocupam 35 GB para 18 GB de dados).

Limitações conhecidas e aceitas por enquanto:
- CE sem `Item` carregado não aparece (a base é o contêiner declarado).
- O filtro de porto não se aplica ao ramo "desembarque efetivo": o ciclo não guarda porto, só recinto.
- Seção "Contêineres" do card lista todos os contêineres do CE, não só o do card.
- Sem fonte de dados de sócios/CPF (filtros "subjetivos" por empresa e sócios ficam para depois).

## Próximos passos previstos

1. Receber do usuário amostras de **escalas, usuários e recintos** e validar as pendências 1–3.
2. **Equipes de trabalho / unidade do usuário** (requisito futuro declarado): o usuário pertence a
   uma unidade (ex.: Porto de Santos, `BRSSZ`, unidade `0817800`) e deve ver por padrão as escalas
   do seu porto e os recintos da sua unidade. Hoje isso é o campo `porto_escala` com padrão fixo
   (`PORTO_ESCALA_PADRAO`); `ovr_recintos` já tem `cod_unidade`/`sigla_unidade`/`id_equipe`, e os
   usuários ficam em `ovr_usuarios`/`ovr_setores` (bhadrasana2).
3. Medir com `--analyze` e decidir índices; se a janela efetiva continuar cara, avaliar limitar o
   ramo do ciclo aos recintos da unidade.
4. Backlog antigo: filtros em linguagem natural; filtros subjetivos por empresa/sócios.
