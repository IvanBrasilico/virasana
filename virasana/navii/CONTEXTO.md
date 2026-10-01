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
| `virasana/navii/referencias/` | Tabelas locais código→nome (NCM, portos, países, navios) + script que as gera |
| `virasana/tests/test_navii.py` | Testes unitários (sessão fake) |
| `virasana/tests/test_navii_referencias.py` | Testes das tabelas de referência e dos geradores |
| `virasana/tests/test_navii_busca_sqlite.py` | Testes de integração: queries reais em SQLite, cenário simulado |

Rodar os testes (a partir da raiz do `virasana`, venv `virasana-venv`, Python 3.9):

```bash
python -m pytest virasana/tests/test_navii.py virasana/tests/test_navii_busca_sqlite.py virasana/tests/test_navii_referencias.py --disable-warnings
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
  real. Por isso (e por decisão do usuário) o filtro Recinto usa só o ciclo de vida. O nome do
  terminal no aviso do card sem ciclo (busca por `cod_carga`) funciona para esses 4 operadores.
  Pares prováveis, por nome, nunca confirmados: 8931404↔BRSSZ009, 8931356↔BRSSZ016,
  8931318↔BRSSZ057, 8931359↔BRSSZ058.
- Há `cod_carga = ''` (vazio, não NULL) em `ovr_recintos` — cuidado ao usar a coluna (casaria com
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

## Tabelas de referência locais (`virasana/navii/referencias/`)

O sistema não tem boas tabelas de posições NCM, portos, países e navios (`laudo_ncm` é ruim e
**deixou de ser usada**). A pedido do usuário há um "BD" provisório em JSON, só para EXIBIR nomes,
carregado sob demanda por `referencias/__init__.py` e gerado por `atualiza_referencias.py`
(baixa das fontes; `--brutos DIR` regenera sem internet). Gerado em 30/09/2026:

| Arquivo | Fonte | Conteúdo |
|---|---|---|
| `ncm.json` | API pública do Portal Único Siscomex (oficial, vigente) | 96 capítulos, 1.228 posições (4 dígitos) |
| `portos.json` | UN/LOCODE (UNECE), espelho `datasets/un-locode` | 17.520 localidades **com função de porto** |
| `portos_ajustes.json` | **mantido à mão**, tem precedência | códigos do Mercante que divergem do LOCODE |
| `paises.json` | IBGE + Wikidata (territórios fora do IBGE: HK, TW, PR...) | 256 siglas ISO, em português |
| `navios.json` | Wikidata (não oficial) | 89 mil números IMO → nome |

**Filtro de uso (pedido do usuário em 30/09/2026):** só interessam navios que operam contêineres
(porta-contêineres, Ro-Ro/ConRo) e portos das rotas com o Brasil; as tabelas completas (89 mil
navios, 17,5 mil portos) assustaram pelo tamanho. As fontes públicas **não permitem** esse filtro:
na Wikidata 60% dos navios têm tipo genérico "ship" (só 20 dos 359 navios da amostra de escalas
estão marcados como porta-contêiner) e o UN/LOCODE não marca porto de contêiner. A solução é
filtrar pelo uso real na base: `atualiza_referencias.py --banco [--desde AAAA-MM-DD]` consulta o
Mercante (somente leitura, padrão últimos 12 meses), grava `usados.json` (navios de escalas cujos
manifestos têm contêiner; portos dessas escalas, dos manifestos e dos CEs do período) e gera
`portos.json`/`navios.json` só com esses códigos. **Pendente: o usuário rodar `--banco` uma vez**
(precisa do banco; até lá as tabelas estão completas). O script também lista os portos usados
que ficaram sem nome — candidatos a `portos_ajustes.json`.

Regras e limitações (importantes):
- **Nunca mostrar nome errado**: código sem nome confiável aparece só como código (+ país). Por isso
  só entram portos com função de porto no LOCODE — o Mercante usa códigos que no LOCODE são outra
  coisa (CNSHA/CNNGB/CNTAO são aeroportos; BRITA seria Itabuna/BA; CNZZZ, uma estação de trem).
  Os casos certos vão em `portos_ajustes.json` (hoje: 8 portos chineses e Itaguaí).
- Códigos **sem nome** que aparecem muito e o usuário pode ajudar a identificar: `BRITA` (214 das
  1000 escalas mais recentes), `BR400`, `BRSAS`, `BRQNS`, `BRAMW`, `BROUT`, `BR015`, `BR147`.
  `xxZZZ` = porto não especificado do país (mostra só o país).
- **Navios**: não existe fonte pública oficial e completa (IMO GISIS e Equasis exigem login e não
  permitem extração em lote). A Wikidata cobriu 74% dos IMOs de uma amostra de escalas e o nome
  pode estar desatualizado (navio rebatizado). `Escala.embarcacao` com 10 dígitos é inscrição
  nacional (navegação interior): sem fonte.
- Os filtros de porto (`porto_origem`, `porto_destino`, `porto_baldeacao`) aceitam código parcial:
  menos de 5 caracteres casa por prefixo — `CN`, `US` filtram pelo país.
- Nesta rede o download exige `curl --ssl-no-revoke` (checagem de revogação indisponível); o
  script tenta `urllib` e cai para o `curl`.

## Decisões do usuário (não reabrir sem motivo)

1. **Ciclo de vida é opcional.** O ciclo falha com frequência em localizar os eventos e, na
   previsão de atracação, ainda nem existe. O card aparece mesmo só com o CE declarado, sem
   desembarque e sem imagem — e a tela deixa isso explícito (selo "Sem ciclo de vida" + aviso).
2. **Unidade do card e da contagem = contêiner declarado** (`Item`), não o CE.
3. **Janela "Desembarque efetivo"**: usa `ciclo.data_desembarque`; se o contêiner não tem
   desembarque localizado, cai para `Escala.dataPrevistaAtracacao` (foi escolha explícita do
   usuário; a alternativa `dataEfetivaPrimeiraAtracacao` foi descartada).
4. **Filtro Recinto** casa só com o recinto do ciclo de vida (desembarque/armazenagem), pelo
   `cod_siscomex`. O terminal do Manifesto (`cod_carga`) **não** é usado: só existe para operadores
   portuários e deixaria recintos de fora. É um filtro secundário ("nem é primordial, poderia até
   ficar de fora") — não investir nele.
5. **Imagem não é exigida por padrão**; há o checkbox "Somente com imagem de escaneamento".
6. **Tráfego: excluir apenas exportação** (`tipoTrafego = '7'`). Cabotagem e passagem devem
   aparecer, mesmo sem imagem: interessa analisar o risco e, se for o caso, mandar descer e escanear.
7. **Porto**: a escala que vale é a do porto do usuário. Por ora é um campo na tela
   ("Porto de atracação (escala)", filtro `porto_escala`, padrão `BRSSZ`; vazio = todos).
8. **Contagem total** do filtro exibida junto com os cards carregados.
9. **Na dúvida, exibir a mais.** É um sistema de análise de risco: melhor mostrar mais e deixar o
   analista decidir do que deixar casos de fora. Por isso **não** se restringe o ciclo de vida por
   porto/unidade — um contêiner previsto para outro porto que desceu em Santos, ou previsto para
   Santos que desceu em outro lugar, interessa à equipe de Santos. O ciclo de vida é o desembarque
   real, e o CE continua trazendo o porto de destino final.
10. **Forma de trabalhar**: mudanças pequenas e incrementais sobre o código existente ("tidy"),
   sem reescrever do zero; mexer o mínimo na tela, que o usuário considera boa; muitos testes com
   dados simulados; plano antes de mudanças não triviais. Produção não pode quebrar.
11. **Produto = `Conhecimento.descricao`** (30/09/2026). O `NCMItem.descritivo` não tem informação
   útil (na base real traz números de BL/contêiner, ex.: `COSU6221941390`); o usuário confirmou
   com uma amostra. O filtro virou critério do CE (`criterios_ce`), dentro da janela.
12. **Filtros salvos** (30/09/2026): no **MongoDB** (coleção `navii_filtros_salvos`, sem DDL no
   MySQL), **só o dono** vê/apaga os seus, e **o período não é salvo** (ao recuperar, vale o
   período sugerido pela tela). Filtros guardados como documento livre, porque os campos mudam.
13. **Listas**: porto de atracação/origem/destino/baldeação, NCM e recinto aceitam vários valores
   ("OU"); na tela, cada valor é um chip, com autocompletar (portos/países e NCM das tabelas locais
   de referência, recintos de `ovr_recintos`).

## Como a busca funciona (desenho atual)

- `monta_query_navii` monta `Item JOIN Conhecimento` + janela + filtros, **sem executar** (exceto
  a resolução do recinto em `ovr_recintos`). `busca_ces_navii` pagina (`Item.ID DESC`, cursor =
  último `Item.ID`), `conta_conteineres_navii` conta, `monta_cards_navii` enriquece em lote
  (ciclos, manifesto/escala, itens/NCMs, empresas, recintos) só para a página.
- Janela prevista: `Conhecimento.manifestoCE IN (manifestos com escala no período [e no porto])`.
- Janela efetiva: `UNION` de (itens com desembarque no período) e (itens pela escala prevista
  **sem** desembarque localizado), como tabela derivada de `Item.ID`.
- **A janela dirige; os filtros são verificados por candidato.** Filtros em outras tabelas
  (NCM/produto, imagem, entregue, recinto, e o "sem desembarque" da janela efetiva) usam `_existe`:
  subquery escalar correlacionada com `LIMIT 1`. Nunca `JOIN` (multiplica linhas) e **nunca
  `IN (subquery)`/`EXISTS`** para esses filtros: o MariaDB os converte em semijoin e pode passar a
  dirigir a busca pela subquery (ver "Tempos medidos"). Exceções deliberadas, que devem mesmo
  dirigir: o semijoin dos manifestos com escala no período e o de `porto_baldeacao`.
- `ovr_recintos` é lida por `tabela_recintos` (uma `Table` leve local). **Não** importar
  `bhadrasana.models.ovr.Recinto` para consultar: o registry do OVR não configura sozinho
  (relationship com RVF) e quebra na primeira query.

Armadilhas já encontradas (têm teste de regressão):
- JOIN com escalas duplica o contêiner (um manifesto tem várias escalas) → inflava a contagem e
  encurtava as páginas. Por isso o semijoin.
- `IN (subquery)` como filtro deixou o banco dirigir a busca pela subquery e explodir em itens sem
  contêiner (16 min medidos). Por isso `_existe` com `LIMIT 1` (ver "Tempos medidos").
- Item com `codigoConteiner` vazio existe aos milhões: nunca chegar em `itensresumo` só pelo contêiner.
- SQLite: `BIGINT PRIMARY KEY` não autoincrementa (`EscalaManifesto` precisa de `ID` explícito nos testes).
- SQLAlchemy 1.4 não renderiza `datetime` com `literal_binds` no dialeto MySQL; o diagnóstico
  compila com parâmetros e monta o SQL legível à parte.

## Estado atual

Revisado em 30/09/2026 (fim do dia). Último commit: `bfc564d`. **Não commitado ainda** (tudo
testado — 102 testes passando nos arquivos de teste do NAVI-I, Entregas e apreensões):
semijoin da janela, exclusão de exportação, filtro `porto_escala`, filtros por candidato com
`_existe`, filtros de Conhecimento dentro da janela (`criterios_ce`), recinto só pelo ciclo,
normalização da entrada (porto em maiúsculas, NCM só dígitos), `--analyze` para MariaDB com resumo
de tempos, tabelas de referência filtradas pelo uso (`usados.json` desde 30/09/2025), filtro de
porto por país, leitura tolerante (gzip/codificação) no gerador de referências; e, fora do NAVI-I,
a tela **Entregas por CNPJ** (`virasana/entregas/`) e o script `dados_transporte_apreensoes.py`.

Na tela: ciclo de vida opcional com selo/aviso; nome do recinto na linha do tempo; contagem
total; checkbox de imagem; porto de atracação (padrão BRSSZ); nomes de portos/países/navios e
descrição oficial da posição NCM ao lado do texto declarado.

O efeito de `criterios_ce` foi medido na terceira rodada (ver "Terceira medição").

Depois da terceira medição (30/09/2026, noite; commit anterior `090ab9f`):
- **Produto** passou a pesquisar `Conhecimento.descricao` (decisão 11). Ainda não medido; deve
  ficar muito mais barato (o texto já é lido junto com o CE; antes eram ~49 linhas de
  `ncmitemresumo` por candidato).
- **Filtros em lista** (decisão 13): `normaliza_filtros` transforma os campos de `CAMPOS_LISTA`
  em listas; `_algum_porto` junta códigos completos num `IN` e países (prefixo) em `LIKE`; NCM vira
  um `OR` de `LIKE` dentro do mesmo `_existe`; recinto, `IN`. A API aceita `?ncm=8471&ncm=6109`
  ou `?ncm=8471,6109`. `busca_manifestos_navii(porto=...)` aceita lista.
- **Autocompletar**: `GET /navii/api/sugestoes?campo=porto|ncm|recinto&q=` —
  `referencias.sugere_portos`/`sugere_ncm` (tabelas locais, sem banco) e
  `busca_navii.sugere_recintos` (`ovr_recintos`, só linhas com `cod_siscomex`). NCM sugere capítulo
  e posição; o usuário pode digitar o código completo.
- **Filtros salvos** (decisão 12): `navii/filtros_salvos.py` + `GET|POST|DELETE /navii/api/filtros`
  (JSON; o POST/DELETE mandam `X-CSRFToken`, lido de `<meta name="csrf-token">`). Índice único
  (usuario, nome); máx. 100 por usuário; só as chaves de `CAMPOS_SALVAVEIS`, normalizadas.
  Testes com coleção falsa em memória (`mongomock` não está instalado no venv).
- Testado também no navegador com o servidor de demonstração (dados simulados).

## Tempos medidos em produção (`--analyze`, 30/09/2026, versão ANTERIOR ao `_existe`)

Rodada de 128 queries, 46 minutos no total. Janela efetiva 15–25/09, prevista 03–15/10.

| Cenário | Página | Contagem |
|---|---|---|
| prevista, porto BRSSZ | 0,3 s | 0,1 s |
| prevista, todos os portos | 0,7 s (9 s com cache frio) | 0,3 s |
| efetiva, porto BRSSZ | 1,5–1,9 s | 2,4 s |
| efetiva, todos os portos (375 mil candidatos) | 2,6 s (63 s com cache frio) | 4,4 s |
| efetiva + produto (texto livre), todos os portos | 32–109 s | 31 s |
| efetiva + NCM completo, todos os portos | **995 s** | 135 s |
| efetiva + porto + recinto + com imagem | **694 s** na 1ª, 4 s na 2ª | 3,4 s |

Causas encontradas no `ANALYZE FORMAT=JSON`:
- **Itens sem contêiner**: o semijoin do NCM passou a dirigir a busca e entrou em `itensresumo`
  pelo índice de `codigoConteiner`; como existem **~2,2 milhões de itens com `codigoConteiner`
  vazio** (carga solta, granel, veículos), cada NCM sem contêiner trouxe 2,2 M linhas (31 × 2,2 M).
  Corrigido com `_existe` (a janela sempre dirige). **Reavaliar com nova medição.**
- O `NOT EXISTS` do "sem desembarque" era materializado varrendo o ciclo inteiro (281 mil linhas,
  3,4 s por query). Também virou `_existe` (acesso indexado por candidato).
- O ramo do terminal do Manifesto no filtro Recinto (removido).
- **Cache frio**: a primeira leitura de cada região das tabelas grandes custa dezenas de segundos.
- As estimativas do otimizador erram muito (estimou 97 mil candidatos, eram 375 mil; 31 linhas de
  NCM viraram 69 milhões). Não confiar em `EXPLAIN` sem `ANALYZE`.

### Segunda medição (30/09/2026, já com `_existe` e com o índice `ix_ciclo_ce_conteiner` criado)

140 queries, 9 minutos no total (antes: 46 min); a mais lenta caiu de 995 s para 44 s.

| Cenário | Efetiva (página) | Prevista (página) |
|---|---|---|
| porto BRSSZ, sem outros filtros | 1,7 s (contagem 2,5 s) | 0,3 s |
| porto BRSSZ + NCM | 1,8–2,4 s | 0,5 s |
| porto BRSSZ + NCM completo | 3,9 s | 0,5 s |
| porto BRSSZ + produto (texto livre) | 9,1 s | 1,4 s |
| porto BRSSZ + recinto + com imagem | 2,8 s | 0,3 s |
| todos os portos, sem filtros | 3,1 s | 0,7 s |
| todos os portos + produto (texto livre) | 44 s | 17,5 s |

O que o `ANALYZE` mostrou nesta rodada:
- **Volume real**: em 11 dias, Santos tem 175 escalas, 4.340 manifestos, 112 mil CEs e **209 mil
  contêineres candidatos** (o país todo: 375 mil). Esse conjunto é recalculado e ordenado a cada
  página e na contagem — é o piso de ~1,6 s da janela efetiva (leitura de CEs 0,3 s, itens 0,6 s,
  verificação do ciclo 0,45 s).
- Depois desta medição os filtros de Conhecimento (exportação, portos, empresa) passaram a ser
  aplicados também DENTRO da janela (`criterios_ce`), para não ler itens/ciclo de CEs descartados.
  **Ganho ainda não medido.**
- **Produto (texto livre)** é o filtro caro: lê em média 40 linhas de `ncmitemresumo` por
  contêiner (15 milhões de linhas no país todo). Só um índice FULLTEXT em `descritivo` (152 M
  linhas) resolveria; combinar com NCM ou porto reduz muito.
- Se o piso de ~1,6 s por página incomodar: o caminho é guardar o resultado da janela (cache por
  filtro) em vez de recalcular a cada rolagem — não feito.

### Terceira medição (30/09/2026 17h, com `criterios_ce` dentro da janela)

Log `navii_diagnostico_20260930_171546.log`: 140 queries, 513 s no total (antes 561 s). Janela
efetiva 15–25/09, prevista 03–15/10.

| Cenário (efetiva) | 2ª medição | 3ª medição |
|---|---|---|
| porto BRSSZ, sem outros filtros | 1,7 s (contagem 2,5 s) | **1,2 s** (contagem 2,0 s) |
| porto de origem / destino / baldeação (todos os portos) | 3,0 s | **0,2–0,9 s** |
| empresa (texto ou CNPJ) | 3,2–4,7 s | **0,5 s** |
| origem + NCM + até 3 NCMs | 10,8 s | **2,0 s** |
| recinto + empresa + entregue | 5,1 s | **0,6 s** |
| porto BRSSZ + NCM (página / contagem) | 2,4 s / 6,7 s | 1,3–2,6 s / **20,5 s** |
| NCM por prefixo, todos os portos (página / contagem) | 3,5 s / 12,3 s | 2,5 s / **17,7 s** |
| produto "brinquedo", todos os portos | 44 s | **99 s** na 1ª, 30 s depois |
| todos os portos, sem filtros | 3,1 s | 20,7 s na 1ª (cache frio), 2,3 s depois |

Prevista: igual à rodada anterior (0,1–4 s; produto 17–25 s).

Leitura do `ANALYZE`:
- `criterios_ce` **funcionou**: filtros de Conhecimento caíram de 3–5 s para menos de 1 s. O
  piso da efetiva com porto BRSSZ ficou em ~1,2 s (140 mil candidatos em Santos).
- **A contagem com NCM é o gargalo que sobrou**: a página para ao achar 20 cards, mas a contagem
  sonda o NCM de TODOS os candidatos (140 mil `_existe` em `ncmitemresumo`, 12 linhas cada, pelo
  `ix_ncmitem_chave` — que já cobre `identificacaoNCM`; é I/O aleatório, 18 s). Como a contagem
  roda na mesma requisição da 1ª página, **a tela espera 20 s para mostrar cards prontos em 2 s**.
  Proposta (não feita): buscar a contagem numa requisição separada, depois dos cards.
- **Produto "brinquedo" não encontrou nenhum contêiner** nos 11 dias (252 mil candidatos
  sondados, 49 linhas de NCM cada = 12 M linhas, 95 s). Sem acerto a página nunca completa e
  varre tudo — é o pior caso. Suspeita: `descritivo` não está em português/pode ser abreviado;
  conferir com amostra antes de concluir. O `ilike` vira `lower(descritivo) LIKE lower(...)`
  (desnecessário com collation `_ci`, mas não muda o plano: não há índice em `descritivo`).
- Os 20 s de "todos os portos" são cache frio (1ª query da rodada); a página seguinte leva 2,3 s.

Outros fatos do diagnóstico com amostras:
- Código de recinto no ciclo de vida = `ovr_recintos.cod_siscomex` (confirmado: `8931404`).
- **Ciclo de vida atrasado**: último desembarque em 14/09/2026 (16 dias antes). Na janela efetiva
  sugerida (15 a 5 dias atrás) não havia nenhum ciclo — tudo veio pelo fallback da escala.
- **Os ciclos mais recentes estão com `numeroCEmercante` vazio** (só o contêiner): não casam com
  `Item` e ficam invisíveis até o CE ser preenchido pelo processo de completar o ciclo.
- Um manifesto de passagem (HKHKG→ARBUE) tem uma escala por porto brasileiro onde o navio atraca
  (BRSSZ, BRIOA, BRRIG) — confirma o critério de usar a escala do porto do usuário.
- CEs recentes por tráfego: 5 (48%), 7 exportação (21%), 3 (15%), 9 (14%), 1 (2%).

## Pendências

Dependem de dados reais (rodar `python virasana/navii/diagnostico_sql.py --amostras`, e
`--analyze` em homologação/fora do pico):

1. ~~Medir de novo depois do `_existe`/`criterios_ce`~~ — feito (terceira medição). Sobrou:
   **contagem assíncrona** (a contagem com NCM custa 18–20 s e trava a 1ª página — proposta, ainda
   não aprovada/feita). O `descritivo` foi conferido: não serve (decisão 11). Medir de novo o filtro
   de produto, agora pela descrição do CE.
2. **Itens sem contêiner** (carga solta/granel/veículos, ~2,2 M de 51 M = 4%): aparecem como card
   com título "--". O usuário aceita excluí-los **se** isso melhorar muito o desempenho (o foco é
   carga conteinerizada). Análise: depois do `_existe` o ganho esperado é pequeno (~4% menos
   candidatos; o desastre de 16 min vinha do plano de execução, não da quantidade). Mantidos por
   ora; decidir com a nova medição. Se excluir: `Item.codigoConteiner != ''` em `monta_query_navii`.
3a. **Card do filhote sem ciclo/imagem** (medido em 01/10/2026, ver
   `analytic_scripts/ciclo_de_vida/CICLO_DE_VIDA.md`): o contêiner consta nos itens do CE master E do
   filhote; o ciclo guarda um só CE (36% das vezes o master) e o NAVI-I liga card↔ciclo pelo CE exato
   (`_mesmo_conteiner_do_ciclo`) — o card do filhote (importador real) fica sem ciclo/imagem. Resolver
   no ciclo de vida (ligar o ciclo à família do CE) ou, enquanto isso, ligar pelo contêiner + família.
3. **Ciclos sem CE**: avaliar se vale casar ciclo e Item só pelo contêiner + proximidade de data
   quando o CE do ciclo está vazio (não feito: risco de casar contêiner reutilizado).
4. **Ponte recinto ↔ terminal do manifesto**: abandonada por decisão do usuário (decisão 4). As
   duas linhas por terminal em `ovr_recintos` continuam sem vínculo; só importa se um dia o filtro
   de recinto precisar alcançar contêineres sem ciclo.

Índices sugeridos, **não criados** (decisão do usuário/DBA, medir antes):

```sql
-- ajuda todos os _existe do ciclo (hoje entram por numeroCEmercante e filtram o contêiner)
ALTER TABLE ciclodevida_ciclo_de_vida_importacao
  ADD INDEX ix_ciclo_ce_conteiner (numeroCEmercante, numeroConteiner, data_desembarque);
ALTER TABLE itensresumo
  ADD INDEX ix_itens_ce_conteiner (numeroCEmercante, codigoConteiner), ALGORITHM=INPLACE, LOCK=NONE;
```

Índices redundantes (prefixo de outro): `ix_itensresumo_numeroCEmercante`,
`ix_ncmitemresumo_numeroCEMercante` (em `ncmitemresumo` os índices ocupam 35 GB para 18 GB de dados).

Limitações conhecidas e aceitas por enquanto:
- CE sem `Item` carregado não aparece (a base é o contêiner declarado).
- O filtro de porto não se aplica ao ramo "desembarque efetivo" — é decisão do usuário (decisão 9).
- Seção "Contêineres" do card lista todos os contêineres do CE, não só o do card.
- Sem fonte de dados de sócios/CPF (filtros "subjetivos" por empresa e sócios ficam para depois).

## Próximos passos previstos

1. Receber do usuário amostras de **escalas, usuários e recintos** e validar as pendências 1–3.
2. **Equipes de trabalho / unidade do usuário** (requisito futuro declarado): o usuário pertence a
   uma unidade (ex.: Porto de Santos, `BRSSZ`, unidade `0817800`) e deve ver por padrão as escalas
   do seu porto e os recintos da sua unidade. Hoje isso é o campo `porto_escala` com padrão fixo
   (`PORTO_ESCALA_PADRAO`); `ovr_recintos` já tem `cod_unidade`/`sigla_unidade`/`id_equipe`, e os
   usuários ficam em `ovr_usuarios`/`ovr_setores` (bhadrasana2).
3. Medir com `--analyze` e decidir índices. (Não limitar o ciclo aos recintos da unidade: decisão 9.)
4. Backlog antigo: filtros em linguagem natural; filtros subjetivos por empresa/sócios.
