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
| `virasana/navii/diagnostico_card.py` | Script: "por que o card de um contêiner não aparece" — itens, CE, ciclos, eventos da API, as buscas da tela em vários períodos com o motivo de cada exclusão e o card em JSON (21/10/2026). Lembrete: o extrato do CE busca sem data; o filtro de contêiner continua preso ao período |
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
| `tipos_conteiner.json` | **mantido à mão** — ISO 6346 (tamanho, grupo, detalhe) | tipo do contêiner, potencial OOG |
| `tipos_bl.json` | **mantido à mão** — `TIPOS_BL` do Mercante | 10 BL único, 11 MBL, 12 HBL, 15 MHBL |

**Tipos de contêiner e de BL (02/10/2026, pedido do usuário):** `referencias.tipo_conteiner(codigo)`
reconhece o código ISO (`45G1` → "40' high cube, uso geral (dry)"), o numérico antigo (`2210`) e
apelidos (`40HC`, `40OT`, `20FR`), com `oog` (open top `U` e plataforma/flat rack `P`: carga fora de
gabarito pode não passar no escâner) e `confirmado`. O grupo **K** (`22K0` a `22K8`, 2,8% dos desembarques)
não é da ISO 6346:1995: é **tanque** (usuário, 02/10/2026: `22K1` = tanque pressurizado). Cobertura
nos desembarques de 30 dias: 97,8% reconhecidos; sem tipo 2,2%; `M7PW`/`M0PW` (0,05%) sem
descrição. `referencias.descreve_tipo_bl`/`tipos_bl` dão sigla + descrição curta; conferido no
banco que só existem 10, 11, 12 e 15 (CEs desde 01/08/2026: 72% BL, 16% HBL, 10% MBL, 1,4% MHBL).

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
14. **Filtros OCULTAR** (16/10/2026; termo escolhido pelo usuário entre Ocultar, Não exibir e Exceto — "Excluir"
   foi descartado porque, em sistema, quer dizer apagar). Só **NCM** e **porto de origem** (chaves
   `ncm_ocultar`, `porto_origem_ocultar`). Regras — sistema de risco, **nunca sonegar informação**:
   - NCM: o card some só quando **TODOS** os NCMs do contêiner batem com algum código oculto (um NCM não oculto
     basta para aparecer); sem NCM declarado, ou com NCM vazio/NULL (desconhecido), o card aparece. Mesma chave
     (CE + contêiner) do filtro positivo — o card lista os contêineres do CE, mas cada contêiner tem o seu card.
   - Origem: porto vazio/NULL aparece (o `NOT (... LIKE ...)` do SQL daria NULL e o card sumiria).
   - Código oculto igual ou prefixo de um código do filtro positivo do mesmo campo é **ignorado, com aviso**
     (filtrar prevalece); o contrário é refinamento (filtrar 8471, ocultar 847130). Código oculto: só letras e
     dígitos (curinga nunca vira LIKE que esconde tudo) e ao menos `MINIMO_OCULTAR` = 2 caracteres.
   - Tela: seletor **Filtrar | Ocultar** no campo; chip oculto em vermelho, com olho cortado e borda tracejada (não
     só a cor); botão no chip para trocar de modo; a tela recusa na hora (com aviso) o que o servidor ignoraria e
     mantém no campo o código recusado. Nos cards, botão em cada NCM e na origem abre o menu Filtrar por / Ocultar
     (código inteiro ou posição/país), que aplica, recarrega e mostra "de X para Y contêineres" com **Desfazer**
     (em vez de pedir confirmação). Os avisos do servidor (filtro salvo ou link) aparecem sob a contagem.
   - Testes: `test_navii_ocultar.py` (cenário adversarial: NCM NULL, '', misto, truncado, com pontos, mesmo
     contêiner em outro CE, curingas, conflitos, porto NULL/''), conferidos com 7 mutantes (todos derrubados) e com
     um roteiro de ponta a ponta no navegador (servidor local com os dados dos testes).
15. **Cards ainda sem desembarque** (ticket dos usuários, 16/10/2026: confusos com cards da janela "Desembarque
   efetivo" que não desembarcaram — entraram pela atracação prevista, a regra 3). Solução: o card diz isso em
   destaque, com a data prevista — selo **Previsto · dd/mm** (hoje/futura) ou **Desembarque não informado**
   (laranja, previsão vencida: falha de informação ou contêiner que não desceu), faixa no topo do card e a etapa
   "Desembarque" como *previsto* (ponto tracejado). Regras:
   - "Sem desembarque" do card = `_tem_desembarque_localizado` (o MESMO critério da janela, por candidato, numa
     consulta só com os itens da página — `_itens_com_desembarque`): sem ciclo, ciclo sem data (loop antigo), e
     pela família gravada pelo ciclo (`ciclodevida_ciclo_ce`). O card NÃO decide pelo ciclo que achou: ele procura
     o ciclo pela família do Mercante, que pode divergir (caso K20 do teste).
   - Data mostrada: a escala DENTRO do período (`busca_manifestos_navii(periodo=...)`), não a maior do manifesto.
   - `previsaoAtracacao` vem do servidor (`_previsao_atracacao`, relativa a hoje): futura, hoje ou vencida.
   - Caixa "Incluir os ainda sem desembarque" (marcada; regra 9, na dúvida exibir a mais): desmarcada = filtro
     `so_com_desembarque`, nas duas janelas; na efetiva tira da consulta o ramo da atracação prevista (mais leve).
   - **Descartado**: `Escala.dataEfetivaPrimeiraAtracacao` — quase nunca preenchida (usuário, 16/10/2026).
   - Testes: `test_navii_sem_desembarque.py` (propriedade "quem sai com o filtro = quem tem o selo", nas duas
     janelas; família divergente; ciclo sem data; desembarque fora do período; manifesto com duas escalas; busca
     pelo contêiner), 7 mutantes no texto (todos derrubados) e roteiro no navegador.
   - Pendência: o card cujo desembarque foi achado só pela família gravada pelo ciclo mostra "Sem ciclo de vida"
     (sem a linha do tempo) — `busca_ciclos_navii` poderia procurar também por `ciclodevida_ciclo_ce`.
16. **Faixa de peso** (20/10/2026): filtros `peso_min`/`peso_max` (kg, `CAMPOS_PESO`) sobre o peso bruto declarado
   do **CONTÊINER** (`Item.pesoBruto`) — escolha do usuário entre o do contêiner e o total do CE (o "Peso bruto
   total" do card, que exigiria somar os itens do CE por candidato). Regras: faixa inclusiva; um lado vazio = sem
   limite; com faixa, o contêiner sem peso declarado (vazio ou inválido) fica de fora; faixa invertida é trocada
   (servidor e tela); valor inválido ou negativo é descartado (`_normaliza_peso`). SQL: `CAST(REPLACE(TRIM(...),
   ',', '.') AS DECIMAL(15,3))` na linha do candidato (`_criterios_peso`, sem consulta extra) — a mesma leitura do
   `_texto_para_float` do card (ponto ou vírgula decimal; separador de milhar não é tratado em nenhum dos dois).
   Também nos filtros salvos, nos chips ("1.200,5 kg") e nos rótulos das estatísticas. Testes:
   `test_navii_filtro_peso.py` (ponto, vírgula, vazio, inválido, espaços, pontas, faixa invertida, contagem, filtro
   salvo, API, tela), 12 mutantes (todos derrubados) e roteiro no navegador.
17. **Imagem pelo contêiner e data, sem ciclo de vida** (20/10/2026; plano em
   `analytic_scripts/ciclo_de_vida/CICLO_DE_VIDA.md`, "Fora de Santos e pesagens", D2). O ciclo de vida só cobre
   Santos; fora dele (ex.: GSLU2053602 escaneado no Rio, 7921302) o card ficava sem imagem. `imagem_fallback.py`,
   só nos cards SEM imagem do ciclo: janela da operação do CE (previsão de atracação −2 a +30 dias; sem previsão, o
   desembarque do ciclo), cortada no 1º evento de outra operação depois da 1ª saída (embarque, desembarque, entrada
   vazio); prefere a inspeção CHEIA da API e a imagem pela defasagem do recinto (confiança média); sem inspeção, só
   a imagem do Mongo (`numeroinformado` + `dataescaneamento`), fora as perto de inspeção vazia (confiança baixa).
   O card leva `imagemFallback` e o painel de imagens, o selo com a origem. Acessório: falha só vai ao log. O filtro
   "Somente com imagem" continua só pelo ciclo. Testes em `test_navii_imagem_fallback.py`.
18. **Pesagem do contêiner: medido x declarado** (20/10/2026; P1–P3 de `CICLO_DE_VIDA.md`, "Fora de Santos e
   pesagens"). O serviço do ciclo de vida grava em `ciclodevida_pesagem` as pesagens do contêiner no ciclo; a da
   retirada no operador e a da chegada ao seco são comparadas com o peso declarado do contêiner (item do master ou
   soma dos filhotes), com o diagnóstico prévio e a PRINCIPAL do ciclo. `pesagem_navii.py`: o card leva `pesagem`
   {principal, pesagens} (selo "Pesagem: …", barras declarado x medido, diagnóstico e confirmação, expansão com os
   dados brutos); filtros `pesagem` (fraude, digitacao, divergente, coerente, comparada), `dif_pct_min` (módulo da
   diferença %) e `ordem_grandeza` (medido ≥ 10× ou ≤ 1/10) — sobre a principal, pela ligação do ciclo
   (`_existe_ciclo(_existe(PESAGENS, ...))`, um acesso indexado por candidato). Termos do usuário: "digitação
   errada" e "possível fraude" (= divergência relevante). Sem a tabela (DDL não aplicada), o card fica sem o bloco
   e o filtro dá erro claro (`tabelas_opcionais`). Premissa do usuário: sem código por recinto — o dado é o que o
   recinto transmitiu, e o erro dele é cobrado dele. Testes: `test_navii_pesagem.py`; E2E no navegador.
19. **Atracação prevista sempre à vista** (21/10/2026, pedido do usuário: "fundamental nos bastidores"). O card
   mostra a data prevista E o porto da escala (`portoAtracacaoPrevista`, de `busca_manifestos_navii`, a mesma
   escala de antes: a do porto filtrado, a do período se houver), também com desembarque — antes só aparecia sem
   desembarque/sem ciclo. O extrato do CE lista as escalas do manifesto (`escalas`: escala, porto, prevista,
   efetiva, navio) e o cabeçalho prefere a escala em `PORTO_ESCALA_PADRAO` (o porto que o ciclo de vida usa para
   escolher o CE; sem ela, a mais tarde — `extrato_navii.atracacao_do_cabecalho`). Testes:
   `test_navii_atracacao_prevista.py`; E2E no navegador (demo).
20. **Filtro por tipo de tráfego** (23/10/2026, pedido do usuário). `trafego` em `CAMPOS_LISTA` (códigos de
   `Conhecimento.tipoTrafego`, vale qualquer um): caixas de marcar na tela como o tipo de BL, opções de
   `referencias/tipos_trafego.json` (Importação 5, Cabotagem 3, Passagem 9; a exportação nunca aparece no NAVI-I).
   Nenhuma marcada = todos; com o filtro, o CE sem tráfego informado fica de fora. É um critério do CE
   (`_criterios_ce`, tabela `_CODIGOS_DO_CE` junto com o tipo de BL): vale também dentro do ramo da atracação
   prevista, e a coluna tem índice. Filtros salvos, chips e estatísticas de uso já o tratam. Testes:
   `test_navii_filtro_trafego.py`; cenário no `diagnostico_sql.py`.
21. **Ciclo pela família GRAVADA pelo ciclo de vida** (23/10/2026, C7 de `CICLO_DE_VIDA.md`: contêiner consolidado
   com vários BLs únicos do mesmo manifesto — todos na `ciclodevida_ciclo_ce`). `busca_ciclos_navii`: sem ciclo pelo
   CE nem pela família do Mercante, procura pela família gravada (uma consulta por página; respeita
   `LIGACAO_FAMILIA`); o card mostra "localizado pelo CE X, da mesma família (master/filhote ou outro BL do mesmo
   manifesto)". `_itens_com_desembarque` (selo da página) usa `_existe_ciclo(atalho_bl_unico=False)`. Os filtros por
   candidato mantêm o atalho do BL único (desempenho medido em 02/10) — limitação: contêiner + "só com desembarque"
   deixa de fora o BL que não é o principal. Testes: `test_navii_sem_desembarque.TestConteinerConsolidado`.
22. **Modos de exibição dos cards** (22/10/2026, pedido do usuário: "como o Explorer do Windows"). Detalhado (o card
   de sempre; *Contêineres e NCMs declarados* num `<details>` recolhido com contagem e prévia dos NCMs),
   Simplificado (grade `auto-fill`, imagem 2,2:1 em cima, só o começo do card) e Galeria (só as imagens em linhas
   justificadas; dados do card simplificado num painel ao passar o mouse/foco; Esc dispensa — WCAG 1.4.13; em tela
   de toque, legenda curta fixa). **Uma só marcação por card**: o modo é a classe `visao-*` da lista e o CSS esconde
   partes; as regras valem para `.card-navii:not(.expandido)` — "Ver detalhes" (`.expandido`) mostra o card
   detalhado no lugar, ocupando a linha. Assim menus, Abrir Ficha, lightbox e coleta de uso valem nos três sem
   código por modo. Galeria: `montaLinhasGaleria` (JS) fecha cada linha na altura mais próxima da alvo
   (`--altura-galeria`, controle deslizante) com ou sem o card seguinte (só CSS, um card sozinho na linha esticava
   2,5×); larguras inteiras sobre a largura real arredondada para baixo (a fracionária quebrava a linha);
   `--proporcao` vem da imagem carregada. Modo e tamanho no `localStorage` (por navegador). Barra de ferramentas
   fixa (≥ 64em) e âncora na troca de modo (o card do topo continua à vista); em tela ≤ 48em, filtros em cima,
   recolhidos de início (antes, lado a lado, sobravam 15 px para os cards). Imagem principal: botão de teclado
   (Enter amplia) e `loading="lazy"`. Coleta de uso: a definição de visto é a mesma (50% do card por 1 s) — na
   galeria vários cards pequenos ficam à vista de uma vez; "Ver detalhes" conta como interação `detalhes`. Testes:
   `test_navii_visoes.py` (a tela); comportamento conferido no navegador com servidor de demonstração (imagens
   sintéticas 20'/40'), desktop e celular.
23. **Cubagem declarada no card** (24/10/2026, observação do usuário na revisão dos sinais de declaração: "o card
   geral e os cards de contêineres não exibem a cubagem informada"). Card do CE: "Cubagem declarada" ao lado do
   peso bruto total — a do CE (`Conhecimento.cubagem`); sem ela, a soma dos contêineres; as duas quando diferem.
   Contêiner: cubagem (`Item.cubagemM3`), ocupação (cubagem ÷ capacidade de referência do tipo, `capacidades` de
   `referencias/tipos_conteiner.json`, no `title`) e densidade (peso ÷ cubagem, kg/m³). Regras de tipo e capacidade:
   `analytic_scripts/declaracao/regras_declaracao.py` (as do diagnóstico). `busca_itens_navii` →
   `_volume_do_conteiner` e `_resumo_dos_conteineres` (`cubagemTotal`); `monta_card_navii` → `cubagemDeclarada`,
   `cubagemTotal`. Testes: `test_navii.py`, `test_navii_busca_sqlite.py`; conferido no navegador (demonstração).

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

Em 02/10/2026 (pedido do usuário — tipos de contêiner/BL e família do CE):
- **Card**: tipo de BL com sigla + descrição curta; tag "Master de N filhotes" (lista no `title`); tipo
  do contêiner (`Item.isoCode`) com descrição e selo **Potencial OOG**, também na lista de contêineres.
- **Família do CE** (`busca_familias_navii`: o CE, o master, os filhotes e os irmãos, uma consulta por
  página pelo índice de `numeroCEMaster`): o ciclo de vida do card é procurado em qualquer CE da
  família com o mesmo contêiner, preferindo o próprio (`busca_ciclos_navii(..., familias)`); quando
  vem de outro CE, o card avisa (`cicloPorCE`). Resolve a pendência 3a **para o card**.
- **Filtros**: `tipo_bl` (lista; caixas de marcar, "nenhum = todos") pelo `Conhecimento` (vale dentro
  da janela) e `sem_oog` (exclui `Item.isoCode` de potencial OOG; sem tipo informado não é OOG).
  Os dois são salvos nos filtros do usuário (`CAMPOS_MARCAVEIS`).
- Testado também no navegador (servidor de demonstração com dados simulados).

Em 02/10/2026 (ciclo de vida v2 em produção; decisões do usuário):
- **Família do CE na janela e nos filtros** (pendência 3a): `_existe_ciclo` = ciclo pelo CE gravado OU
  (`LIGACAO_FAMILIA`) por qualquer CE da família em `ciclodevida_ciclo_ce` (dois `_existe`, o segundo
  pelo `ix_ciclo_ce_ce`); a janela "efetiva" ganhou um terceiro ramo no `UNION` (ciclos do período ->
  `ciclo_ce` não principal -> `Item`). O "sem desembarque localizado" também considera a família.
  **Preocupação do usuário: desempenho** (milhões de CEs). Desliga sem mudar código com
  `NAVII_LIGACAO_FAMILIA=0`; medir com `diagnostico_sql.py --analyze --so-ciclo` com e sem
  `--sem-familia` ANTES de reiniciar a aplicação em produção. Ainda NÃO medido.
- **Filtro de estágio** (`estagio`, lista; `estagios_ciclo.py`): no operador, em trânsito, em recinto
  seco, entregue, reembarcado, incompleto/abandono. Decisão do usuário: "concluído" = **entregue**
  (concluído no operador ou no TRA), com as outras opções para ver cargas em vários estágios. O
  `concluido` antigo (links/filtros salvos) vira o estágio "entregue" em `normaliza_filtros`. Ciclos
  antigos (sem `situacao`) só entram em "entregue" (`concluido`).
- **Card**: selo = estágio (com a descrição no `title`); situação + motivo; entrada no seco implícita;
  etapa de embarque (passagem/baldeação); escaneamentos com recinto, confiança, motivo e "fora".
- As colunas v2 estão no ORM (`ciclo_de_vida.py`) como `deferred(group='v2')` — as telas antigas e o
  loop antigo não as selecionam; quem mostra usa `undefer_group('v2')`. `CicloDeVidaCE` mapeia a
  `ciclodevida_ciclo_ce`. `test_estagios_ciclo` confere ORM x tabelas do serviço e SQL x Python dos
  estágios.

Em 02/10/2026 (fim do dia; pedido do usuário) — **Fichas OVR** no card e na completude
(`navii/fichas_ovr.py`): fichas do sistema Fichas (bhadrasana2, `ovr_ovrs`) pelo CE do card, pelos outros
CEs da família e — só as ABERTAS (fase Iniciada/Ativa/Suspensa) — pelo contêiner da RVF
(`ovr_verificacoesfisicas.numerolote`; o número do contêiner se repete em outras operações). FMA
(`tipooperacao = 0`) = abandono informado pelo recinto (`fma_update.py`). Card: selos "Ficha OVR aberta" e
"FMA (abandono)" e a lista com link `/bhadrasana2/ovr?id=` (URL montada no servidor). Completude (detalhe):
coluna Ficha OVR e contagem "com Ficha OVR (N com FMA)". Uma consulta por página (CE e contêiner, por
índice); sem filtro por ficha (não pedido — avaliar desempenho antes, se vier).

Em 13/10/2026 (pedido do usuário) — **links para diagnosticar a operação**:
- Filtro `conteiner` (lista; normaliza para letras e dígitos; só vírgula/ponto e vírgula separam, porque o
  número vem com espaço). Com ele, o índice de `itensresumo.codigoConteiner` dirige a busca e a janela vira
  condição por candidato (`_janela_por_candidato`) — sem a tabela derivada da janela inteira.
- A tela abre filtrada pela URL (`_filtros_iniciais`: `conteiner`, demais filtros, `tipo_janela`,
  `data_inicio`/`data_fim`), no lugar dos padrões (sem o porto sugerido). O número do contêiner no card é link
  para essa pesquisa (mesma janela e período, nova aba; `NAVII_URL` vem do servidor).
- Completude (detalhe): o contêiner é link para o NAVI-I filtrado por ele, na janela do desembarque ±1 dia.

Em 14/10/2026 (pedidos do usuário):
- **Filtro `ce`** (lista; só dígitos): o índice de `itensresumo.numeroCEmercante` dirige a busca. Sem
  `data_inicio`/`data_fim`, **sem limite de data** (`_periodo_navii` devolve `(None, None)`); com datas, a janela é
  condição por candidato, como no contêiner. O número do CE no card é link para essa pesquisa (sem período). Na
  tela, com CE informado e as datas vazias, o período fica "sem limite de data"; sem CE, período vazio volta à
  sugestão.
- **Risco histórico por CE** (`risco_navii.py`; ver `virasana/risco_historico/CONTEXTO.md`): selo e memória de
  cálculo no card; filtro `risco` (`alto`, `medio`, `sinal`) por candidato (`_existe` na chave única de
  `risco_historico_ce`). Cenários no `diagnostico_sql.py` (também em `--so-ciclo`).

Em 15/10/2026 (pedido do usuário) — **filtro "Risco CORAD"** (slider com liga/desliga; `corad` = 0..4): matriz
`matriz_risco_corad` (classe `MatrizCorad` do Fichas, `bhadrasana/models/riscocorad.py`, usada pela TABELA Core
— a classe divide a base ORM com os modelos do Fichas e configurá-los quebrava fora do Fichas). Inclusivo: nível 0
= só os importadores que não constam; N = não constam + classificação <= N. Uma condição só, em `criterios_ce`:
"não há linha da raiz do consignatário com classificação acima de N" (chave única de `cod_emp_interv_rad`).
Não consta = raiz fora da matriz, classificação vazia ou consignatário que não é CNPJ. Rótulos do usuário:
Não consta, Alto, Médio, Baixo, Muito baixo — a matriz descreve 3 como "SEM HISTORICO" e 4 como "BAIXO RISCO"
(`RiscoEmpresa` em riscocorad.py). Cenário no `diagnostico_sql.py`.

Em 15/10/2026 (pedidos do usuário):
- **DTA** (`dta_navii.py`; tabela `navii_dtas`, gravada pelo `integracao/dta/dta_manager.py`; definição Core
  parcial, só os campos da declaração): filtro `tem_dta` ('sim'/'nao', por candidato pelo índice de `nr_ce`) e
  filtro `dta` (número → CEs da DTA pela chave única, que dirigem a busca como o filtro `ce`; sem datas, sem limite
  de período; com `ce` junto, os comuns). Card: selo e seção da DTA (número, tipo, registro, desembaraço,
  previsão de chegada, concluído, transportador e CNPJ, valor declarado e moeda, descrição).
- **CORAD no card** (`busca_corad`): classe do consignatário ao lado do nome ("Não consta" sem raiz na matriz).
- **Extrato do CE** (`/navii/extrato?CE=`, `extrato_navii.py`, `templates/navii_extrato.html`, renderizado no
  servidor): o CE uma vez (o card sem contêiner: Mercante, família, navio, risco histórico com memória, CORAD, DTA,
  Fichas) e um card por contêiner (busca por `ce`), com a imagem em destaque (miniatura recortada; clique abre a
  imagem cheia `/image/<id>`), linha do tempo, escaneamentos do ciclo e os **eventos da API Recintos** (4 tabelas,
  uma consulta cada, pelo índice do contêiner; janela: desembarque ou atracação prevista − 5 dias até a última
  etapa + 5 dias, ou + 120 dias com o ciclo em aberto). Todo número de CE das telas do virasana leva ao extrato:
  card do NAVI-I (CE e master), completude, Entregas por CNPJ, Transporte das apreensões — pelo helper global de
  template `url_extrato_ce` (registrado pelo blueprint do NAVI-I; os templates conferem `is defined`). As telas do
  Fichas (bhadrasana2) não foram mexidas.
- `tabelas_opcionais.py`: conferência (com cache) das tabelas que nem todo banco tem (risco, DTA, CORAD, API).
- **Fichas no topo do card** (pedido do usuário): um link "Ficha <id>" por ficha relacionada (cores: aberta,
  encerrada, FMA; dica com número/ano, tipo, fase, via), abrindo `/bhadrasana2/ovr?id=` em nova aba; a lista do fim
  do card saiu. Mesmo rótulo no extrato e na completude. Ao mexer nisso, um recorte no JS apagou as funções do
  risco (os cards não carregavam): `test_prefixo_proxy.test_script_do_navii_so_chama_funcoes_definidas` agora
  confere que toda função chamada no script da página existe.
- **Tempos na contagem** (sugestão do usuário): `api_ces` cronometra (`_cronometrado`, `perf_counter`) a consulta da
  página, a contagem (só na 1ª página) e a montagem dos cards, e devolve `tempos` (segundos; `contagem` None nas
  páginas seguintes). A tela acrescenta à linha da contagem "Tempos: contagem X s · consulta Y s + cards Z s ·
  resposta W s" — a resposta é medida no navegador (inclui rede e desenho dos cards); da 2ª página em diante,
  "última página: …" e a contagem continua a da 1ª. Resposta ≥ 10 s fica em destaque, com dica para restringir o
  filtro.
- **Log de uso** (pedido do usuário, 15/10/2026): `uso_navii.py` grava uma linha por ação no log do servidor com o
  selo `NAVII_USO` (`acao=tela|pesquisa|extrato usuario=... janela= pagina= filtros={json} total= n_cards=
  t_consulta= t_contagem= t_cards= t_total=`; a tela aberta por link traz `link=`; pesquisa inválida traz `erro=`).
  Com `t_total` >= `SEGUNDOS_LENTA` (10 s, o mesmo limite do destaque na tela, passado ao JS pelo template) sai em
  WARNING e termina com `NAVII_LENTA` — a mesma linha, para o total de usos continuar sendo o total de `NAVII_USO`.
  Valores com espaço vão entre aspas (JSON): a linha continua separável por espaço e um texto digitado não abre
  outra linha no log. Logger próprio (`virasana.navii.uso`), sem handler: sai pelo handler raiz do
  `ajna_commons.flask.log`. A linha antiga `busca_ces_navii: ...` (INFO, logger do ciclo de vida) virou DEBUG.
  Exemplos de grep na Referência técnica do manual.

### Sexta medição (16/10/2026, `--amostras --analyze` completo e depois `--so-ciclo`)

- **Nenhuma regressão**: nos cenários das duas rodadas, 159 s contra 162 s em 15/10.
- **Ocultar** (efetiva, porto BRSSZ, página / contagem): 1,6–2,2 s / 2,0–3,6 s — o nível do padrão da tela (2,1 / 2,7).
  Prevista: até 1,2 s.
- **Só com desembarque** (caixa "Incluir os ainda sem desembarque" desmarcada): efetiva **0,8 / 1,1 s** (o ramo da
  atracação prevista custa ~1,2–1,5 s do padrão); prevista 0,7 / 0,7 s. A contagem separada dos "ainda sem
  desembarque" no cabeçalho custaria ~1,1 s a mais por pesquisa (contagem sem a previsão) — não feita; quem quiser o
  número desmarca a caixa.
- **Mais lentos, todos sem porto de atracação** (a tela usa BRSSZ): "até 1 NCM distinto" contagem 11,2 s; "NCM completo"
  página 9,4 s (com o porto, 3,1 s; em 30/09, 995 s); "NCM por prefixo" contagem 9,9 s. Candidatos a otimizar se o log
  de uso (`NAVII_LENTA`) mostrar essas buscas na prática.
- A seção 5 (enriquecimento) não media `busca_ciclos_navii` nem `_itens_com_desembarque`: a `SessaoCapturadora` não
  aceitava iteração ("'QueryCapturada' object is not iterable"). Corrigido (`__iter__`), com teste.

### Quinta medição (15/10/2026, `--analyze --so-ciclo`, filtros novos)

Janela efetiva 30/09–10/10, porto BRSSZ (página / contagem): padrão 2,6 s / 3,0 s; **risco "só alto"** 2,6 / 3,5;
**risco "com algum sinal"** 2,5 / 3,5; **com DTA** 2,6 / 3,4; **CORAD até alto** 1,7 / 2,3 (mais rápido: o critério
entra também no ramo da atracação prevista). Janela prevista: tudo abaixo de 1,2 s. Os filtros novos custam no máximo
~0,5 s na contagem. `risco_historico_ce` é utf8mb4 (key_len 60) e o `itensresumo` latin1: o índice único do risco é
usado mesmo assim (ref "func": o valor latin1 é convertido). Os cenários de CE e de DTA usavam números de exemplo
inexistentes ("Impossible WHERE", resumo dizia ERRO): o diagnóstico passou a usar um CE, um contêiner e uma DTA reais
e recentes (`amostras_reais`) e marca "sem dados" quando o ANALYZE não executa.

### Quarta medição (02/10/2026 tarde): ligação pela família (`--so-ciclo`, com e sem `--sem-familia`)

Logs `com_familia.log` / `sem_familia.log` (61 consultas cada; janela efetiva 17–27/09). Tempos com cache
quente (página seguinte / contagem); a 1ª consulta de cada rodada pagou cache frio (25 s e 8,5 s):

| Cenário (efetiva) | Sem família | Com família |
|---|---|---|
| porto BRSSZ, sem outros filtros | 2,4 s / 2,5 s | 3,0 s / 3,8 s |
| filtros do ciclo (imagem, estágio, recinto), todos os portos | 3,2–3,5 s / 4,8–5,2 s | 4,5–5,0 s / 6,8–7,5 s |
| prevista com filtros do ciclo | 0,9–1,0 s | 1,2–1,3 s |

De onde veio o custo extra (ANALYZE, "entregues", todos os portos): (1) segundo acesso no "sem desembarque
localizado" do ramo da atracação prevista — 218 mil candidatos, ~0,75 s; (2) ramo da família no `UNION`
— ~0,3 s (lê a `ciclodevida_ciclo_ce` inteira, 97 mil linhas, que crescerá); (3) segundo acesso no filtro —
~0,55 s, só na contagem (a página para nos 20 cards). O MariaDB faz o segundo acesso pelo índice de
contêiner do ciclo e depois `uq_ciclo_ce` (eq_ref).

Ajuste feito em seguida: **BL único (tipo 10, ~72% dos CEs) não faz o segundo acesso** (`TIPO_BL_UNICO`) — a
família dele é só ele mesmo, já achado pelo CE do ciclo. Deve cortar (1) e (3) em ~70%. **Medir de novo.**
Conferir no banco que BL único nunca é membro NÃO principal de família:
`SELECT cr.tipoBLConhecimento, cc.papel, cc.principal, COUNT(*) FROM ciclodevida_ciclo_ce cc JOIN
conhecimentosresumo cr ON cr.numeroCEmercante = cc.numeroCEmercante GROUP BY 1, 2, 3`.

**Quinta medição (02/10/2026, 16h; `com_familia2.log`, só 15 das 61 consultas — copiado com a rodada em
andamento e com o `--reprocessa` gravando ao mesmo tempo):** com o índice `ix_cdv_data_ce_conteiner` (criado
pelo usuário) a janela lê só o período (50 mil linhas em 20 ms, antes 366 mil e até 0,6 s); com o atalho do
BL único, o segundo acesso do "sem desembarque localizado" caiu 58%. Página com filtro do ciclo ~3,7 s
(sem família 3,4 s; antes 4,9 s); contagem ~6,2 s (sem família 4,9 s; antes 7,2 s). Sobras corrigidas em
seguida: (1) na consulta principal o MariaDB antecipava a subconsulta da família para `itensresumo`, antes de
juntar o CE, e o atalho se perdia (240 mil execuções na contagem) — o teste do BL único foi para DENTRO da
subconsulta; (2) o ramo da família no `UNION` lia a `ciclodevida_ciclo_ce` inteira (192 mil linhas, dobrou com
o reprocessamento) — agora `STRAIGHT_JOIN` a partir dos ciclos do período. **Medir de novo.**

**Sexta medição (02/10/2026, 16h53; `com_familia3.log`, completa, com o código de 005bcc6):** página com
filtro do ciclo 3,6–3,9 s (sem família 3,2–3,5 s), contagem 6,2–6,9 s (sem família 4,8–5,2 s). O `STRAIGHT_JOIN`
funcionou (ramo da família parte dos 50 mil ciclos do período; 0,45 s, igual, mas não cresce mais com a
tabela). O teste do BL único DENTRO da subconsulta foi pior: o MariaDB deixou de antecipar "tem ciclo" para
`itensresumo` e passou a juntar o CE dos 247 mil candidatos (+0,68 s) — voltou para FORA (como na quinta
medição: só ~17 mil candidatos chegam ao CE na contagem). Custo da família que fica: ~+0,3–0,5 s por página e
~+1,3 s na contagem. **O que mais pesa para o usuário é a contagem na MESMA requisição da 1ª página**
(página + contagem ≈ 10 s com filtro do ciclo): proposta pendente de contagem assíncrona.

**Sétima medição (13/10/2026, local, com o teste do BL único de volta para FORA):** página com filtro do ciclo
3,3–3,6 s, contagem 5,4–6,5 s ("entregues": 5,6 s, contra 6,9 s com o teste dentro); só ~18 mil candidatos
chegam à junção com o CE. Confirma a decisão. Cache frio na 1ª consulta da rodada (55 s) e na 1ª com imagem (18 s).

Achado à parte (vale com ou sem família): o ramo do desembarque efetivo varre o índice
`ix_ciclo_ce_conteiner` do ciclo INTEIRO (366 mil linhas, 0,2–0,6 s; eram 281 mil em 30/09) em vez de ler
só o período — o índice de data não cobre CE e contêiner. Índice sugerido (DBA; medir antes/depois):
`ALTER TABLE ciclodevida_ciclo_de_vida_importacao ADD INDEX ix_cdv_data_ce_conteiner (data_desembarque,
numeroCEmercante, numeroConteiner), ALGORITHM=INPLACE, LOCK=NONE;`

## Tratamento do risco: Fichas pelo NAVI-I (plano aprovado em 17/10/2026)

Fluxo (teoria da análise de risco / compêndio da OMA): análise de risco (NAVI-I) -> tratamento do risco no **Fichas**
(bhadrasana2). O analista que considera um card de risco abre, pelo card, uma Ficha de **Repressão na importação**
(`tipooperacao = 1`); uma aba do NAVI-I acompanha essas Fichas (analista, auditor responsável, supervisor) até a
**verificação física informada**; opcionalmente, as mais avançadas e o histórico.

**O que o Fichas já tem** (código: `bhadrasana/models/ovr.py`, `rvf.py`, `ovrmanager.py`, `rvfmanager.py`; manual:
`bhadrasana2/docs/site`, "Roteiro de uso na importação"):
- Roteiro base: analista abre e encaminha ao chefe -> chefe programa a RVF, atribui auditor e responsável ->
  verificação física informada (texto e fotos) -> providências (bloqueio do CE, laudos, intimações) -> encerramento.
- Os tipos de evento são cadastráveis (`ovr_tiposevento`), mas o workflow se guia pelos **eventos especiais**
  (`EventoEspecial`): 1 Responsável, 2 **RVF = "Verificação física informada"** (o Fichas gera ao informar RVF com 5
  ou mais imagens), 5 Auditor responsável, 6 Mudança de setor, 7 Inspeção não invasiva, 4/8/9 encerramento. Há
  também evento fixo por id no código (`assistente_checkapi`: id 20). **Usar o evento especial, nunca o id** (os ids
  variam entre bases). Bloqueio de CE (28/24), "Verificação Física Agendada" (46) e "Interrupção de despacho" (34)
  não têm evento especial — por nome.
- Regras: todo evento muda a fase e o status (`tipoevento_id`) da Ficha, menos o "meramente informativo"; fase >= 3
  (concluída/arquivada) bloqueia evento comum; evento exige ser o responsável; só perfil Operador/Supervisor cria;
  "Minhas Fichas" mostra os setores do usuário e as Fichas com que ele interagiu. Usuário = CPF em `ovr_usuarios`.
- **Imagens**: a RVF guarda as imagens no Mongo do Fichas (`MONGODB_RISCO`, banco `risco`; imagens das RVFs e anexos
  dos eventos) — outro servidor/banco que o do virasana (`MONGODB_URI`, só as imagens de escaneamento). Ao ir para o
  Fichas, a imagem é COPIADA e ganha outro `_id`. Atenção: `virasana/db.py` tem um `mongodb_risco = conn['risco']`
  no servidor do VIRASANA — não é o banco de imagens do Fichas; a criação pelo NAVI-I abre a sua conexão com
  `MONGODB_RISCO` (como `bhadrasana/main.py`).
- O atalho antigo `/bhadrasana2/nova_inspecaoni/<imagem>` (GET; links em `view_file.html` e na exportação) reaproveita a
  última Ficha do CE de QUALQUER tipo e fase, grava em vários commits, não leva o risco e geraria "Verificação física
  informada" com 5 imagens — a versão nova corrige.

**Decisões (usuário, 17/10/2026)**: (1) a regra de criação fica num módulo NOVO do bhadrasana2 (só aditivo, testado
na suíte do Fichas, uma transação: Ficha, flag, eventos, RVF e imagens), chamado por um endpoint do NAVI-I (POST,
CSRF e usuário do NAVI-I); (2) diálogo de confirmação no card (o que será criado, Fichas já abertas, texto do risco
editável); (3) CE com Ficha de repressão ABERTA: o contêiner entra como nova RVF na mesma Ficha; (4) a aba lista
todas as Fichas de repressão na importação dos setores do usuário, com o selo NAVI-I nas abertas pelo NAVI-I; (5)
flag "NAVI-I" em `ovr_flags` (registro, sem DDL); (6) resumo do risco nas observações (1.000 caracteres) e a memória
de cálculo completa na `descricao_interna` da RVF; (7) responsável inicial = o analista que abriu.

**Medição de 17/10/2026** (`diagnostico_fichas_repressao.py`, 24 meses): 1.376 Fichas tipo 1 — **75% da SACIT**
(conferência física de DTA canal vermelho: eventos 44/45, "conferência física realizada remotamente") e 22% da
**EQRIMP** (a repressão de fato; padrão: abertura -> Bloqueio de CE incluído -> Verificação Física Agendada em
minutos). Evento 22 em 19% (mediana 5,1 d, p90 17,8 d); auditor em 12% (mediana 46 d — não serve de alerta);
bloqueio de CE em 5% (mediana 0,1 d); 78% das RVFs sem imagem e só 26% com contêiner (Fichas da EQRIMP nascem sem
RVF: contêineres da aba vêm também do CE); 152 abertas sem o evento 22, 70% com mais de 1 ano (SACIT, paradas);
ciclo de vida dos 75 contêineres dessas Fichas: 0 localizados (antigas; normalizar o contêiner); observações: 71%
acima de 500 caracteres (resumo NAVI-I curto); atalho antigo usado em 3 Fichas; nenhum evento excluído; flags em uso
são "Operação X". Fim do acompanhamento: evento 22 OU fase >= 3.

**Fases**: 0 diagnóstico das Fichas atuais (`analytic_scripts/diagnostico_fichas_repressao.py`) -> 1 botão "Abrir
Ficha" no card -> 2 aba "Fichas em acompanhamento" (card simplificado por Ficha, com o ciclo de vida de cada
contêiner, últimos eventos e alertas: **saiu sem verificação física**, desembarcado sem bloqueio de CE, previsão
vencida, sem auditor) -> 3 histórico e manual. Ajustes aprovados após a medição (17/10/2026): no diálogo, caixas
opcionais "Bloqueio de CE já incluído" e "Verificação física agendada" (eventos 28/46, por nome); resumo nas observações
com até 500 caracteres (se faltar, amplia-se o campo); fim do acompanhamento = evento especial 2 OU fase >= 3; na aba,
só "saiu sem verificação física" é alerta (auditor e bloqueio viram informação).

**Fase 1 FEITA (17/10/2026)** — "Abrir Ficha" no card:
- Regra no Fichas: `bhadrasana2/bhadrasana/models/navii_ficha.py` (`previa`, `abre_ficha_navii`, `ERROS_DE_NEGOCIO`),
  testes em `bhadrasana2/tests/test_navii_ficha.py` (14, rodar com `--rootdir=tests --ignore=ajna_commons`).
  Armadilhas: o `__init__` de `BaseRastreavel` (OVR, RVF, ImagemRVF, EventoOVR) DESCARTA os argumentos nomeados
  (menos `user_name`) — criar e atribuir campo a campo (`_novo`); o Fichas apaga o arquivo do GridFS ao excluir a
  imagem — cada RVF ganha arquivo PRÓPRIO (nome `navii_rvf<id>_<ordem>_<nome>`), nunca compartilhado; eventos pelo
  `gera_eventoovr(commit=False)` (mesma regra de fase/status), com `create_date` em segundos seguidos (ordem).
- NAVI-I: `ficha_navii.py` (card do contêiner por CE + contêiner, sem limite de data; resumo, descrição da carga,
  memória de cálculo, imagens lidas do GridFS do virasana, recinto atual; conexão própria com `MONGODB_RISCO`; o módulo
  do Fichas é importado na hora — se falhar, 503 só nesta função); rota `GET|POST /navii/api/ficha` (prévia e
  abertura; do cliente vêm só o resumo e as caixas); botão "Abrir Ficha" no cabeçalho do card e diálogo (Esc fecha,
  foco volta ao botão; o botão vira o link "Ficha N"). Log `NAVII_USO acao=ficha`. Testes:
  `test_navii_ficha_card.py` (tabelas REAIS do Fichas no SQLite + dois mongomock). 18 mutantes nos dois lados, todos
  derrubados; roteiro no navegador com servidor de demonstração (nova, acrescentar, já existe).
- Implantação: `git pull` do bhadrasana2 E do virasana; `MONGODB_RISCO` no ambiente do processo web do virasana.

**Fase 2 FEITA (17/10/2026)** — aba "Fichas em acompanhamento" (`/navii/fichas`, API `GET /navii/api/fichas?minhas=
&navii=&verificadas=`; abas "Análise de risco | Fichas em acompanhamento" no cabeçalho das duas telas e entrada no menu
NAVII). Só no virasana e SOMENTE LEITURA:
- `acompanhamento_fichas.py` declara as tabelas do Fichas com as colunas lidas (Core, como `fichas_ovr`; o teste
  `TestColunas` confere contra os models) — não importa os models do bhadrasana. Quais: `tipooperacao = 1`, fase < 3,
  sem evento especial 2 não excluído (com "verificadas", entram também), dos setores do usuário (o dele e os
  subordinados em qualquer nível, pelo `pai_id`, à prova de ciclo no cadastro) OU em que ele abriu / é responsável /
  auditor. Usuário sem cadastro no Fichas: aviso, lista vazia. Limites: 2.000 lidas, 200 cards (aviso).
- Card: fase e status (último evento válido), pessoas (nome; sem cadastro, CPF mascarado), dias desde a abertura,
  setor (nome de qualquer setor), bloqueio de CE (o primeiro, por nome), verificação agendada (a última) ou informada,
  4 últimos eventos (CPF no motivo vira nome ou máscara). Contêineres: os das RVFs (`numerolote` normalizado); sem RVF
  com contêiner, os do CE no Mercante (até 20). Por contêiner, o ciclo pelas buscas do NAVI-I em lote (família do
  CE): estágio, onde está (seco: armazenagem), desembarque, saída SÓ a definitiva (entregue/reembarcado — a saída do
  porto rumo ao seco não é saída), previsão de atracação se ainda sem desembarque, último escaneamento e a última
  imagem. **Alerta (o único)**: contêiner entregue ou reembarcado sem verificação física informada — verificação
  EXCLUÍDA não cala o alerta; os com alerta vêm primeiro.
- Link do contêiner para o card no NAVI-I com CE e janela de ±1 dia do desembarque (efetiva) ou, sem desembarque, da
  previsão (como a completude): sem período a tela usaria o padrão e o contêiner antigo não apareceria.
- Testes: `test_navii_acompanhamento.py` (20; cenário F1–F9 com setores 1>2, 9 e 7<->8; inclui ida e volta do link na
  `/navii/api/ces`). 42 mutantes, todos derrubados (escopo por papel, fase 3, outra flag, neto de setor, reembarcado,
  excluída, limites, link). Roteiro no navegador com servidor de demonstração sob `/virasana`. Log `NAVII_USO
  acao=fichas` (tela e api, com total e alertas).

## Estatísticas de uso — aba gerencial (plano aprovado em 17/10/2026)

Objetivo: medir o uso do NAVI-I e a produtividade por usuário/setor em períodos (ano, semestre, trimestre, mês,
comparando com o anterior), o quanto do total se consegue analisar, o quanto os parâmetros consideram risco, e o
que as pesquisas geraram no Fichas (Fichas abertas, verificação, resultado). Números RELATIVOS, não só absolutos.

**Unidades**: pesquisa = 1ª página de uma busca (id do servidor; as seguintes pertencem a ela); sessão de análise =
pesquisas da mesma aba do navegador sem pausa > 30 min; card carregado (contado no servidor) / **visto** (>= 50% na
tela por 1 s seguido) / **examinado** (visto + interação: imagem, descrição, extrato, risco, menu Filtrar/Ocultar,
Abrir Ficha) / acionado (Ficha). **Refinamento**: a cobertura conta CONTÊINERES DISTINTOS vistos por usuário/setor
no período — refinar e rever não infla; pesquisas medem esforço; a pesquisa anterior da mesma aba fica guardada para
classificar a transição depois.

**Funil**: universo do período (mesma regra da contagem da tela, por janela e dia) -> selecionados pelos parâmetros
(seletividade = total / universo, mediana; pesquisa sem filtro à parte) -> vistos (profundidade = vistos / total) ->
examinados -> Fichas -> verificação informada -> com resultado. Cobertura = contêineres distintos vistos / universo,
pela DATA DO CONTÊINER (desembarque ou previsão), não da pesquisa. Por usuário só números normalizados (vistos por
hora ativa — pausas > 5 min fora —, Fichas por 100 examinados).

**Coleta**: o servidor registra a pesquisa (usuário, setor no momento, janela, período, filtros normalizados, origem,
total, tempos) e os cards enviados (item, posição, data de referência); o navegador (IntersectionObserver) manda em
lotes os segundos visíveis e as interações (`POST /navii/api/uso`, CSRF, `fetch keepalive`) — o servidor só aceita
cards que enviou àquele usuário naquela pesquisa. "Abrir Ficha" leva o id da pesquisa (vínculo exato). Nunca derruba
a tela; o log `NAVII_USO` continua.

**Fichas no período** (só leitura): repressão na importação abertas, NAVI-I x demais, status com as definições do
dashboard de operações do Fichas (sem aberturas / RVF = aguardando saneamento / TG = mercadorias informadas /
concluída / arquivada) e resultados (`ovr_resultados` por tipo e valor; valor do TG). Maturação: "resultado até hoje"
com a idade mediana + comparação por coorte ("resultado em até N dias da abertura"). Atribuição certa (flag NAVI-I /
id da pesquisa) e provável (Ficha aberta no Fichas com CE visto no NAVI-I pelo mesmo usuário até 30 dias antes).

**Decisões (usuário, 17/10/2026)**: (1) armazenamento no **Mongo do virasana** (como os filtros salvos: sem DDL,
formato flexível enquanto as definições são refinadas); (2) números por usuário: cada um vê os seus, o perfil
**Supervisor** do Fichas (`ovr_perfis_usuarios`) vê os usuários dos seus setores (o dele e os subordinados), os demais
só os totais por setor; o manual avisa que o uso é registrado; (3) definições de visto/examinado/sessão aprovadas;
(4) atribuição provável incluída (F3); (5) sem histórico de logs a aproveitar (sistema ainda não em produção).

**Fases**: F0 diagnóstico (custo da contagem do universo por dia; Fichas por período conferidas com o dashboard;
tempo até o resultado, para a coorte) -> F1 coleta, sem tela (vai a produção cedo para juntar dados; um script confere
os números) -> F2 aba "Estatísticas de uso" (funil até examinados, cobertura, seletividade, comparação de períodos,
setor/usuário, planilha) -> F3 Fichas e resultado no período (funil completo, maturação, NAVI-I x demais, atribuição
provável) -> F4 ajustes com dados reais.

**F0 FEITA (17/10/2026)** — `analytic_scripts/diagnostico_estatisticas_uso.py` (reaproveita a carga do
`diagnostico_fichas_repressao.py`; teste `test_diagnostico_estatisticas_uso.py`, 9 mutantes derrubados): universo dia a
dia nas duas janelas com o tempo de cada contagem e a conferência "soma dos dias = período" (avisa DUPLA CONTAGEM);
Fichas tipo 1 por mês (NAVI-I, RVF, só RVF de INI, verificação, TG, resultado, encerradas sem resultado); status do
dashboard x proposto; resultados por tipo; maturação por coorte (em até 30/90/180/365 dias). **Achado de desenho**: o
dashboard de operações chama de "aguardando saneamento" qualquer Ficha com RVF — a Ficha do NAVI-I nasce com a RVF da
inspeção não invasiva, então a estatística usa a VERIFICAÇÃO FÍSICA INFORMADA (evento especial 2); resultado = TG ou
`ovr_resultados`.

**Medição de produção (17/10/2026, 30 dias, sem porto; `estatisticas_uso.log`)**:
- Universo: efetiva 21–49 mil contêineres/dia, 0,8 s/dia (máx. 2,1 s), os 30 dias de uma vez em 11,4 s; prevista
  20–55 mil/dia, 0,15 s/dia, período em 1,9 s. **A soma dos dias passou do período: 1,73x (efetiva) e 1,88x
  (prevista)** — não é erro da busca: sem porto, o manifesto entra em CADA dia em que cai uma das suas escalas (uma
  por porto em que o navio atraca; a efetiva repete menos porque só o ramo dos ainda sem desembarque usa a escala).
  O universo NACIONAL por dia não soma: contar por período ou medir o do porto (`--porto`, acrescentado depois).
- Fichas tipo 1: 1.375 em 25 meses (~55/mês); NAVI-I 0 (ainda não em produção); verificação informada 19%; TG 9%
  (R$ 533 mi); resultado 9% (Perdimento em 122 Fichas, R$ 458 mi; Crédito em 1); arquivadas sem resultado 76% (a
  conferência de DTA da SACIT — ver a medição anterior). **A fase do Fichas confirma a definição**: Concluída = com
  resultado (122 de 123), Arquivada = sem resultado (1.040 de 1.041). O "aguardando saneamento" do dashboard (183)
  mistura 136 sem verificação e 47 com verificação informada — confirma usar a verificação.
- Maturação: verificação com mediana de 5 d e PLATÔ de 18–19% já em 30 d (a coorte de 30 d basta); TG ou resultado
  3% em 30 d, 6% em 90, 9% em 180, 12% em 365 — ainda cresce em 1 ano (resultado: mediana 109 d, p90 250 d);
  encerramento com mediana de 0,2 d (abre e fecha no mesmo dia: conferência).
- Consequências para F2/F3: universo por período (não soma de dias) ou do porto; indicador rápido = verificação em
  até 30 d; resultado por coorte "em até 180 d" (~3/4 do que chega em 1 ano) com aviso de maturação nos períodos
  recentes; a base "NAVI-I x demais" separada por setor (a SACIT é outro processo). Seção 5 (por setor) e `--porto`
  acrescentadas para a próxima rodada.

**Segunda medição (17/10/2026, `--porto BRSSZ`; `estatisticas_uso_brssz.log`)**:
- Com o porto da escala (o padrão da tela), a soma dos dias fica a ~1% do período: efetiva 357.192 x 354.269
  (+0,8%), prevista 341.258 x 337.160 (+1,2%) — o excesso nacional era mesmo das escalas em outros portos. O resíduo
  (~1%) são manifestos com mais de uma escala no próprio porto. Custo: efetiva 0,3 s/dia e 30 dias em 8 s; prevista
  0,08 s/dia e 1,3 s o período. Santos é ~63% do nacional na efetiva (354 mil x 565 mil em 30 dias): o ramo do
  desembarque não usa o porto (decisão 9), mas o ciclo de vida já é dos recintos daqui; quem caiu foi o ramo da
  previsão de outros portos.
- Destino final (`portoDestFinal`) NÃO é preciso para somar; mudaria o sentido do universo (tiraria a carga de
  passagem que escala aqui) e o desalinharia da base das pesquisas (`porto_escala`) — a seletividade (total da
  pesquisa / universo) precisa da mesma base. Pode servir de recorte (destino aqui x passagem), não de universo.
- **Por setor (a descoberta principal)**: SACIT 1.027 Fichas (75%) com 0% de verificação, 0% de TG/resultado e 89%
  encerradas sem resultado — é OUTRO processo (conferência de DTA); EDAIM 22, também sem verificação nem resultado;
  **EQRIMP 299 (22%)**: verificação 85% (84% em até 30 d), TG ou resultado 32% em 90 d, 45% em 180 d, **56% em 1
  ano**, 38% encerradas sem resultado. Os números agregados do tipo 1 não servem de base: a comparação "NAVI-I x
  demais" fica DENTRO do setor (EQRIMP), e "resultado em até 180 d" pega ~80% do que chega em 1 ano.
- Proposta para F2/F3 (a confirmar com o usuário): universo do porto da escala (BRSSZ, configurável), contado
  EXATO por período civil (mês, trimestre, semestre, ano) e guardado depois que o período fecha — não a soma dos
  dias (erro de ~1%); série diária só para gráfico; Fichas por setor, comparação NAVI-I x demais no mesmo setor,
  verificação em até 30 d e TG ou resultado em até 180 d. **Aprovada pelo usuário (17/10/2026).**

**Plano da F2 (aprovado em 17/10/2026)**: aba "Estatísticas de uso" — período civil (mês, trimestre, semestre, ano;
setas), o em curso comparado com o MESMO TRECHO do anterior (1–17/out x 1–17/set); uso pela data da pesquisa,
cobertura pela data do contêiner; blocos: funil (universo -> vistos -> examinados -> Fichas pelo card), uso (com
variação), como se pesquisa (seletividade das pesquisas com critério, profundidade, origem, janela, filtros,
transições), por setor, por usuário (acesso; ordem ALFABÉTICA, sem ranking; coluna principal "vistos por hora"),
série diária, planilha xlsx. Universo: porto da escala, janela EFETIVA para a cobertura, exato por mês e somado para
trimestre/semestre/ano (o resíduo só vem de manifesto com escalas no porto em meses diferentes — medir), guardado
(`navii_uso_universo`; definitivo 15 dias após o fim, antes disso recalculado com mais de 6 h), calculado pela própria
aba mês a mês com progresso (sem cron); por dia só para a seletividade (~1%). Entregas: F2a (cálculo, sem tela) e F2b
(tela, API, planilha, manual).
- Refinamento na F2a: a unidade dos cards é o ITEM do Mercante (`Item.ID` = o card: um contêiner num CE). É estável —
  `resume_mercante.processa_resumo` insere uma vez e atualiza no lugar (só apaga com o CE). Os vistos já são
  guardados por item: o ajuste da coleta previsto no plano NÃO é preciso. A mesma leitura explica o resíduo de ~1% do
  universo por dia: a carga trata a ATUALIZAÇÃO de `EscalaManifesto` como inclusão (o manifesto fica com a escala
  antiga e a nova).

**F2a FEITA (17/10/2026)** — o cálculo, sem tela: `navii/estatisticas_uso.py` (ver a docstring).
- Períodos (`Periodo`, `le_periodo` — códigos '2026-10', '2026-T4', '2026-S2', '2026'; ano >= 2000), `comparacao`
  (em curso: até hoje x o mesmo número de dias do anterior, limitado ao fim dele; o último dia do mês ainda é parcial),
  `pedacos` (o intervalo por mês civil).
- Por pesquisa e agregação (`vistos`, `examinados`, `sessoes`, `minutos`, `transicao` — o script de conferência da
  coleta passou a usá-los daqui —, `Agregado.numeros`: pesquisas, usuários, sessões, horas, enviados, vistos e
  examinados DISTINTOS por item, repetidos, acionados (nova ou acrescentada; "já existia" não) e Fichas novas,
  vistos/hora, examinados/vistos, acionados/examinados, profundidade mediana; `por_dia`).
- Seletividade (`na_base`: com período e `porto_escala` = o porto; `com_criterio`: algum filtro além do porto; dias
  até 92; falta de um dia = "sem universo").
- Universo (`navii_uso_universo`, chave porto|janela|início|fim; `universo` soma os meses e diz o que falta;
  `calcula_pendencias` com orçamento de segundos — ao menos um por chamada; `Pedaco.de_dict` valida o que a tela
  devolve: janela válida, no máximo um mês).
- Cobertura exata: `busca_navii.itens_na_janela` (NOVA, aditiva — "destes itens, quais estão na janela no porto",
  `_janela_por_candidato` + `_criterios_ce`), porque o `monta_query_navii` normaliza o filtro `ce` para dígitos; o
  teste confere que dá o MESMO conjunto da contagem da tela (com CE de exportação e escala em outro porto de
  adversários). Margem de 30 dias antes e depois para as pesquisas que viram.
- Acesso (`acesso_do_usuario`: `ovr_usuarios`/`ovr_setores`/`ovr_perfis_usuarios`, perfil 2 = Supervisor; sem as
  tabelas ou sem cadastro, só ele) e `estatisticas(session, db, cpf, tipo, codigo, porto, agora)` -> o JSON da aba,
  com `pendentes`.
- Diagnóstico: `--meses-universo N` mede a soma dos N últimos meses fechados x os N meses de uma vez (falta rodar
  em produção com `--porto BRSSZ`).
- Testes: `test_navii_estatisticas_uso.py` (24), `test_diagnostico_estatisticas_uso.py` (12); 32 + 17 mutantes
  derrubados.

**F2b FEITA (17/10/2026)** — a aba "Estatísticas de uso":
- Rotas (`views_navii.py`): `GET /navii/estatisticas` (tela), `GET /navii/api/estatisticas?tipo=&codigo=` (401 sem
  usuário, 503 sem o Mongo, 400 período inválido), `POST /navii/api/estatisticas/universo {pendentes}` (até
  MAX_PENDENTES=1000 pedaços, cada um validado por `Pedaco.de_dict`; calcula com ORCAMENTO_UNIVERSO=8 s e devolve os
  `restantes`), `GET /navii/estatisticas/planilha` (xlsx: Resumo, Por setor, Por usuário — só os que quem baixa vê —,
  Por dia). Porto: `BRSSZ` ou a config `navii_porto_estatisticas`. Log `NAVII_USO acao=estatisticas`.
- Módulo: a cobertura passou a ter também os EXAMINADOS no universo (o funil todo na base da data do contêiner);
  `acesso.setores` com nome (o setor sem pesquisas aparecia só como código); `planilha_xlsx`, `variacao`.
- Tela (`navii_estatisticas.html`): tipos Mês/Trimestre/Semestre/Ano e setas (a seguinte desabilitada no período em
  curso), nota do trecho comparado, progresso do cálculo do universo (chama até zerar, no máximo 2 rodadas por
  período), funil (universo -> vistos -> examinados), uso com variação (sem cor de "bom/ruim"), como se pesquisa,
  barras por dia (SVG, sem biblioteca), tabelas por setor e por usuário, "Como os números são contados". Abas nas três
  telas e entrada no menu NAVII.
- Testes: `test_navii_estatisticas_tela.py` (6) + proxy (a tela em TELAS; o teste das funções do JS passou a cobrir as
  três telas — pegou uma chamada indireta). 15 mutantes: 14 derrubados; o vivo é equivalente (o `login_required` já
  barra o sem usuário). Roteiro no navegador (demo sob `/virasana`, como Supervisor): jan/2026 bate com os testes
  (universo 4, vistos 3 = 75%, examinados 1, seletividade 50%), 1º tri soma os meses (6 contêineres), mês em curso com
  o mesmo trecho, planilha conferida, largura de celular sem rolagem lateral.
- Falta: rodar `--meses-universo 3 --porto BRSSZ` em produção; levar coleta + aba à homologação.

**Medição dos meses (17/10/2026, `meses_universo.log`, porto BRSSZ)**: soma dos meses x os 3 meses de uma vez:
efetiva +0,34% (1.085.940 x 1.082.309), prevista +0,66% — a soma dos meses serve. **Mas o TEMPO**: julho e agosto
levaram 131 s e 134 s CADA (cache frio do banco); setembro 12,6 s; os 3 meses de uma vez, depois, 27 s. O virasana
roda no gunicorn SEM `--timeout` (supervisor_virasana.conf): vale o padrão de 30 s — um pedaço de mês frio derrubaria
o worker antes de gravar, e a aba falharia para sempre nos meses antigos. **A F2b NÃO deve ir à homologação assim.**
Opções levadas ao usuário: (A) universo = soma de DIAS, calculados pela aba sob demanda (cada dia cabe na
requisição; +~0,8% sobre o exato, constante entre períodos); (B) meses exatos por um script agendado, a aba só lê;
(C) A + B. `--ate AAAA-MM-DD` no diagnóstico mede dias antigos (frios) para confirmar a (A).

**Dias frios (18/10/2026, `dias_frios.log`, 22–31/05, porto BRSSZ)**: efetiva até 7,5 s por dia (média 3 s; o
período, depois de aquecido, 1,8 s); prevista ~0,1 s; soma dos dias +0,12% do período. Em maio a efetiva é IGUAL à
prevista dia a dia: ainda não há desembarques do ciclo de vida nesse mês — tudo entra pela previsão.
**Opção (A) adotada** (a medição era a condição combinada):
- Universo = SOMA DE DIAS (`universo_dos_dias`, uma consulta `$in` no Mongo; `dias_entre`; o `pedacos` por mês saiu).
- `Pedaco.de_dict` só aceita UM dia: um pedaço maior seria uma consulta longa a pedido do cliente (derrubaria o
  worker de 30 s). ORCAMENTO_UNIVERSO = 5 s (um dia frio que comece no limite termina bem antes dos 30 s);
  MAX_PENDENTES = 2000.
- A cobertura começa no INÍCIO DO REGISTRO do uso (`inicio_do_registro`: a primeira pesquisa gravada): antes dele é
  desconhecida, não zero — e não se calcula universo de dias sem uso (abrir "Ano" no primeiro ano não dispara
  centenas de dias). Período todo antes: cobertura None e sem pendências ("Sem registro de uso neste período");
  começando no meio: `desde` ("Cobertura desde DD/MM, início do registro do uso").
- Testes e mutantes refeitos (F2a 36/36; F2b 14/15, o vivo equivalente); roteiro no navegador das duas mensagens.

**F3 FEITA (18/10/2026)** — Fichas e resultado no período (`navii/resultado_fichas.py`, só leitura; para validar com
usuários):
- Fichas tipo 1 ABERTAS no período (`COALESCE(datahora, create_date)`, o último dia inteiro), por SETOR e ORIGEM:
  'navii' = flag NAVI-I (só a Ficha NOVA: o "acrescentar contêiner" do card não põe a flag); 'provavel' = as demais
  cujo CE foi VISTO (segundos ou clique; card só enviado não conta) no NAVI-I por QUEM ABRIU a Ficha até 30 dias
  ANTES da abertura (depois não conta); 'demais'. Setores com mais Fichas primeiro.
- Por grupo: abertas; verificação física informada (especial 2, não excluída — outro evento especial não conta) em
  até 30 d e TG ou resultado em até 180 d, só entre as abertas há o prazo ou mais (senão "em maturação"; o prazo
  exato conta); até hoje: com verificação, com resultado (TG ou `ovr_resultados`), encerradas sem resultado (fase >=
  3 e sem resultado), em andamento; valor do TG e do PERDIMENTO (tipo 2 — crédito e multa não entram).
- Na aba: seção "Fichas e resultado" (indicadores: Fichas pelo card, prováveis, Fichas pelo card por 1.000 cards
  examinados, total; tabela por setor e origem); JSON `fichas` {atual, anterior, prazos, porMilExaminados}; aba
  "Fichas" na planilha. Sem as tabelas do Fichas: `None` e a tela avisa.
- Testes: `test_navii_resultado_fichas.py` (8; tabelas REAIS do Fichas; adversários: verificação excluída, outro
  evento especial, outra flag, crédito, sem datahora, limites do dia, FMA, CE visto por outro usuário, depois da
  abertura, 32 dias antes, card só enviado, Ficha do card com CE visto antes, encerrada COM resultado, mês em curso).
  21 mutantes derrubados. Roteiro no navegador (demo com as tabelas do Fichas): mar/2026 e out/2026 ("em
  maturação").
- Pontos a validar com os usuários: a regra do "provável" (quem abriu, 30 dias antes); os prazos 30/180; ler a
  comparação dentro do setor; se "encerradas sem resultado" e "em andamento" bastam; valores.

**F1 FEITA (17/10/2026)** — coleta, sem tela:
- `navii/coleta_uso.py` (Mongo do virasana, coleção `navii_uso_pesquisas`, um documento por pesquisa; ver a docstring):
  `registra_pesquisa` (1ª página: usuário, setor no Fichas no momento — `ovr_usuarios`, em cache 10 min —, aba,
  anterior SÓ do mesmo usuário, origem da lista ou "outra", janela, período usado na busca, filtros normalizados sem as
  datas, total, tempos, cards enviados com posição e dias de desembarque/previsão, até 1.000); `registra_pagina`
  (páginas seguintes, só do dono; posição continua; `n_cards` conta todos); `registra_vistos` (só cards ENVIADOS àquele
  usuário naquela pesquisa; segundos com `$max` — reenviar não soma —, teto de 3.600 s por card, NaN/booleano/negativo
  descartados; interações da lista, com `$addToSet`; até 200 por lote); `registra_acao` (Ficha aberta pelo card).
  **Visto** = segundos OU interação (quem clicou viu: examinados ⊂ vistos); **examinado** = com interação.
- `views_navii.py`: a 1ª página de `/navii/api/ces` devolve `pesquisa`; as seguintes a recebem; `POST /navii/api/uso`
  (401 sem usuário, 400 sem JSON, 404 pesquisa de outro/inexistente); "Abrir Ficha" grava a ação na pesquisa. Sem
  Mongo, Mongo fora ou setor ilegível: a tela segue (`_coleta`/`_setor` engolem e registram WARNING).
- `navii.html`: id da aba em `sessionStorage`; `IntersectionObserver` por card (>= 50% do card ou o card cobrindo
  metade da tela; trecho < 1 s não conta; aba escondida fecha os trechos); interações por delegação (imagem,
  descrição, extrato, contêiner, detalhes, menu Filtrar/Ocultar, Abrir Ficha, selo de Ficha); lotes a cada 20 s, ao
  trocar de pesquisa (`reiniciarBusca` -> `encerraPesquisaUso`) e ao esconder/fechar a aba (`fetch keepalive`);
  origem da pesquisa: link (URL), salvo, card, desfazer, tela.
- Conferência: `analytic_scripts/diagnostico_coleta_uso.py --dias 7` (volume, sessões, cards enviados/vistos/
  examinados somados e DISTINTOS por contêiner — a diferença é a repetição do refinamento —, profundidade, segundos por
  card, transições mais/menos critérios, por usuário com CPF mascarado, Fichas). Protótipo das contas da F2.
- Testes: `test_navii_coleta_uso.py` (17), `test_diagnostico_coleta_uso.py` (7), vínculo da Ficha em
  `test_navii_ficha_card.py`; 30 + 9 mutantes derrubados. Roteiro no navegador (demo sob `/virasana`): link -> card ->
  card -> desfazer encadeados pela `anterior`, vistos enviados na troca de pesquisa, 2ª página na mesma pesquisa.
  Observação do roteiro: em tela estreita o card tem ~1.700 px — um card cobre a tela inteira e só ele conta (correto).

## Busca por texto — Meilisearch (plano aprovado em 18/10/2026)

Item 4 do backlog antigo ("linguagem natural"). Caso de uso: achar CEs por um texto digitado — embarcador mesmo com
erro de digitação, e-mail, telefone, endereço ou nome que às vezes só está no `notify` — e ver os cards mais
relevantes (e, entre eles, os mais recentes) primeiro. É busca por PALAVRAS tolerante a erro (não semântica; o modo
híbrido com embeddings do Meilisearch fica para depois, se precisar).

**Desenho**: Meilisearch (binário único, versão fixa, systemd no Rocky Linux 9, só em 127.0.0.1, chave mestra; o
virasana usa uma chave de busca e o indexador uma de escrita; o navegador NUNCA fala com o Meilisearch). Um documento
por CE (chave `numeroCEmercante`): `numeroCEMaster`, `numeroCEmercante`, `codigoEmpresaNavegacao`, `consignatario`,
`descricao`, `embarcador`, `portoDestFinal`, `portoOrigemCarga`, `numConhecimento`, `cnpjNotify`, `notify` + os NOMES
do consignatário e do embarcador quando o campo guarda CNPJ (`laudo_empresas`; os dois campos guardam o CNPJ da
empresa BR ou o nome da estrangeira) + a DATA DE REFERÊNCIA (previsão de atracação da escala em Santos; sem ela, a
primeira escala) para filtrar e desempatar. Pesos: embarcador, nomes, notify, descrição, CNPJs e números; sem
tolerância a erro nos números. Ranking: relevância, depois os mais recentes. O índice é derivado do MariaDB (perdeu,
reconstrói).

**Carga**: inicial dos CEs dos últimos 3 meses; depois, a cada poucos minutos, os CEs novos pelo `ID` (o
`last_modified` do CE NÃO tem índice) e os CEs dos manifestos com escala nova/alterada (`last_modified` indexado de
`Escala`/`EscalaManifesto`). Retenção: 12 meses (apaga os mais antigos). **A janela é sempre por ID**: a
`create_date` do CE é a da ÚLTIMA ALTERAÇÃO (ON UPDATE; ver `risco_historico/CONTEXTO.md`), e o menor ID com
`create_date` recente é um CE antigo alterado. O ID de uma data sai da busca binária na chave primária
(`DiagnosticoTexto.id_na_data`: a criação de um ID é a menor `create_date` do bloco de IDs a partir dele), que o F1
reaproveita na carga inicial e na retenção. Nada de agregar ou varrer `conhecimentosresumo` (~28 M linhas, 13 GB de
dados): só faixas de ID e índices.

**Aba**: `/navii/texto`, o MESMO `navii.html` em "modo texto" (só o campo de texto — busca enquanto se digita, a partir
de 3 caracteres — e o período, padrão previsão de atracação de hoje -15 a +30 dias; o menu Filtrar/Ocultar do card
escondido); a API do virasana consulta o Meilisearch, monta os cards dos CEs da página pelo caminho existente e os
reordena pela relevância; cada card mostra o TRECHO ENCONTRADO ("encontrado em: notify — ..."); rolagem infinita;
coleta de uso com origem "texto". Se o Meilisearch cair, só esta aba avisa.

**Decisões (usuário, 18/10/2026)**: card no mesmo template em modo texto; TODOS os CEs de importação do país; data de
referência = escala em Santos (senão a primeira); alterações do próprio CE depois de indexado FICAM DE FORA por
enquanto (sem re-sync); retenção de 12 meses; indexador em serviço próprio no supervisor (separado do ciclo de vida).
**Sem mascaramento**: o sistema é de uso interno da Receita Federal e os usuários têm acesso a esses dados (nome,
telefone, e-mail, endereço, CPF/CNPJ) — indexados e exibidos como estão; sigilo/LGPD não é requisito de desenho aqui
(o controle de acesso por perfil/setor, quando pedido, é regra de negócio).

**Fases**: F0 diagnóstico só SQL (CEs por mês, preenchimento e tamanho dos campos, e-mail/telefone no notify, CNPJ x
nome em embarcador/consignatário e cobertura dos nomes, escalas por manifesto e Santos, CEs sem escala, cards por CE,
tamanho estimado do índice) -> F1 índice (instalação, `texto_navii.py`, indexador, supervisor, testes com Meilisearch
simulado) -> F2 aba -> F3 homologação e ajustes com os usuários.

**F0 (`analytic_scripts/diagnostico_texto_navii.py`)**: a 1ª rodada em produção (18/10/2026) delimitou a janela pela
`create_date`, pegou o ID 842.381 (CE antigo alterado) e agrupou ~27 M linhas por mais de uma hora, até cair a
conexão (2013). Reescrito no mesmo dia: fronteiras de mês por ID (busca binária), CEs por mês = diferença das
fronteiras (a tabela tem ~28,0 M linhas para AUTO_INCREMENT 28,3 M: quase sem lacunas; a amostra mede), exportação
pelo índice `ix_tipoTrafego` dentro da faixa de IDs, campos/notify/empresas/escalas/cards numa AMOSTRA de blocos
de IDs espalhados (20 mil IDs em blocos de 500), `max_statement_time` de 120 s por conexão (`--limite`; a seção que
estoura registra o erro e o diagnóstico segue). Exemplos de notify inteiros (sem mascaramento). Volume informado
pelo usuário: ~400 mil CEs por mês; o F0 do risco histórico mediu ~13 mil IDs por dia, ~27% de exportação.

**Resultado do F0 (produção, 18/10/2026, ~1 min no total)**:
- Volume: 340–410 mil IDs por mês, ~25% de exportação; importação 250–343 mil por mês (~10,4 mil por dia). Índice
  de 3 meses ~0,95 M CEs; de 12 meses ~3,8 M. Lacunas de ID: 1%. Leitura pela chave: ~8 mil CEs/s (3 meses ~2 min,
  12 meses ~10 min, sem a indexação).
- Texto do resumo por CE: média 331 caracteres (descrição 202, embarcador 63) → índice ~1,2–3,3 GB (3 meses) e
  ~5–13 GB (12 meses), antes dos campos que faltam (abaixo).
- **`notify` e `cnpjNotify` vazios em 100% da amostra**: o parser (`integracao/mercante/mercante.py`,
  `ParseXML._parse_node` faz `node.find(campo)`, só filhos diretos) não alcança `entregaExterior/notify` e
  `entregaExterior/cnpjNotify`. Afeta também a Entregas por CNPJ (`entregas/busca_entregas.py` procura o CNPJ no
  `cnpjNotify`, sempre vazio).
- Consignatário: preenchido em 79%, sempre CNPJ; `laudo_empresas` tem nome para só 20% deles (embarcador com CNPJ:
  21%, nome para 27%) — não serve de fonte de nomes. A consulta em lotes de 500 levou ~1 s por lote.
- Escalas: 3% sem data de referência; 65% dos CEs do país têm escala em Santos (o manifesto lista as escalas do
  navio). Do CE criado até a previsão de referência: p10 0, mediana 7, p90 15 dias.
- Contêineres: 1 em 66%, 2–5 em 17%, mais de 5 em 3%, **nenhum em 14%** (sem card no NAVI-I).

**XMLs do Mercante** (350 arquivos locais em `virasana/mercante`, 14/01/2026, 4.960 CEs de importação; scripts de
análise fora do repositório): `entregaExterior/notify` 77% (média 68 caracteres; nome e endereço, 13% "SAME AS
CONSIGNEE", 8% telefone, 3% e-mail; igual aos dados complementares em 33% dos CEs), `entregaExterior/cnpjNotify` 53%,
`consignatario/dadosComplementaresConsignatario` 77% (nome do consignatário, 39% com endereço, 36% com o CNPJ no
texto), `embarcador/idEmbarcador` 100% (16% com telefone, 39% endereço; quando há `cnpjShipper` — 11% — o resumo
guarda só o CNPJ e perde o nome), `observacoes` 15% (média 148). Nada disso, além do `idEmbarcador`, chega ao
`conhecimentosresumo`; os `dadosComplementaresConsignatario` provavelmente estão na tabela bruta
`conhecimentosEmbarque` (o `_to_dict` manda a chave; a conferir). Movimento A: 44% dos CEs dos arquivos, mas quando
o mesmo lote tem o anterior, A muda a descrição em 6% e o notify/nome em 2% — deixar as alterações de fora custa pouco
no texto. **Decisão (usuário, 18/10/2026)**: o F1 usa só os campos que estão no SQL; a correção do parser fica
PENDENTE (item 5 das Pendências). Os XMLs são apagados depois da carga no banco: não há histórico para reprocessar.

**F1 — índice (18/10/2026, escrito e testado com Meilisearch simulado; falta instalar e ativar)**:
- `navii/mercante_por_id.py`: `limites_dos_ids`, `criacao_do_id`, `id_na_data` (busca binária), `limita_tempo`
  (`max_statement_time` por conexão) — saíram do diagnóstico, que agora os importa.
- `navii/texto_navii.py`: `ClienteMeili` (REST por `requests`, sem SDK: índice, configuração, documentos, apagar por
  filtro, tarefas com `espera`, busca), `cliente_do_ambiente` (`NAVII_MEILI_URL`, `NAVII_MEILI_CHAVE_BUSCA`,
  `NAVII_MEILI_CHAVE_ESCRITA`), `referencia` (regra única, o diagnóstico usa a mesma), `documento_do_ce`,
  `CONFIGURACAO`/`configuracao_difere`, `corpo_da_busca`/`le_resultado`/`busca` (`Resultado`, `Achado` com os
  trechos marcados) e `trecho_html` (escapa o texto do CE; o encontrado vira `<mark>`).
- **Documento** (chave `ce`): `id` (ID do Mercante: retenção e ordem), `manifesto`, `master`, `bl`, `embarcador` e
  `consignatario` (o NOME: o da estrangeira ou o de `laudo_empresas` para o CNPJ — só 20% têm), `embarcadorCnpj` e
  `consignatarioCnpj` (o CNPJ, em campo próprio), `notify`, `cnpjNotify` (vazios até a Pendência 5), `descricao`,
  `transportador`, `origem`, `destino`, `referencia` (segundos da meia-noite UTC; None sem escala com data).
- **Índice**: campos de busca nessa ordem de peso — nomes, notify, descrição, depois CNPJs e números; filtráveis
  `referencia`, `id`, `origem`, `destino`; ranking padrão + `referencia:desc`; SEM tolerância a erro de digitação
  nos CNPJs e números (com tolerância, um CNPJ de 14 dígitos aceitaria 2 dígitos trocados).
- `navii/indexador_texto.py` (`Indexador`, serviço `supervisor_navii_texto.conf`, `autostart=false`): sessão nova por
  volta; (1) CEs novos pelo ID em páginas de 2.000, até 10 por volta, exportação fora; (2) escalas e vínculos
  escala-manifesto com `create_date`/`last_modified` desde a marca menos 5 min → refaz os CEs JÁ indexados (ID entre
  as marcas) desses manifestos; (3) retenção uma vez por dia: `id < id_na_data(hoje - 12 meses)`. Marcas
  (`id_inicial`, `ultimo_id`, `escalas_desde` — hora do BANCO —, `retencao_em`) no índice `navii_texto_estado`,
  gravadas só depois de o Meilisearch confirmar as escritas. Sem marcas ou sem o índice de CEs: carga inicial (cria,
  configura, marca no 1º ID de 3 meses atrás). Configuração conferida a cada volta e atualizada só se mudou.
  `--simula` não escreve nada e funciona sem o Meilisearch (mede a carga no banco). `max_statement_time` de 300 s.
- `navii/MEILISEARCH.md`: instalação no Rocky Linux 9 (binário de versão fixa, usuário próprio, systemd, só
  127.0.0.1, chaves de busca e de escrita, dimensionamento, recarga do zero).
- **Chaves (19/10/2026, na 1ª instalação)**: o `curl -X POST /keys` da 1ª versão do MEILISEARCH.md criava uma chave
  NOVA a cada execução (valor diferente), e o indexador sem `NAVII_MEILI_CHAVE_ESCRITA` caía em HTTP 401. Agora as
  chaves têm UID FIXO (`texto_navii.CHAVES`; o valor é derivado do UID e da chave mestra): `indexador_texto.py
  --chaves` (com `MEILI_MASTER_KEY` só na sessão) cria o que falta, mostra sempre os mesmos valores em linhas
  `export` e lista as chaves "NAVI-I" avulsas para apagar; o indexador confere o ambiente antes de começar
  (`problema_no_ambiente`) e o 401/403 diz qual variável conferir.
- **Ativação (19/10/2026)**: Meilisearch instalado; 1ª volta da carga inicial em produção — marca no ID 27.200.326
  (CEs criados desde 20/07/2026), 20 mil IDs em 7,8 s (com a espera da indexação), 16.428 documentos (18% de
  exportação fora), ~1,19 M IDs na fila (~60 voltas). `--busca "texto" [--de --ate]` consulta o índice pelo mesmo
  `texto_navii.busca` da aba (chave de busca): conferência da API real antes do F2.
- Testes: `test_navii_mercante_por_id.py`, `test_navii_texto.py` (sessão HTTP simulada) e
  `test_navii_indexador_texto.py` (Meilisearch em memória, com falha de tarefa sob comando). Mutantes: 39/39.
- **Ativação**: instalar (MEILISEARCH.md) → `indexador_texto.py --uma-vez --simula` (mede: conferir o tempo das
  consultas de escala — `escalamanifestoresumo.escala` pode não ter índice) → `--uma-vez` → `--situacao` →
  `supervisorctl start navii_texto` → acompanhar a carga (~60 voltas de 20 mil IDs) → conferir buscas pela API.
  Depois, F2 (aba). **Conferido pelo usuário em 19/10/2026** (carga e `--busca`).

**F2 — aba (19/10/2026, escrita, testada e ATIVADA em produção no mesmo dia — conferido pelo usuário)**. Plano
aprovado com uma mudança do usuário: a busca dispara
a partir de **5** caracteres (não 3). Decisão: CE sem item no Mercante aparece numa linha compacta (CE, trecho,
extrato), sem card.
- `views_navii.py`: `GET /navii/texto` (o mesmo `navii.html`, `modo_texto=True`; `?q=` abre buscando; o contexto
  das duas abas sai de `_contexto_da_tela`) e `GET /navii/api/texto?q=&de=&ate=&pagina=`: `texto_navii.busca`
  (`CES_POR_PAGINA_TEXTO` = 10 CEs por página, timeout de 10 s) → `cards_do_texto`: `busca_ces_navii` com o filtro
  `ce` (sem limite de data; até `MAX_CARDS_TEXTO` = 300 cards por página, com aviso se passar) → `monta_cards_navii`
  → ordem da relevância dos CEs (sort estável: dentro do CE, Item.ID decrescente) → `trechos` em cada card (HTML do
  `trecho_html`, já escapado) e `sem_card`. `total` = CEs (estimativa do Meilisearch; ele pagina até 1.000). Sem a
  chave ou com o Meilisearch fora: 503 com mensagem (só esta aba). Cliente de busca: `app.config['navii_meili']`
  (testes, demonstração) ou o do ambiente (`NAVII_MEILI_CHAVE_BUSCA`).
- Coleta de uso: origem nova `texto` (`coleta_uso.ORIGENS`), janela `texto`, filtros `{'texto': ...}`, período (de,
  ate) só quando os dois vêm; `_registra_pesquisa` virou `_registra_na_coleta` (comum às duas abas). As estatísticas
  rotulam origem e janela ("Busca por texto"); a seletividade ignora essas pesquisas (sem porto da base).
- `navii.html` em modo texto: painel de filtros no DOM mas escondido (o script da análise o usa), barra com texto e
  período (padrão `periodo_texto_padrao`: hoje -15 a +30), pausa de 600 ms na digitação (uma pesquisa por pausa; Enter
  e datas disparam já), `estadoBusca.pedido` descarta a resposta de uma busca já trocada, `carregarPaginaTexto` pagina
  por número, `continuaSeVisivel` carrega a próxima quando a página é curta e o fim da lista segue à vista,
  `montaTrechos` no card ("Encontrado em"), `montaLinhaSemCard`, `botaoAcoes` vazio (sem Filtrar/Ocultar). A aba
  "Busca por texto" nas quatro telas (abas com quebra de linha no celular) e no menu NAVII.
- Testes: `test_navii_texto_tela.py` (Meilisearch simulado: ordem contra a dos IDs, trechos escapados, sem_card,
  páginas, 5 caracteres, data inválida, 503, cliente do ambiente, página cortada, coleta) e a tela em
  `test_prefixo_proxy.py` (inclusive as funções do JS). Mutantes 18/18. E2E no navegador (servidor de demonstração com
  busca simulada): pausa na digitação (1 pedido), período no filtro, rolagem, link `?q=` (origem `link`), página curta,
  `<script>` da descrição escapado, celular sem rolagem lateral, aba de análise igual.
- **Ativação**: `NAVII_MEILI_CHAVE_BUSCA` (a do `--chaves`) no ambiente do programa `virasana` do supervisor e
  reiniciar o virasana. Depois, F3: homologação com os usuários.

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
   (`_mesmo_conteiner_do_ciclo`) — o card do filhote (importador real) fica sem ciclo/imagem.
   **Resolvido no card em 02/10/2026** (ciclo procurado na família) e, no mesmo dia, **na janela
   "efetiva" e nos filtros** pela `ciclodevida_ciclo_ce` (ciclos v2). Falta MEDIR o custo em produção
   (`diagnostico_sql.py --so-ciclo` com e sem `--sem-familia`). Ciclos antigos continuam ligados só
   pelo CE gravado.
3. **Ciclos sem CE**: avaliar se vale casar ciclo e Item só pelo contêiner + proximidade de data
   quando o CE do ciclo está vazio (não feito: risco de casar contêiner reutilizado).
4. **Ponte recinto ↔ terminal do manifesto**: abandonada por decisão do usuário (decisão 4). As
   duas linhas por terminal em `ovr_recintos` continuam sem vínculo; só importa se um dia o filtro
   de recinto precisar alcançar contêineres sem ciclo.
5. **Parser da carga do Mercante perde textos do CE** (achado do F0 da busca por texto, 18/10/2026; fica para
   depois por decisão do usuário). Em `integracao/mercante/mercante.py`, o `ParseXML._parse_node` faz
   `node.find(campo)` — só filhos diretos —, então:
   - `entregaExterior/notify` (77% dos CEs nos XMLs) e `entregaExterior/cnpjNotify` (53%) nunca são lidos: as
     colunas `notify`/`cnpjNotify` existem na tabela bruta `conhecimentosEmbarque` e no `conhecimentosresumo`, mas
     estão sempre vazias. Afeta a Entregas por CNPJ (`entregas/busca_entregas.py` procura o CNPJ no `cnpjNotify`);
   - `consignatario/dadosComplementaresConsignatario` (77%: nome, muitas vezes endereço) só vai para a tabela bruta,
     e só se ela tiver a coluna `dadoscomplementaresconsignatario` (o `_to_dict` manda a chave; conferir com
     `SHOW COLUMNS FROM conhecimentosEmbarque LIKE 'dados%'` — se tiver, há histórico no banco);
   - `observacoes` (15%, texto livre) não é lida;
   - quando o embarcador tem `cnpjShipper` (11%), o `idEmbarcador` (nome e endereço) se perde: o resumo guarda só o
     CNPJ.
   O que fazer: ler os caminhos aninhados (`entregaExterior/notify` etc.) e cortar no tamanho das colunas (notify
   200, cnpjNotify 45 — o MariaDB recusa texto maior, erro 1406); colunas novas para dados complementares,
   observações e nome do embarcador na bruta e no resumo (`ALTER TABLE ... ADD COLUMN`, instantâneo no MariaDB 10.11
   quando a coluna vai no fim) e no modelo `Conhecimento`. Vale só daí em diante (os XMLs são apagados depois da
   carga). Qualquer UPDATE de preenchimento no `conhecimentosresumo` tem de fixar `create_date = create_date`, senão
   a coluna (ON UPDATE) marca os CEs como alterados e o risco histórico re-extrai todos. O índice de texto já leva
   `notify` e `cnpjNotify` no documento: passam a valer para os CEs novos sem mudar o índice; os campos novos entram
   no `texto_navii.documento_do_ce`.

Índices sugeridos (conferido em 23/10/2026): `ix_ciclo_ce_conteiner` foi criado em 30/09; `ix_itens_ce_conteiner` não, e não faz falta (`ix_itens_chave` já entra pelo CE):

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
4. Backlog antigo: ~~filtros em linguagem natural~~ — feito como Busca por texto (Meilisearch, 19/10/2026);
   filtros subjetivos por empresa/sócios (sem fonte de dados).

**Backlog priorizado** (22/10/2026): `BACKLOG.md`, na raiz do repositório virasana — junta estas pendências, as do
ciclo de vida e as do risco histórico, com a decisão que falta em cada uma.
