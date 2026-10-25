# NAVI-ETL — plano (para a equipe, 24/10/2026)

Três documentos: este (objetivo, decisões, fases), **`INVENTARIO.md`** (endpoints e arquivos — **para a equipe
confirmar**) e **`AMOSTRAS.md`** (coleta, anonimização, gabaritos, sombra, estatísticas, inventário do ambiente).

## 1. Objetivo e princípios

Tirar as **entradas de dados** do bhadrasana2 e do virasana — as rotas que recebem a API Recintos, as imagens, a
DUE, a DTA e os CNPJs — e o **`virasana_periodic`** (o único ETL de PULL, que baixa o Mercante do Aniita) para uma
aplicação nova e separada, `navi_etls/`, na raiz do repositório virasana; modernizá-la; e só então cortar a
produção para ela.

- **Copiar, não mover**: nada do código atual muda. As rotas antigas continuam no ar até um pedido de remoção.
- **Mesmos contratos até o corte** (caminho, corpo, resposta, códigos HTTP): quem chama só troca a URL base e passa
  a mandar a chave. Remodelar contrato é da F5 em diante.
- **Mesmas tabelas e coleções**: a aplicação nova não cria nem altera tabela.
- **Independente**: o pacote não importa `virasana`, `bhadrasana`, `ajna_commons` nem `ajnaapi` (o gate confere).
- **Tudo verificado por um corpus** de amostras reais anonimizadas: gabaritos do código atual, testes de não
  regressão e sombras comparando versões.
- **Versões oficiais da distribuição**, descobertas por um inventário das máquinas antes de escolher.
- Padrão do NAVI-I para documentação (`CONTEXTO.md`, manual no ajna_docs) e qualidade (gate com pytest, flake8,
  bandit e radon; `tox`; sem runner no GitLab).

## 2. Escopo (detalhe em `INVENTARIO.md`)

| Fonte | Rotas | Quem chama |
|---|---|---|
| API Recintos — eventos | E1–E5 (bhadrasana2: `upload_arquivo_json_api/api_json` e `/api`, `api_recintos/maisrecentes`, `imagens_maisrecentes`, `atualiza_ponteiro`) | ContÁgil (DataLake) |
| API Recintos — imagens | E6 (virasana: `api/inspecaonaoinvasiva`) | ContÁgil (DataLake) |
| Avatar | E7 (virasana: `api/uploadbson`) | Avatar |
| DUE, CNPJ, DTA | E8–E14 (virasana: `integracao/...`) | ContÁgil |
| Mercante (PULL) | P1 (`virasana_periodic`, sem o `acertos_sql`) | supervisor |

Também vai, como **referência** (não roda em produção), o código de quem gera o que chega: a parte do Avatar que
monta e envia o BSON, os scripts do ContÁgil (a equipe fornece; são Jython 2.7) e os scripts de envio manual do
virasana — em `navi_etls/fontes/`, cada um com a origem registrada.

## 3. Decisões (usuário, 24/10/2026)

| # | Assunto | Decisão |
|---|---|---|
| 1 | Escopo | DTA e CNPJs entram, com os GET de apoio; telas e rotas desativadas ficam fora |
| 2 | `acertos_sql` (no periódico) | **Não** vai: compensa um bug do Fichas que está sendo corrigido |
| 3 | `/grid_data` chamado por HTTP | Vira a mesma consulta direto no Mongo, já na cópia |
| 4 | Autenticação | Chave por chamador, obrigatória a partir do corte |
| 5 | Corte | Um só, depois da modernização, validado por gabaritos e sombra |
| 6 | Versões | Inventário da estação e do servidor primeiro; as mais novas **oficiais da distribuição**, nada instalado "por fora" |
| 7 | CI/CD | Modelo do NAVI-I (o GitLab não tem runner) |
| 8 | Apache | O usuário altera |
| — | Sombra | Por **amostras coletadas e anonimizadas** no repositório, não por tráfego duplicado |
| — | Contratos | Iguais até a F4; remodelar, se valer a pena, na F5 em diante |

## 4. Fases

### F0 — Preparação (nenhum código de produção)

| Tarefa | Entrega | Quem |
|---|---|---|
| F0.1 Inventário do ambiente | `diagnosticos/inventario_ambiente.py` (**pronto**, 24/10) rodado na estação e no servidor → `ambiente/*.md`; escolha das versões | rodar e escolher: usuário |
| F0.2 Confirmação do inventário | `INVENTARIO.md` revisado: scripts do ContÁgil e outros chamadores, Avatar em uso, envio manual | equipe |
| F0.3 Fontes de referência | `fontes/avatar/`, `fontes/contagil/`, `fontes/virasana/`, com `ORIGEM.md` | equipe fornece o ContÁgil; desenvolvimento copia |
| F0.4 Estatísticas de volume | `diagnosticos/volumes.py` (**pronto**, 24/10) → volumes por dia, lotes, imagens por origem, espaço (RAW × resumo); metas da F3 e números das melhorias da seção 7 | rodar: usuário |
| F0.5 Contratos | `CONTRATOS.md`: por rota, o corpo, a resposta, os códigos, o que grava e o que acontece no reenvio — tirados do código e conferidos nas amostras | desenvolvimento; equipe revisa |
| F0.6 Corpus | coleta (originais fora do repositório) → `anonimiza.py` → `confere_anonimizacao.py` → `amostras/` + `MANIFESTO.md` | coleta: equipe e usuário; ferramentas: desenvolvimento |
| F0.7 Gabaritos | o código **atual** sobre o corpus, em bases vazias → `tests/esperado/` | desenvolvimento |

Pronto quando: inventário confirmado, versões escolhidas, corpus no repositório com a conferência passando e
gabaritos gerados.

### F1 — Cópia fiel (Flask, com o Python e as bibliotecas de hoje)

Os arquivos do `INVENTARIO.md` (seção 2), com só as adaptações listadas lá; testes copiados; testes de contrato e
de gabarito sobre o corpus (SQLite e mongomock); teste de independência (nenhum import proibido).
Pronto quando: gate passando e gabaritos reproduzidos.

### F2 — Estrutura e homologação

Aplicação Flask só com essas rotas; prefixo `/navi_etls` (e `/navi_etls_hom`) e porta nova — o usuário altera o
Apache; supervisor para a web e para o periódico; gate `qualidade_etls.py` e `tox` no modelo do NAVI-I;
`CONTEXTO.md`; página técnica "Integrações (NAVI-ETL)" no ajna_docs (`docs2.0`); rota de saúde e a verificação no
`saude_navi`; a chave por chamador atrás de uma opção; implantação em homologação e **sombra** com o corpus.
Pronto quando: sombra sem diferença em homologação.

### F3 — Modernização (versões da F0.1)

Python mais novo oficial da distribuição, FastAPI com os contratos em pydantic, SQLAlchemy 2, driver MySQL
escolhido por medição (mysqlclient, PyMySQL, asyncmy, conector MariaDB — o que a distribuição suportar), pymongo,
servidor ASGI. Correções da seção 6 e, se aprovadas, as melhorias da seção 7, **com os mesmos contratos**. Pronto quando: gabaritos (as mudanças de propósito
explicadas), sombra e metas de desempenho da F0.4.

### F4 — Corte em produção (uma vez, fonte por fonte)

Ordem proposta, do mais simples ao mais delicado: (1) Avatar (E7); (2) imagens da API Recintos (E4–E6); (3)
eventos da API Recintos (E1–E3, E5); (4) ContÁgil — DUE, CNPJ, DTA (E8–E14); (5) o periódico, com o antigo
**parado antes** (os dois juntos baixariam e gravariam os mesmos XML). Em cada fonte: chave configurada, URL trocada,
acompanhamento pelo health check por 24 a 48 h; voltar é trocar a URL de volta. Desligar o `virasana_periodic`
também desliga o `acertos_sql`: a correção do Fichas precisa estar em produção antes do passo 5.

### F5 em diante — Remodelagem (por pedido)

Contratos novos onde valer a pena (gravação registro a registro, ponteiro único por fonte, imagem sem duplicata...),
sempre com o corpus e os gabaritos. A correção do parser do Mercante (`notify`/`cnpjNotify` vazios, P1.4 do
`BACKLOG.md`) não muda contrato e pode entrar antes, se pedida — é dado perdido todo dia.

## 5. Quem faz o quê

- **Equipe e usuário**: os scripts do ContÁgil (fornecer, a opção "só salvar" para a coleta, URL e chave no corte);
  BSON de um Avatar em uso; rodar o inventário, as estatísticas e as coletas na estação e no servidor; guardar a
  chave da anonimização; revisar o texto livre marcado pela ferramenta; Apache e supervisor na implantação.
- **Desenvolvimento**: a cópia, os testes, as ferramentas (inventário, estatísticas, coleta, anonimização,
  conferência, gabarito, sombra), a documentação e o roteiro de cada implantação.

## 6. Achados no código atual (não se corrigem lá; na cópia, F3)

- **API Recintos, `persiste_df`**: um erro 1062 (chave duplicada) no commit desfaz o **lote inteiro** e a rota
  responde sucesso — os eventos novos do lote se perdem sem aviso; uma consulta por evento (`is_duplicate`); log de
  ERROR por evento de acesso ("Forçando a barra...").
- `maisrecentes`: `GROUP BY MAX` nas tabelas de eventos inteiras a cada chamada, com sorteio dos recintos parados;
  `imagens_maisrecentes`: agregação no `fs.files` desde 01/01/2026 a cada chamada e uma consulta por recinto.
- **E6**: o reenvio do mesmo evento grava a imagem de novo (sem conferência de duplicata); recinto fora de
  `ovr_recintos` vira `RECINTO_NAO_ENCONTRADO` na unidade ALFSTS; erro devolve a mensagem interna.
- **E2 e E1 têm formatos diferentes** (`partes_resultado` objeto num, lista no outro).
- **E7**: com `todir=True` está quebrado (`open(..., 'b')`); o Avatar não usa. O Avatar **apaga o BSON na origem**
  quando recebe `success`: a rota só pode responder `success` depois de gravar (como hoje) — teste de contrato.
- **DUE e periódico chamam o próprio virasana por HTTP** (`/grid_data`, `verify=False`); o `/grid_data` aceita
  consulta arbitrária ao `fs.files` sem login (fica no virasana; fora deste escopo).
- **Nenhuma entrada tem autenticação**; o endereço do Aniita está fixo no código.

## 7. Melhorias anotadas (não mudam contrato; fase a decidir)

### 7.1 O XML do escâner lido na hora do BSON (dispensa o passo `dados_xml_grava_fsfiles`)

- **Hoje**: o E7 grava no GridFS os pares imagem + XML que o Avatar monta no mesmo BSON (mesmo `metadata`). Depois,
  a cada ~5 min, o periódico (`xmli.dados_xml_grava_fsfiles`) procura até 10.000 imagens **sem `metadata.xml`**
  escaneadas nos últimos 5 dias, acha o XML pelo contêiner (upload em ±1 h) ou pelo nome do arquivo, decodifica
  (chardet e mais cinco codificações) e grava `metadata.xml`. As imagens da API Recintos (E6) **nunca têm XML**:
  voltam à busca em todas as voltas, por 5 dias.
- **Proposta**: no E7, o par já está no `BsonImageList` — ler o XML com o **mesmo** `xml_todict` e gravar a imagem
  já com `metadata.xml`. O passo do periódico fica como rede de segurança (par em BSON separado, XML ilegível)
  enquanto se mede; com a medida em zero, sai. A imagem do E6 deixa de ser candidata.
- **Cuidados**: o dicionário gravado tem de ser idêntico (o gabarito confere o `fs.files` final); o casamento
  imagem ↔ XML dentro do BSON pelas mesmas regras de hoje; o custo passa para a recepção (medir nos BSON grandes).
- **Medida** (`diagnosticos/volumes.py`, seção I): % das imagens do Avatar com XML por dia; candidatas do passo
  por origem a cada volta.

### 7.2 O Mercante num passo só (sem as tabelas RAW)

- **Hoje**: dois passos. `processa_xml_mercante` grava cada nó do XML numa tabela **RAW** (colunas texto, só
  acrescenta: `conhecimentosEmbarque`, `manifestosCarga`, `itensCarga`, `NCMItemCarga`, `ConteinerVazio`,
  `EscalaManifesto`, `exclusoesEscala`, `escala`). Depois `resume_mercante` lê da RAW o que passou do
  `controleresumo` e aplica nas `*resumo` — num lote, primeiro todas as inclusões, depois as atualizações, depois as
  exclusões, com uma consulta por linha. As RAW crescem sem limite.
- **Proposta**: ler o XML, converter os tipos e aplicar I/A/E direto nas `*resumo`, **na ordem do arquivo**, em lote
  (gravação pela chave natural). As RAW e o `controleresumo` deixam de ser necessários.
- **O que se perde**: as RAW são hoje o **único histórico** dos movimentos (os XML são apagados depois da carga).
  Alternativa barata: guardar o XML original **compactado** (arquivo ou GridFS, por um prazo a definir) — permite
  reprocessar, inclusive depois da correção do parser (`notify`, P1.4 do `BACKLOG.md`).
- **Cuidados**: confirmar com a equipe quem mais lê as RAW (no código, só o resumo; pode haver consultas avulsas ou
  extrações); as RAW que existem ficam (apagar é decisão do usuário e do DBA, depois); o gabarito e a sombra
  comparam as `*resumo` (diferença só onde a ordem do arquivo mudar o resultado, e explicada).
- **Medida** (`volumes.py`, seções E e M): MB das RAW × `*resumo`; linhas RAW por dia e por movimento (% de
  atualização e exclusão); o que falta resumir.

*Recomendação*: as duas na **F3** (não mudam contrato e simplificam justo o código que seria modernizado), cada uma
com a sua sombra; a decisão sai depois dos números do `volumes.py`.

## 8. Riscos e cuidados

- Dois periódicos ao mesmo tempo: nunca (passo 5 da F4).
- Modelos copiados conferidos contra o `SHOW CREATE TABLE` da produção (teste), para a cópia não divergir da tabela.
- Cópias de referência (Avatar, ContÁgil) envelhecem: `ORIGEM.md` com o commit ou a data; reconferir quando a fonte
  mudar.
- Dados reais: originais e chave da anonimização fora do repositório; conferência automática antes de cada commit
  de amostra.
- Se a distribuição não oferecer o Python 3.14, o alvo é a versão mais nova que ela oferece.
