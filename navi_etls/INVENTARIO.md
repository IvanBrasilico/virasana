# NAVI-ETL — inventário de endpoints e arquivos (para confirmar com a equipe, 24/10/2026)

Lista do que será **copiado** para `navi_etls/` (nada do código atual muda) e do que a equipe precisa fornecer.
Plano em `PLANO.md`; amostras, anonimização, sombra e estatísticas em `AMOSTRAS.md`.

## 1. Endpoints

As rotas novas mantêm o **mesmo caminho e o mesmo contrato** (corpo, resposta, códigos HTTP); muda só o prefixo
(`/bhadrasana2` ou `/virasana` → `/navi_etls`). Remodelar contrato fica para a F5 em diante.

| # | Método e rota atual | Quem chama | Entrada | Resposta | Grava em |
|---|---|---|---|---|---|
| E1 | `POST /bhadrasana2/upload_arquivo_json_api/api_json` | ContÁgil (DataLake) | JSON `{"partes_resultado": [{"eventos": [{"jsonOriginal", "dadosTransmissao"}]}]}` | 200 / 500 `{"msg"}` | `apirecintos_acessosveiculo`, `_pesagensveiculo`, `_embarquedesembarque`, `_inspecoesnaoinvasivas` (tipos 1, 3, 4, 25); `apirecintos_registros_rejeitados` |
| E2 | `POST /bhadrasana2/upload_arquivo_json_api/api` | ContÁgil (DataLake) | multipart `file`: `.json` (`partes_resultado` é **um objeto**, não lista) ou `.zip` (`tipoEvento.txt` + `json.txt`) | 200 / 404 / 500 `{"msg"}` | idem E1 |
| E3 | `GET /bhadrasana2/api_recintos/maisrecentes` | ContÁgil (DataLake) | — | `{"msg", "maisrecentes": [{"tipoEvento", "codigoRecinto", "dataHoraTransmissao"}]}` | — (lê eventos e o controle; sorteia os recintos parados) |
| E4 | `GET /bhadrasana2/api_recintos/imagens_maisrecentes` | ContÁgil (DataLake, imagens) | — | `{"msg", "maisrecentes": [{"codigoRecinto", "dataHoraTransmissao"}]}` | — (lê `fs.files` e o controle tipo `99`) |
| E5 | `POST /bhadrasana2/api_recintos/atualiza_ponteiro` | ContÁgil (DataLake) | JSON `{"codigoRecinto", "tipoEvento", "dataFim"}` | 200 / 400 / 500 `{"msg"}` | `apirecintos_controle_extracao` |
| E6 | `POST /virasana/api/inspecaonaoinvasiva` | ContÁgil (DataLake, **imagens da API Recintos**); scripts de envio manual | multipart: campo `json` = `{"jsonOriginal": {"dataHoraOcorrencia", "dataHoraTransmissao", "codigoRecinto", "listaConteineresUld"}}` + arquivo `imagem` (.jpg/.jpeg) | 201 `{"message", "file_id"}` / 400 / 500 `{"error"}` | GridFS (`fs.files` com `metadata`: data, contêiner, recinto e unidade pela tabela `ovr_recintos`) |
| E7 | `POST /virasana/api/uploadbson` | Avatar (`avatar/utils/dir_monitor.py`) | multipart `file` (.bson) e `sync` | sempre 200 `{"success", "mensagem", "taskid"}` | GridFS (pares imagem .jpg + XML do escâner) |
| E8 | `GET /virasana/integracao/lista_ctrs_sem_due` | ContÁgil (DUE, passo 1) | `inicio`, `fim`, `codigos_recintos` | `{"success", "total_registros", "periodo", "codigos_recintos", "data"}` | — |
| E9 | `GET /virasana/integracao/lista_ctrs_com_due` | ContÁgil | idem | idem | — |
| E10 | `GET /virasana/integracao/lista_dues_sem_detalhes` | ContÁgil | idem | idem (`data` = números de DUE) | — |
| E11 | `POST /virasana/integracao/insere_cnpjs` | ContÁgil | JSON `[{"cnpj", "nome"}]` | 200 / 400 / 500 | `laudo_empresas` |
| E12 | `POST /virasana/integracao/atualiza_due_acessoveiculo_e_mongo` | ContÁgil | JSON `{"dues": [...], "escaneamentos_sem_due": [...]}` | 200 / 400 / 500 | `apirecintos_acessosveiculo` (DUE) e `fs.files.metadata` |
| E13 | `POST /virasana/integracao/upload_dues` | ContÁgil | JSON `{"dues": [...], "itens_dues": [...]}` | 200 / 422 (parcial) / 500 | `pucomex_due`, `pucomex_due_itens`, `pucomex_due_conteiner` |
| E14 | `POST /virasana/integracao/upload_dados_dta` | ContÁgil | JSON `{"registros": [...]}` | 200 / 400 / 500 | `navii_dtas` |
| P1 | `virasana_periodic` (supervisor, laço de ~5 min) — **PULL** | — | API do Aniita (`http://10.61.8.187:3002`: `consultaArquivos`, `download`) | — | `fs.files.metadata.xml`; tabelas do Mercante e `*resumo`; `arquivosbaixados`; `fs.files.metadata` (Mercante) |

**Imagens da API Recintos:** entram pelo **E6** (no virasana). As rotas do bhadrasana2 (E1 e E2) gravam só os
eventos no MySQL, inclusive o da inspeção (tipo 25, sem a imagem); o E4 e o E5 com tipo `99` dizem ao script de
onde continuar a baixar as imagens. *A confirmar no script do ContÁgil.*

**Fora** (telas ou desativados): `/upload_arquivo_json_api` (formulário; usa uma `session` indefinida e falha sempre),
`/inspecaonaoinvasiva` e `/uploadbson` (formulários), `/dues/update` (responde 404), `/grid_data`, `*_bulk` da
exportação, `acertos_sql` do periódico (decisão do usuário: compensa um bug do Fichas que está sendo corrigido).

## 2. Código a copiar (destino em `navi_etls/src/navi_etls/`)

| Destino | Origem | Linhas | O que vai |
|---|---|---|---|
| `apirecintos/rotas.py` | bhadrasana2 `bhadrasana/routes/apirecintos.py` | 477 | E1–E5 (sem a tela) |
| `apirecintos/modelos.py` | bhadrasana2 `bhadrasana/models/apirecintos.py` | 955 | eventos (4 tipos), validação, rejeições, controle, `processa_json`, `persiste_df` |
|  | bhadrasana2 `bhadrasana/models/__init__.py` | ~70 de 309 | `Base`, `BaseRastreavel`, `BaseDumpable` |
|  | bhadrasana2 `bhadrasana/models/ovr.py` | ~20 de 794 | `Recinto` (`ovr_recintos`) |
| `apirecintos/normalizacao.py` | bhadrasana2 `bhadrasana/models/apirecintos_normalizacao.py` | 149 | inteiro |
| `imagens/inspecao.py` | virasana `virasana/routes/inspecaonaoinvasiva_app.py` | 107 | E6 (sem a tela) |
| `imagens/bson.py` | virasana `virasana/views.py` (`api_upload`, `valid_file`) e `virasana/workers/tasks.py` (`trata_bson`) | ~75 | E7 |
| `imagens/bsonimage.py` | ajna_commons `ajna_commons/models/bsonimage.py` | 255 | inteiro |
| `integracao/rotas.py` | virasana `virasana/routes/integracao_app.py` | 591 | E8–E14 |
| `integracao/due_manager.py` | virasana `virasana/integracao/due/due_manager.py` | ~700 de 1.086 | só o que os endpoints usam (fora `due_summary`, `DueView` e outras de tela) |
| `integracao/due_alchemy.py` | virasana `virasana/integracao/due/due_alchemy.py` | 105 | inteiro |
| `integracao/empresa.py` | bhadrasana2 `bhadrasana/models/laudo.py` | ~30 de 121 | `Empresa`, `get_empresa` (o arquivo inteiro puxa `virasana.integracao.bagagens`) |
| `integracao/dta_manager.py` | virasana `virasana/integracao/dta/dta_manager.py` | 429 | inteiro |
| `periodico/servico.py` | virasana `virasana/scripts/periodic_updates.py` | 149 | o laço e os 4 passos (sem `acertos_sql` e sem os imports mortos) |
| `periodico/xmli.py` | virasana `virasana/integracao/xmli.py` | 300 | inteiro |
| `periodico/mercante/` | virasana `virasana/integracao/mercante/`: `processa_xml_mercante.py`, `mercante.py`, `mercantealchemy.py`, `resume_mercante.py`, `mercante_fsfiles.py`, `mercante_marshmallow.py` | 192 + 300 + 522 + 163 + 158 + 156 | inteiros |
| `comum/log.py`, `comum/sanitiza.py` | ajna_commons `flask/log.py`, `utils/sanitiza.py` | 90 + 89 | inteiros |
| `comum/config.py` | ajna_commons `flask/conf.py` | ~15 de 80 | só as variáveis de ambiente (`SQL_URI`, `MONGODB_URI`/`DATABASE`); sem o `SECRET` gravado no diretório, o `dominate` e o redis |

Total: ~6.500 linhas copiadas, ~5.000 usadas. **Adaptações já decididas** (as únicas, na F1): imports refeitos para o
pacote novo; `config` e `log` próprios; `Recinto` e `Empresa` mínimos; as duas chamadas HTTP ao próprio virasana
(`POST /virasana/grid_data`, em `due_manager.raspa_container` e `mercante_fsfiles`) trocadas pela mesma consulta
direto no Mongo; a pasta dos XML do Mercante (`mercante/`, relativa ao diretório) por configuração.

**Testes a copiar**: bhadrasana2 `tests/test_apirecintos_normalizacao.py` e `tests/test_apirecintos_rejeicao.py`;
ajna_commons `tests/bsonimage_test.py` e `tests/sanitizar_test.py`. O `tests/test_api_recintos_json.py` do
bhadrasana2 vira teste de contrato com amostra anonimizada. DUE, DTA, CNPJ, E6, E7 e o periódico não têm teste hoje.

## 3. Código de quem GERA o que chega (cópia de referência em `navi_etls/fontes/`; não roda em produção)

Cada pasta com um `ORIGEM.md`: repositório, commit ou data, responsável, o que foi copiado e por quê.

| Destino | Origem | O que vai |
|---|---|---|
| `fontes/avatar/` | repositório **Avatar** (`origin_lab`, commit `d8e5781`) | a geração do BSON: `avatar/utils/bsonimage.py` (150), a exportação em `avatar/utils/utils.py` (432; a função que monta o `BsonImageList`), `avatar/utils/le_xmlv2.py` (192, XML dos escâneres), `avatar/utils/dir_monitor.py` (195, envio ao E7), `avatar/utils/dir_utils.py`, `avatar/utils/conf.py`, `avatar/models/models.py` (192, banco local: fontes e contêineres escaneados), `avatar/cli/avatar_cli.py` e `avatar_upload_bson.py`. Fora: a interface gráfica (`tkgui/`). Exemplos: `tests/xmls_exemplo/` (124 KB) e uma seleção de `tests/pastas_exemplo/` (são 114 MB, por recinto) |
| `fontes/contagil/` | **a fornecer pela equipe** (scripts do ContÁgil, Jython 2.7) | (a) eventos da API Recintos: DataLake → E3, E1/E2, E5; (b) imagens da API Recintos: DataLake → E4, E6, E5 com tipo `99`; (c) DUE: E8–E10 → ReceitaData/Pucomex → E11–E13; (d) DTA → E14 |
| `fontes/virasana/` | virasana `virasana/scripts/inspecaonaoinvasiva.py` e `Enviar imagens TRANSBRASA para o AJNA.py` (iguais, 90 cada); `virasana/scripts/imagensupload.py` e `virasana/workers/dir_monitor.py` | envio manual de imagens (E6) e de BSON (E7) |
| (só documentado) | API do Aniita (Mercante) | `consultaArquivos` e `download`, os parâmetros e o formato dos nomes dos arquivos |

Para confirmar com a equipe: a lista de scripts do ContÁgil (e se há outro chamador destas rotas); se o Avatar
ainda roda e em quais recintos; se o envio manual (`fontes/virasana/`) ainda é usado; **quem lê as tabelas RAW do
Mercante** (`conhecimentosEmbarque`, `manifestosCarga`, `itensCarga`, `NCMItemCarga`, `ConteinerVazio`,
`EscalaManifesto`, `exclusoesEscala`, `escala`) além do resumo — consultas avulsas, extrações para o DataLake
(melhoria 7.2 do `PLANO.md`).
