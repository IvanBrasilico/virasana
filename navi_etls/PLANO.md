# NAVI-ETL — plano (rascunho para revisão, 24/10/2026)

Pedido do usuário (24/10/2026): tirar do bhadrasana2 e do virasana as **entradas de API** que carregam as bases da
DUE, da API Recintos e das imagens no MongoDB, e o `virasana_periodic` (o único ETL de PULL), para uma aplicação
nova e separada em `navi_etls/` (raiz do repositório virasana), com estrutura, documentação, qualidade e CI/CD
próprios; depois, modernizar (Python 3.14, FastAPI, driver MySQL melhor). **Copiar, não mover: nada do código
atual muda.** Nada aqui está aprovado.

## 1. O que entra (mapa do código atual)

### Entradas (push): quem chama, rota, o que grava

| Base | Rota (hoje) | Quem chama | Grava em | Código |
|---|---|---|---|---|
| API Recintos | `POST /bhadrasana2/upload_arquivo_json_api/api_json` | scripts do DataLake (JSON) | `apirecintos_acessosveiculo`, `_pesagensveiculo`, `_embarquedesembarque`, `_inspecoesnaoinvasivas`, `_registros_rejeitados` | bhadrasana2 `routes/apirecintos.py` |
| API Recintos | `POST /bhadrasana2/upload_arquivo_json_api/api` (arquivo .json/.zip) | idem | idem | idem |
| API Recintos | `GET /bhadrasana2/api_recintos/maisrecentes` | DataLake (de onde continuar) | — (lê eventos e o controle) | idem |
| API Recintos | `GET /bhadrasana2/api_recintos/imagens_maisrecentes` | DataLake (imagens) | — (lê `fs.files` e o controle, tipo `99`) | idem |
| API Recintos | `POST /bhadrasana2/api_recintos/atualiza_ponteiro` | DataLake | `apirecintos_controle_extracao` | idem |
| DUE | `GET /virasana/integracao/lista_ctrs_sem_due`, `lista_ctrs_com_due`, `lista_dues_sem_detalhes` | ContÁgil (passo 1) | — (lê AcessoVeiculo, InspecaoNaoInvasiva, `fs.files`) | virasana `routes/integracao_app.py` |
| DUE | `POST /virasana/integracao/upload_dues` | ContÁgil (passo 3) | `pucomex_due`, `pucomex_due_itens`, `pucomex_due_conteiner` | idem |
| DUE | `POST /virasana/integracao/atualiza_due_acessoveiculo_e_mongo` | ContÁgil | `apirecintos_acessosveiculo` (DUE) e `fs.files.metadata` | idem |
| DUE | `POST /virasana/integracao/insere_cnpjs` | ContÁgil | `laudo_empresas` | idem |
| DTA | `POST /virasana/integracao/upload_dados_dta` | ContÁgil | `navii_dtas` | idem |
| Imagens | `POST /virasana/api/uploadbson` (multipart, campo `file`) | Avatar (`avatar/utils/dir_monitor.py`) | GridFS (`fs.files`, `fs.chunks`) | virasana `views.py` + `workers/tasks.py` |

### O PULL: `virasana_periodic`

`supervisor_virasana_periodic.conf` → `virasana/scripts/periodic_updates.py --daemon`: a cada ~5 min, só estes
passos (o resto dos imports está morto):

1. `xmli.dados_xml_grava_fsfiles` — XML dos escâneres para `fs.files.metadata.xml`;
2. `processa_xml_mercante.get_arquivos_novos` — **baixa** os XML do Mercante da API do Aniita
   (`http://10.61.8.187:3002`, endereço fixo no código);
3. `processa_xml_mercante.xml_para_mercante` e `resume_mercante.mercante_resumo` — tabelas do Mercante e os
   `*resumo` (as que o NAVI-I lê);
4. `mercante_fsfiles.update_mercante_fsfiles_dias` — Mercante para `fs.files.metadata` (10 dias);
5. `acertos_sql.update` — **não é ETL**: uma correção do Fichas (insere `ovr_eventos` de RVF sem evento).

### Dependências a copiar junto (~6,5 mil linhas; ~5 mil usadas)

- **API Recintos** (bhadrasana2): `models/apirecintos.py` (955: eventos, validação, rejeições, controle,
  `processa_json`, `persiste_df`), `models/apirecintos_normalizacao.py` (149), de `models/__init__.py` só `Base`,
  `BaseRastreavel`, `BaseDumpable`; de `models/ovr.py` só `Recinto`; de `views.py` só `valid_file`.
- **DUE/DTA/CNPJ** (virasana): `integracao/due/due_manager.py` (1.086; os endpoints usam ~700), `due_alchemy.py`
  (105), `integracao/dta/dta_manager.py` (429); do bhadrasana2 `models/laudo.py` só `Empresa` e `get_empresa` (o
  arquivo inteiro puxa `virasana.integracao.bagagens`).
- **Imagens**: `trata_bson` (24) e, do ajna_commons, `models/bsonimage.py` (255).
- **Periódico**: `periodic_updates.py` (149), `integracao/xmli.py` (300), `integracao/mercante/`
  `processa_xml_mercante.py` (192), `mercante.py` (300), `mercantealchemy.py` (522), `resume_mercante.py` (163),
  `mercante_fsfiles.py` (158), `mercante_marshmallow.py` (156), `integracao/acertos_sql.py` (65).
- **Comum** (ajna_commons): `flask/log.py` (90), `utils/sanitiza.py` (89) e, de `flask/conf.py`, só as variáveis de
  ambiente (`SQL_URI`, `MONGODB_URI`/`DATABASE`) — sem o `SECRET` gravado no diretório, o `dominate` e o redis.

**Fora**: as telas (`/upload_arquivo_json_api` com formulário — usa uma `session` indefinida e falha sempre;
`/uploadbson`), `/dues/update` (desativado, responde 404), `/grid_data` e os `*_bulk` da exportação (telas), e as
funções de tela do `due_manager` (`due_summary`, `DueView`...).

### Testes que existem

Só a API Recintos tem testes aproveitáveis (bhadrasana2: `test_apirecintos_normalizacao.py`,
`test_apirecintos_rejeicao.py`, `test_api_recintos_json.py` — este acessa a rede ao ser importado) e, no
ajna_commons, `bsonimage_test.py` e `sanitizar_test.py`. DUE, DTA, CNPJ, `uploadbson` e o periódico **não têm
teste**: os de caracterização são escritos na cópia (F1).

## 2. Achados (no código atual — não se corrige lá; na cópia, na F3)

- **API Recintos, `persiste_df`**: um erro 1062 (chave duplicada) no commit desfaz o **lote inteiro** e o endpoint
  responde sucesso — os eventos novos daquele lote se perdem sem aviso; uma consulta por evento (`is_duplicate`);
  log de ERROR por evento de AcessoVeiculo ("Forçando a barra...").
- `maisrecentes`: `GROUP BY MAX` nas tabelas de eventos inteiras a cada chamada, com sorteio dos recintos parados.
  `imagens_maisrecentes`: agregação no `fs.files` desde 01/01/2026 a cada chamada e uma consulta de recinto por linha.
- **DUE e periódico chamam o próprio virasana por HTTP** (`POST /virasana/grid_data`, `verify=False`) para ler o
  `fs.files` — a cópia dependeria do virasana web no ar. O `/grid_data` aceita consulta arbitrária ao `fs.files` sem
  login (fica no virasana; fora deste escopo).
- **Nenhuma entrada tem autenticação** (os `@login_required` estão comentados; CSRF desligado).
- `/api/uploadbson` com `todir=True` está quebrado (`open(..., 'b')`); o Avatar não usa. O Avatar **apaga o BSON na
  origem** quando recebe `success`: a rota nova só pode responder `success` depois de gravar (como hoje).
- Mercante: `notify`/`cnpjNotify` chegam vazios (P1.4 do `BACKLOG.md`, dado perdido todo dia) — a cópia do parser
  é o lugar natural da correção, quando pedida.

## 3. Estrutura proposta

```
navi_etls/                      raiz no repositório virasana
  CONTEXTO.md                   (este plano vira o contexto, no padrão do NAVI-I)
  pyproject.toml                pacote próprio, versões fixadas (lock)
  qualidade_etls.py, tox.ini    gate no padrão do qualidade_navii.py (pytest, flake8, bandit, radon)
  wsgi_etls.py                  prefixo /navi_etls atrás do Apache
  supervisor_navi_etls.conf, supervisor_navi_etls_periodico.conf
  src/navi_etls/
    app.py, config.py, db.py, log.py, sanitiza.py
    apirecintos/  rotas, modelos, normalizacao, ingestao
    due/          rotas (integracao/*), due_manager (só o usado), due_alchemy, empresa
    dta/          dta_manager
    imagens/      rota uploadbson, bsonimage
    periodico/    servico (o laço do virasana_periodic), xmli, mercante/, acertos_sql
  tests/          caracterização, contrato (amostras reais em tests/dados, anonimizando CPF, CNPJ e nomes)
```

Regras: **as mesmas rotas e contratos** (payload, resposta, códigos HTTP) — quem chama só troca a URL base; **as
mesmas tabelas e coleções** (sem `create_all`, nenhuma DDL nova); o pacote **não importa** `virasana`,
`bhadrasana`, `ajna_commons` nem `ajnaapi` (o gate confere). Documentação: `CONTEXTO.md` aqui e, no ajna_docs
(`docs2.0`), uma página técnica "Integrações (NAVI-ETL)" com rotas, contratos e operação.

## 4. Fases

| Fase | Entrega | Produção? |
|---|---|---|
| F0 | Este mapa; contratos de cada rota tirados do código; amostras reais de payload (anonimizadas) como dados de teste | não |
| F1 | **Cópia fiel** das entradas e do periódico, com imports refeitos e só as adaptações listadas (config e log próprios, `Recinto`/`Empresa` mínimos, `/grid_data` por HTTP trocado pela mesma consulta direta ao Mongo, imports mortos fora); testes de caracterização e de **equivalência** (no venv do virasana, Python 3.9: a mesma entrada na função original e na cópia dá as mesmas linhas, em SQLite e mongomock) | não |
| F2 | Estrutura: app Flask só com essas rotas, gate, tox, CI (decisão 7), supervisor, wsgi, rota de saúde; implantação em **homologação** (`*_HOM`, prefixo próprio) para validar Apache, supervisor e prefixo | homologação |
| F3 | Modernização: Python 3.14, FastAPI com os contratos em pydantic, SQLAlchemy 2, driver MySQL por medição (mysqlclient, PyMySQL, asyncmy, conector MariaDB), pymongo 4 (API assíncrona), uvicorn; correções da seção 2 (1062 com `INSERT ... ON DUPLICATE KEY`/`IGNORE`, sem consulta por evento, ponteiros pela tabela de controle) — os testes de contrato da F1 continuam passando | homologação |
| F4 | **Corte** por fonte, depois de uma **sombra** em homologação (o mesmo envio no antigo e no novo, ou o reenvio das mesmas janelas; comparação das tabelas, como a sombra do ciclo de vida): Avatar, DataLake (API Recintos), ContÁgil (DUE/DTA/CNPJ) e o periódico — este com o antigo **parado antes** (os dois juntos baixariam e gravariam os mesmos XML). Voltar = trocar a URL de volta. O código antigo fica até pedido de remoção | sim |

Difere do pedido em dois pontos, para revisar: a cópia do periódico entra junto na F1 (as dependências se cruzam:
`fs.files`, Mercante) e o corte em produção é **um só**, depois da modernização — a cópia Flask é validada pelos
testes de equivalência e em homologação, sem um primeiro corte que seria refeito na F3.

## 5. Decisões (com a recomendação)

1. **Escopo**: incluir DTA e CNPJs (estão no mesmo fluxo do ContÁgil) e os GET de apoio; deixar de fora as telas,
   `/dues/update`, `/grid_data` e a exportação. *Recomendo assim.*
2. **`acertos_sql`** (correção do Fichas que roda no periódico): copiar com uma opção para desligar — senão ela
   para quando o periódico antigo for desligado. *Recomendo copiar e documentar como "não é ETL".*
3. **`/grid_data` por HTTP** → a mesma consulta direto no Mongo já na F1. *Recomendo.*
4. **Autenticação**: hoje nenhuma. Chave por chamador (cabeçalho), obrigatória a partir do corte — quem chama
   já troca a URL nessa hora. *Recomendo; na F2 atrás de uma opção.*
5. **Corte único depois da modernização, com sombra** (acima) ou um corte da cópia Flask e outro depois. *Recomendo
   o único.*
6. **Python 3.14 no servidor**: como instalar (pacote do sistema, compilado, ou `uv` com Python próprio) — depende
   do sistema operacional e do acesso ao PyPI (proxy, espelho). *Recomendo `uv` (instala o Python e trava as
   versões), se houver acesso.*
7. **CI/CD**: o GitLab interno (`desenvolvimento-interno-catalogo`) tem runner? Se tiver, `.gitlab-ci.yml` na raiz
   do repositório rodando só quando `navi_etls/**` muda; se não, o gate local e o `tox` no servidor, como no NAVI-I.
8. **Apache e porta**: prefixo `/navi_etls` (e `/navi_etls_hom`) e uma porta nova — quem altera o Apache?

## 6. Perguntas (o que não está no código)

- Onde estão os scripts que chamam (DataLake para a API Recintos; ContÁgil para DUE/DTA/CNPJ)? Você consegue trocar
  a URL e, para a sombra, mandar o mesmo envio para os dois?
- As imagens da API Recintos (ponteiro `99`) entram no Mongo por qual caminho — pelo `/api/uploadbson` ou direto?
- Volume típico (eventos por dia e tamanho dos lotes; BSON por dia) — para as metas de desempenho da F3 (dá para
  medir no banco, se preferir).
