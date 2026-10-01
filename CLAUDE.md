# virasana

## O que é este repositório

`virasana` é o módulo de imagens do sistema AJNA (Vigilância Aduaneira): recebimento, exibição e interação com imagens de escaneamento (raio-X de contêineres/cargas), integração com bases externas (Mercante, DUE, API Recintos, bagagens/viajantes, risco/conformidade) e processamento assíncrono via Celery.

**Também está em produção, em uso ativo** — mesma restrição de estabilidade do sistema irmão [bhadrasana2 (Fichas)](../bhadrasana2/CLAUDE.md): não arriscar quebrar produção com mudanças não solicitadas.

## Relação com bhadrasana2 (Fichas)

`virasana` e `bhadrasana2` são projetos irmãos do mesmo ecossistema AJNA e **dependem um do outro via `sys.path`** (não via pip), não via um pacote publicado:

- `bhadrasana2` importa `virasana.integracao.mercante.mercantealchemy` (ver `tests/app_creator.py` do bhadrasana2).
- `virasana/wsgi_production.py` e `virasana/main.py` incluem `../bhadrasana2` (e `../bhadrasana`, `../ajna_api`) no `sys.path`, embora o uso direto de `bhadrasana.models.db_session` esteja comentado em `main.py` no momento.

Ao mexer em modelos/tabelas compartilhados (ex.: `mercantealchemy`), considerar o impacto nos dois repositórios.

## Stack

- Flask 2.3.3, SQLAlchemy 1.4.54, pandas 2.1.4 (ver [setup.py](setup.py)).
- Processamento de imagem/visão computacional: `opencv-python-headless`, `Pillow`, `scikit-learn`, `scipy`, `matplotlib`.
- Persistência: MariaDB 10.11 (não é MySQL: medir queries com `ANALYZE FORMAT=JSON`, não `EXPLAIN ANALYZE`) via SQLAlchemy (`db.py` → `mysql`); tabelas do Mercante são grandes (ncmitemresumo ~150M linhas, itensresumo ~50M, conhecimentosresumo ~22M) — toda busca precisa partir de um índice seletivo + MongoDB (`mongodb`, `mongodb_risco`).
- **Celery** para tarefas assíncronas/periódicas (`virasana/workers/tasks.py`, `dir_monitor.py`; scripts de start em [celery.sh](celery.sh)/[celery_periodic.sh](celery_periodic.sh), configs em `supervisor_virasana_celery.old`/`supervisor_virasana_periodic.conf`).
- Deploy via gunicorn ([Procfile](Procfile)); WSGI de produção em [wsgi_production.py](wsgi_production.py), de teste em [wsgi_teste.py](wsgi_teste.py).

## Dependências de repositórios irmãos

Assim como o bhadrasana2, este repo depende de projetos irmãos referenciados via `sys.path` (não instalados via pip):

- `ajna_commons` (`../ajna_docs/commons`) — conf, login, logging.
- `../bhadrasana2` — ver seção acima.
- `../ajna_api` — API Recintos.
- Documentação/instalação centralizada no projeto principal **AJNA** (não presente neste workspace).

## Arquitetura / mapa de módulos

- `virasana/main.py` — ponto de montagem: cria `app`, conecta Mongo (`mongodb`, `mongodb_risco`) e MySQL, registra todos os blueprints/rotas (`*_app.configure(app)` ou `app.register_blueprint(...)`).
- `virasana/routes/` — um arquivo por blueprint/"app" (padrão `*_app.py` com função `configure(app)`): `imagens_app.py`, `apirecintos_app.py`, `conformidade_app.py`, `escaneamento_app.py`, `pyxvis_app.py`, etc. **Padrão a seguir para novas "apps".**
- `virasana/integracao/` — integrações com bases externas, organizadas por domínio: `mercante/`, `carga2/`, `due/`, `bagagens/`, `risco/`, `sivana/`, `transito/`.
- `virasana/models/` — modelos SQLAlchemy/Mongo (`models.py`, `image_search.py`, `auditoria.py`, `anomalia_lote.py`).
- `virasana/usecases/` — regras de negócio (`apirecintos_manager.py`, `auditoria.py`, `bagagens_manager.py`).
- `virasana/navii/` — **NAVI-I**, tela de análise de risco na importação (`/navii`, blueprint `bp_navii`, template gov.br `templates/navii.html`). **Antes de mexer, ler [virasana/navii/CONTEXTO.md](virasana/navii/CONTEXTO.md)**: decisões do usuário, modelo de dados, medições do banco, armadilhas e pendências. Em resumo: a busca parte do contêiner declarado (`Item`), o Ciclo de Vida é opcional, filtros em outras tabelas usam `_existe` (subquery com `LIMIT 1`, nunca `IN`/`JOIN`), `ovr_recintos` é lida por `tabela_recintos` (não importar `bhadrasana.models.ovr`). `navii/referencias/` tem tabelas locais código→nome (NCM, portos, países, navios) geradas de fontes públicas e filtradas pelo uso real na base (`atualiza_referencias.py --banco`); `navii/diagnostico_sql.py` mede as queries da tela (`--analyze`). `navii/filtros_salvos.py` guarda os filtros salvos de cada usuário no **MongoDB** (coleção `navii_filtros_salvos`).
- `virasana/entregas/` — **Entregas por CNPJ** (`/entregas_cnpj/`, blueprint `bp_entregas`, template `templates/entregas_cnpj.html`): saídas pelos gates (AcessoVeiculo) dos contêineres dos CEs de importação de um CNPJ; tela + planilha. Lógica em `busca_entregas.py`, reusada pelo script `analytic_scripts/entregas_cnpj.py`.
- `virasana/analytic_scripts/` — scripts de análise rodados à mão (não por cron): `dados_transporte_apreensoes.py` (apreensões de drogas do Fichas → acessos de veículo: placa, motorista, transportadora; CSV), `entregas_cnpj.py`, `ciclo_de_vida/` (tabelas do Ciclo de Vida usadas pelo NAVI-I).
- `virasana/workers/` — tarefas Celery e monitoramento de diretórios.
- `virasana/scripts/` — scripts standalone de importação/atualização (rodados via cron/periodic, não pela app web).
- `virasana/forms/`, `virasana/exportacao/`, `virasana/analises/` — WTForms, exportação de dados, análises pontuais.
- `virasana/tests/` — pytest (alguns arquivos são scripts de laboratório/exploração, não testes automatizados formais — ex.: `cargalab.py`, `xmllab.py`, `image_search_tester.py`).

## Produção: prefixo `/virasana` atrás de proxy (obrigatório para código novo)

No Servidor a app roda via WSGI atrás de um **proxy Apache**, montada no prefixo **`/virasana`** por
`DispatcherMiddleware` ([wsgi_production.py](wsgi_production.py); o `wsgi_debug.py` local também monta `/virasana`). O Apache só encaminha o
que está sob o prefixo e repassa um Host interno. Funcionar no `localhost` **não** prova que funciona
no Servidor. Regras:

- Toda URL interna sai de `url_for(...)` nos templates; o JS **recebe** as URLs do servidor
  (`var API = {{ url_for('bp.rota') | tojson }};`) e nunca monta caminhos começando com `/`.
- Sem redirect absoluto: use `redirect(url_for(...))` ou caminho relativo (padrão do código antigo,
  ex.: `redirect('bagagens?...')`). **Rotas índice com `strict_slashes=False`**: o redirect
  automático da barra final (`/tela` → `/tela/`) sai com o Host interno e quebra atrás do proxy
  (foi o caso de `/entregas_cnpj` em 01/10/2026).
- Link para a outra app usa o prefixo dela fixo: `/bhadrasana2/...` ou `/virasana/...`.
- Teste montando a app no prefixo (modelo: `virasana/tests/test_prefixo_proxy.py`, no repo virasana):
  abre cada tela com e sem barra final sem redirect e confere que todo `href`/`src`/`action`/URL do
  JS começa com o prefixo.

## Como rodar os testes

```bash
python -m pytest --cov=virasana virasana/tests
```

(equivalente ao `testenv` do [tox.ini](tox.ini); `testenv:check` roda `flake8`/`bandit`/`isort`).

A suíte inteira falha neste ambiente por motivos antigos e alheios (coleta sem `ajna_commons` no path, Mongo). Para as funcionalidades novas, rodar os testes delas, que usam SQLite em memória e não tocam bases reais (venv: `virasana-venv`):

```bash
python -m pytest virasana/tests/test_navii.py virasana/tests/test_navii_busca_sqlite.py virasana/tests/test_navii_referencias.py virasana/tests/test_navii_filtros_salvos.py virasana/tests/test_entregas_cnpj.py virasana/tests/test_dados_transporte_apreensoes.py virasana/tests/test_prefixo_proxy.py --disable-warnings
```

## Restrição principal: não arriscar produção

Mesma política do [bhadrasana2](../bhadrasana2/CLAUDE.md):

- Mudanças aditivas e isoladas (nova rota/blueprint próprio) em vez de refatorações do que já está em produção.
- Cuidado redobrado com `virasana/workers/` (Celery) e `virasana/scripts/` usados em cron/periodic — quebrar um desses pode parar processamento assíncrono sem erro visível na hora.
- Não fazer upgrade de dependências centrais (Flask, SQLAlchemy, OpenCV) como efeito colateral de outra tarefa.
- Rodar a suíte de testes relevante antes de considerar uma mudança pronta.
- Mudanças em modelos compartilhados com bhadrasana2 (ex.: `mercantealchemy`) são de maior risco — confirmar com o usuário antes.

## Como trabalhar neste repo (workflow)

Mesmo workflow do bhadrasana2: plano antes de mudanças não-triviais ou que toquem código compartilhado; novas funcionalidades isoladas em blueprint próprio; testes antes de dar por concluído.
