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
- Persistência: MySQL/SQLAlchemy (`db.py` → `mysql`) + MongoDB (`mongodb`, `mongodb_risco`).
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
- `virasana/navii/` — **NAVI-I**, produto de análise de risco na importação para analistas (não é um projeto/repo separado — é um módulo deste repo). Blueprint `bp_navii` em `/navii`, template gov.br em `virasana/templates/navii.html`. Reaproveita as entidades de `analytic_scripts/ciclo_de_vida/ciclo_de_vida.py` (`CicloDeVidaImportacao`), `integracao/mercante/mercantealchemy.py` (`Conhecimento`, `Manifesto`, `Enumerado`) e, do bhadrasana2, `bhadrasana.models.laudo` (`Empresa`/CNPJ→nome, `NCM`/descrição oficial) como fonte de dados — não duplicar esses models. Ainda sem fonte de dados de sócios/CPF (filtros subjetivos automáticos ficam para um próximo incremento).
- `virasana/workers/` — tarefas Celery e monitoramento de diretórios.
- `virasana/scripts/` — scripts standalone de importação/atualização (rodados via cron/periodic, não pela app web).
- `virasana/forms/`, `virasana/exportacao/`, `virasana/analises/` — WTForms, exportação de dados, análises pontuais.
- `virasana/tests/` — pytest (alguns arquivos são scripts de laboratório/exploração, não testes automatizados formais — ex.: `cargalab.py`, `xmllab.py`, `image_search_tester.py`).

## Como rodar os testes

```bash
python -m pytest --cov=virasana virasana/tests
```

(equivalente ao `testenv` do [tox.ini](tox.ini); `testenv:check` roda `flake8`/`bandit`/`isort`).

## Restrição principal: não arriscar produção

Mesma política do [bhadrasana2](../bhadrasana2/CLAUDE.md):

- Mudanças aditivas e isoladas (nova rota/blueprint próprio) em vez de refatorações do que já está em produção.
- Cuidado redobrado com `virasana/workers/` (Celery) e `virasana/scripts/` usados em cron/periodic — quebrar um desses pode parar processamento assíncrono sem erro visível na hora.
- Não fazer upgrade de dependências centrais (Flask, SQLAlchemy, OpenCV) como efeito colateral de outra tarefa.
- Rodar a suíte de testes relevante antes de considerar uma mudança pronta.
- Mudanças em modelos compartilhados com bhadrasana2 (ex.: `mercantealchemy`) são de maior risco — confirmar com o usuário antes.

## Como trabalhar neste repo (workflow)

Mesmo workflow do bhadrasana2: plano antes de mudanças não-triviais ou que toquem código compartilhado; novas funcionalidades isoladas em blueprint próprio; testes antes de dar por concluído.
