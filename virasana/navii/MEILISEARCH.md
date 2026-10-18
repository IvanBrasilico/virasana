# Meilisearch da busca por texto do NAVI-I — instalação no Rocky Linux 9

Índice de CEs para a aba "Texto" do NAVI-I (plano e decisões em `CONTEXTO.md`, "Busca por texto"). O Meilisearch
fica **só em 127.0.0.1**: o navegador nunca fala com ele, quem consulta é o virasana (chave de busca) e quem escreve é
o indexador (`indexador_texto.py`, chave de escrita). O índice é **derivado do MariaDB**: se perder, apaga e
recarrega (não precisa de backup, dump nem snapshot).

## Dimensionamento (F0, produção, 18/10/2026)

| | CEs | Disco do índice (ordem de grandeza) |
|---|---|---|
| Carga inicial (3 meses) | ~0,95 milhão | 1 a 3 GB |
| Retenção (12 meses) | ~3,8 milhões | 5 a 13 GB |

Reserve **30 GB** em `/var/lib/meilisearch` (a indexação usa espaço temporário além do índice). Memória: o índice é
lido por mapeamento de arquivo; deixe uns 4 GB livres para o cache do sistema e limite a indexação a 2 GiB e 2
threads (abaixo) para não disputar com o MariaDB e o virasana. Ritmo contínuo: ~10 mil CEs novos por dia.

## 1. Binário (versão fixa)

Escolha a última versão estável **1.x** em https://github.com/meilisearch/meilisearch/releases e fixe o número. O
código usa só a API estável da 1.x (apagar por filtro existe desde a 1.2). Se o servidor não acessa a internet,
baixe em outra máquina e copie (scp).

```bash
VERSAO=v1.X.Y          # a versão escolhida
curl -L -o meilisearch "https://github.com/meilisearch/meilisearch/releases/download/${VERSAO}/meilisearch-linux-amd64"
sha256sum meilisearch  # anote no registro da instalação
sudo install -m 0755 -o root -g root meilisearch /usr/local/bin/meilisearch
sudo restorecon -v /usr/local/bin/meilisearch          # SELinux: rótulo de binário do sistema
/usr/local/bin/meilisearch --version
```

## 2. Usuário, pastas e configuração

```bash
sudo useradd --system --home-dir /var/lib/meilisearch --shell /sbin/nologin meilisearch
sudo mkdir -p /var/lib/meilisearch/data /var/lib/meilisearch/dumps /var/lib/meilisearch/snapshots
sudo chown -R meilisearch:meilisearch /var/lib/meilisearch
sudo chmod 750 /var/lib/meilisearch
openssl rand -base64 32   # a chave mestra (guarde no cofre de senhas da equipe)
```

`/etc/meilisearch.env` (dono root, `chmod 600`):

```ini
MEILI_ENV=production
MEILI_HTTP_ADDR=127.0.0.1:7700
MEILI_MASTER_KEY=<a chave mestra gerada acima>
MEILI_DB_PATH=/var/lib/meilisearch/data
MEILI_DUMP_DIR=/var/lib/meilisearch/dumps
MEILI_SNAPSHOT_DIR=/var/lib/meilisearch/snapshots
MEILI_NO_ANALYTICS=true
MEILI_MAX_INDEXING_MEMORY=2GiB
MEILI_MAX_INDEXING_THREADS=2
MEILI_LOG_LEVEL=INFO
```

## 3. Serviço (systemd)

`/etc/systemd/system/meilisearch.service`:

```ini
[Unit]
Description=Meilisearch (busca por texto do NAVI-I)
After=network.target

[Service]
Type=simple
User=meilisearch
Group=meilisearch
EnvironmentFile=/etc/meilisearch.env
ExecStart=/usr/local/bin/meilisearch
Restart=on-failure
LimitNOFILE=65535
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/meilisearch

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now meilisearch
systemctl status meilisearch
curl -s http://127.0.0.1:7700/health          # {"status":"available"}
ss -ltnp | grep 7700                           # só 127.0.0.1:7700 — nenhuma porta no firewall
```

## 4. Chaves (uma de busca, uma de escrita)

Com a chave mestra só para criar as outras (não usar a mestra no virasana):

```bash
MESTRA='<a chave mestra>'
curl -s -X POST http://127.0.0.1:7700/keys -H "Authorization: Bearer $MESTRA" -H 'Content-Type: application/json' \
  --data '{"description": "NAVI-I busca (virasana)", "actions": ["search"], "indexes": ["navii_ces"], "expiresAt": null}'
curl -s -X POST http://127.0.0.1:7700/keys -H "Authorization: Bearer $MESTRA" -H 'Content-Type: application/json' \
  --data '{"description": "NAVI-I indexador", "actions": ["documents.*", "indexes.create", "indexes.get", "settings.*", "tasks.get", "stats.get"], "indexes": ["navii_ces", "navii_texto_estado"], "expiresAt": null}'
```

O campo `key` de cada resposta é a chave. Variáveis de ambiente (no mesmo lugar onde estão `SQL_URI` e
`MONGODB_URI` para o supervisor — **não** no repositório):

| Variável | Quem usa | Valor |
|---|---|---|
| `NAVII_MEILI_URL` | virasana e indexador | `http://127.0.0.1:7700` (é o padrão) |
| `NAVII_MEILI_CHAVE_BUSCA` | virasana (aba Texto, F2) | a chave "NAVI-I busca" |
| `NAVII_MEILI_CHAVE_ESCRITA` | indexador | a chave "NAVI-I indexador" |

## 5. Indexador (supervisor)

Ver `supervisor_navii_texto.conf` na raiz do virasana (`autostart=false` até a primeira carga ser conferida).

```bash
cd /home/ajna/apps/ajna/virasana
virasana-venv/bin/python virasana/navii/indexador_texto.py --uma-vez --simula   # mede uma volta, não escreve
virasana-venv/bin/python virasana/navii/indexador_texto.py --uma-vez            # 1ª volta: cria o índice e a marca
virasana-venv/bin/python virasana/navii/indexador_texto.py --situacao
sudo mkdir -p /var/log/virasana/navii_texto && sudo chown ajna /var/log/virasana/navii_texto
sudo supervisorctl reread && sudo supervisorctl update && sudo supervisorctl start navii_texto
```

A carga inicial anda em voltas de 20 mil IDs (10 páginas de 2.000), sem pausa enquanto houver fila; `--situacao`
mostra os IDs que faltam. Depois, uma volta a cada 2 minutos.

## Operação

- **Recarregar do zero** (versão nova do Meilisearch, índice corrompido, mudança grande de documento): parar o
  indexador, apagar os dois índices (`curl -X DELETE .../indexes/navii_ces` e `.../indexes/navii_texto_estado`, com a
  chave mestra) e iniciar o indexador — sem marcas, ele refaz a carga inicial.
- **Atualizar o Meilisearch**: entre versões, o formato do banco pode mudar. Como o índice é derivado, o caminho mais
  simples é parar, trocar o binário, apagar `/var/lib/meilisearch/data` e recarregar.
- **Meilisearch fora do ar**: só a aba Texto avisa; o indexador registra o erro e tenta de novo na volta seguinte (as
  marcas só andam depois de o Meilisearch confirmar as escritas).
