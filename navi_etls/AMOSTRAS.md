# NAVI-ETL — amostras, anonimização, sombra e estatísticas (proposta, 24/10/2026)

Ideia do usuário: em vez de duplicar o tráfego de produção, **coletar amostras** do que chega (XML do Mercante,
JSON dos scripts, BSON do Avatar), **anonimizar** e guardar no repositório. A partir delas: dados de teste, sombras
comparando versões do código e testes de não regressão para mudanças futuras. Endpoints e arquivos em
`INVENTARIO.md`; fases em `PLANO.md`.

## 1. O corpus

```
navi_etls/amostras/              NO repositório: só anonimizado e pequeno (meta: < 20 MB)
  mercante/                      XML por tipo (CONHECIMENTO, MANIFESTO, ITEM, ESCALA), recortados
  apirecintos/                   JSON de E1 (lista de partes) e E2 (.json com objeto, .zip), por tipo de evento
  imagens/                       E6 (json + jpg) e E7 (.bson)
  due/  dta/                     corpos de E11–E14
  MANIFESTO.md                   de onde veio cada amostra (fonte, janela, recinto) e o que cobre
../navi_etls_amostras_reais/     FORA do repositório (estação e servidor): os originais, para a sombra e as medidas de
                                 desempenho; nunca entra no git
```

O que cada fonte precisa cobrir (casos que o código trata de forma diferente):

| Fonte | Cobertura |
|---|---|
| Mercante (P1) | os quatro tipos e `exclusoesEscala`; inclusão e retificação; master, filhote e master-filhote; contêiner vazio; vários NCMs no item; acentuação (latin1); um arquivo grande (desempenho) |
| API Recintos (E1, E2) | os quatro tipos (1 acesso e agendamento, 3 pesagem, 4 embarque/desembarque, 25 inspeção); inclusão e retificação (`tipoOperacao` R); contingência; o que é rejeitado (placa, contêiner, texto quebrado — mojibake); duplicados no lote e entre lotes; os dois formatos de E2 |
| Imagens da API Recintos (E6) | vários recintos; recinto fora de `ovr_recintos`; datas com fuso; reenvio do mesmo evento |
| Avatar (E7) | um BSON por fornecedor de escâner (os XMLs mudam de formato); vários pares imagem + XML; um BSON grande (desempenho) |
| DUE e CNPJ (E11–E13) | DUE com itens e vários contêineres; DUE já gravada (atualização); campos inválidos (data, tamanho) que geram o 422 parcial; CNPJ novo e já existente |
| DTA (E14) | todas as colunas; data e decimal inválidos; caracteres de controle (C1) |

## 2. Coleta (só leitura; nada do código atual muda)

| Fonte | Já existe | Como coletar mais |
|---|---|---|
| Mercante | ~746 XML na estação (`virasana/mercante/`, de jun/2025 e jan/2026; 244 MB; fora do git) | `coleta_mercante.py --de --ate`: chama `consultaArquivos` e `download` do Aniita e grava numa pasta — **não** grava `arquivosbaixados` nem usa a pasta do periódico. Conferir se o Aniita guarda o histórico das janelas antigas |
| API Recintos | `bhadrasana2/tests/json/consulta.json` (84 eventos do tipo 1, JSON) e `formato_contagil.json` (**não é JSON**: `repr` de dicionário do Jython, com `u'...'`); fora do git | os scripts do ContÁgil gravam em arquivo cada corpo que mandariam (opção "só salvar"), para janelas escolhidas — mudança nos scripts do ContÁgil, fora destes repositórios |
| Imagens (E6) | — | idem: o script grava o `json` e a `imagem` de cada envio |
| Avatar (E7) | `avatar/tests/pastas_exemplo/` (pastas reais por recinto: jpg + XML, 114 MB) e `tests/xmls_exemplo/` | (a) copiar BSON da pasta de saída de um Avatar antes do envio (o Avatar apaga depois que o envio dá certo); (b) gerar com o próprio código do Avatar (`fontes/avatar/`) a partir das pastas de exemplo; (c) remontar do GridFS só os campos que o Avatar manda |
| DUE, CNPJ, DTA (E11–E14) | `virasana/dues.csv`, `itens_dues.csv`, `escaneamentos_sem_due.csv`, `cnpjs_nomes.csv` (na raiz da estação, usados pelo teste manual do `integracao_app`) | os scripts do ContÁgil gravam o corpo de cada envio |
| GET de apoio (E3, E4, E8–E10) | — | não se coletam: a resposta depende do banco. O gabarito sai do corpus carregado numa base vazia (seção 4) |

## 3. Anonimização (pseudônimos consistentes e válidos)

Ferramenta `navi_etls/amostras/anonimiza.py` (no repositório) com uma **chave** que fica fora dele (variável de
ambiente, guardada pelo usuário). O mesmo valor original vira sempre o mesmo pseudônimo (HMAC com a chave): as
ligações entre arquivos se mantêm (CE ↔ contêiner ↔ DUE ↔ imagem), uma amostra nova se encaixa nas antigas, e sem a
chave não dá para desfazer nem testar candidatos. A tabela de troca não é gravada.

| Dado | Regra |
|---|---|
| Contêiner (ISO 6346) | mantém o prefixo do dono (4 letras: é da empresa de navegação, não do importador); troca os 6 dígitos e recalcula o dígito verificador |
| CNPJ | troca a raiz (8 posições, a mesma troca para a mesma raiz), mantém a filial, recalcula os 2 DVs; aceita o **CNPJ alfanumérico** (em vigor desde jul/2026) |
| CPF | troca os 9 dígitos e recalcula os 2 DVs |
| CE do Mercante, DUE, DI, número da DTA | mantém a estrutura (unidade, ano, `BR` na DUE) e troca o sequencial; o DV de cada um é conferido nos números reais na F0 antes de fixar a regra |
| BL (`numConhecimento`) | mantém o prefixo de 4 letras (SCAC) e troca o resto (mesmo tamanho e tipo de caractere) |
| Placa | mantém o formato (antigo `AAA9999` ou Mercosul `AAA9A99`) |
| Chave de NF-e (44 dígitos) | remonta com o CNPJ já trocado, número e código novos e o DV recalculado |
| Nomes (motorista, exportador, importador, consignatário, embarcador, notify), endereço, telefone, e-mail, protocolos (UUID) | nomes e dados fictícios, com a mesma troca para o mesmo valor |
| Texto livre (descrição da mercadoria, observações) | troca os números que a regra reconhece (CPF, CNPJ, CE, contêiner); o que sobrar com cara de nome vai para revisão manual |
| Imagem (jpg) | no repositório, uma imagem sintética pequena com o mesmo nome (já trocado); a real fica só fora do repositório (raio X pode ter contêiner e data gravados na imagem); sem EXIF |
| XML do escâner (dentro do BSON) | as mesmas regras (contêiner, placa, operador) |
| Fica igual | NCM, pesos, volumes, datas, recintos, portos, navios, tipos e códigos de domínio |

Regra de validade: o que era **válido continua válido** e o que era **inválido continua inválido do mesmo jeito**
(o contêiner com DV errado continua com DV errado) — senão os testes de rejeição perdem o sentido.
`confere_anonimizacao.py` falha se achar no corpus anonimizado algum valor original, ou um CPF ou CNPJ válido que
a ferramenta não gerou.

## 4. Gabaritos e sombra

- **Gabarito (F0)**: o código **atual** roda sobre o corpus, sem HTTP, em bases vazias (SQLite e mongomock), e as
  saídas viram arquivos esperados (`navi_etls/tests/esperado/`): linhas por tabela, documentos e respostas das
  rotas. O mesmo corpus carregado responde os GET de apoio (E3, E4, E8–E10).
- **Testes do gate**: a cópia (F1) e a versão moderna (F3) têm de reproduzir o gabarito. Quando uma correção mudar
  um resultado de propósito (o lote perdido no erro 1062, por exemplo), o gabarito muda com a explicação registrada.
- **Sombra (antes de cada implantação)**: o mesmo corpus — e as amostras reais, fora do repositório — no código
  antigo e no novo, em bases de verdade e vazias (MariaDB e Mongo de homologação, esquemas tirados de
  `SHOW CREATE TABLE` da produção). `compara.py` compara as tabelas e coleções pela chave natural, sem ids,
  `create_date` e `ObjectId`, e lista as diferenças. É o que pega o que o SQLite não mostra (`ON DUPLICATE KEY`,
  collation, `max_allowed_packet`).
- No código antigo, a chamada ao `/grid_data` por HTTP é trocada só no roteiro da sombra pela consulta direta.

## 5. Estatísticas de volume (pronto, 24/10/2026; só leitura, roda na estação como os outros diagnósticos)

`virasana-venv/Scripts/python.exe navi_etls/diagnosticos/volumes.py --dias 30 --saida volumes.log` (SQL_URI e
MONGODB_URI do ambiente; `--dias-detalhe 3` para os lotes; `--secoes espaco recintos imagens mercante due`), com
`max_statement_time` e janelas por data indexada ou pela chave (nunca varre tabela grande). Também mede o que as
melhorias 7.1 e 7.2 do `PLANO.md` precisam: o espaço das tabelas RAW do Mercante contra as `*resumo` e o que o
passo do XML do periódico procura de novo a cada volta.

1. **API Recintos**, por tabela: eventos por dia (`create_date`, a gravação) e por recinto; **lotes** por dia
   (aproximado pelos segundos distintos de `create_date`) e eventos por lote (mediana, p95, máximo); rejeições por
   dia e motivo; ponteiros do controle e o atraso por recinto e tipo.
2. **Imagens** (`fs.files` pelo `uploadDate`, indexado): por dia e por **origem** — Avatar (`metadata.idcov`), API
   Recintos (`metadata.datahoratransmissao`), outras (pelo conjunto de chaves do `metadata`); tamanho (soma e
   p50/p95 de `length`); por recinto; envios do Avatar (segundos distintos) e imagens por BSON.
3. **DUE**: `pucomex_due` por mês de `data_registro_due`, itens e contêineres (as tabelas não têm data de gravação);
   `laudo_empresas`: total.
4. **DTA**: `navii_dtas`, total e por data (a coluna de data achada pela estrutura da tabela).
5. **Mercante**: `arquivosbaixados` por dia e por tipo; CEs por dia pela janela de ID.

Fica de fora o tamanho em bytes de cada corpo recebido (não é gravado em lugar nenhum): sai da coleta.

## 6. Inventário do ambiente (decisão 6; pronto, 24/10/2026)

`navi_etls/diagnosticos/inventario_ambiente.py`: só biblioteca padrão; roda no Windows de desenvolvimento e no
servidor (`--banco` usa o venv atual para perguntar às bases; `--pypi` testa o acesso). Com `--grava`, salva
`navi_etls/ambiente/<máquina>_<data>.md`:

    virasana-venv/bin/python navi_etls/diagnosticos/inventario_ambiente.py --venv virasana-venv --banco --pypi --grava

O que registra:

- sistema operacional (versão e build no Windows; `/etc/os-release` e kernel no Linux);
- Python: os interpretadores instalados e as versões **oferecidas pelos repositórios oficiais** da distribuição
  (`dnf list --available 'python3*'` ou `apt-cache policy python3.*`);
- `pip freeze` dos venvs atuais (virasana e bhadrasana2);
- MariaDB: `VERSION()`, `@@version_comment`, charset e collation, `sql_mode`, `max_allowed_packet`,
  `max_statement_time`; os pacotes de cliente instalados;
- MongoDB: versão, `featureCompatibilityVersion`, motor de armazenamento;
- Apache (versão e módulos de proxy), supervisor, compilador e cabeçalhos de desenvolvimento (para drivers em C);
- acesso ao PyPI (direto, proxy ou espelho).

Regra do usuário: só versões **oficiais da distribuição** (nada instalado "por fora"). Leitura proposta, a
confirmar: o interpretador e as bibliotecas de sistema vêm da distribuição; os pacotes Python, por `pip` no venv
do projeto, como hoje no virasana e no bhadrasana2. Se a distribuição não oferecer o 3.14, o alvo é a versão mais
nova que ela oferece.
