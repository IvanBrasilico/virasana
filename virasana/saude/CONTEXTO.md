# Health check diário do NAVI (v1, 23/10/2026) — publicado e ativo

Saiu do `BACKLOG.md` na limpeza de 23/10/2026: o desenho, os limiares e o histórico da 1ª rodada ficam aqui; o
estado de cada achado fica no histórico das rodadas (tela `/saude/`, "novo" / "desde dd/mm").

**Um relatório curto por dia**, OK / ATENÇÃO / FALHA por componente, reaproveitando o que cada parte já mede. Só
leitura; cada consulta SQL com limite de 120 s; uma verificação que quebra vira FALHA dela e as outras seguem.
Limiares provisórios (constantes no topo de cada módulo), a calibrar com as primeiras semanas.
- **Serviço** `saude_navi` (supervisor; `servico_saude.py`, `supervisor_saude.conf`): acorda a cada minuto, grava o
  batimento e roda às 7 h (uma vez por dia; se subir depois das 7 h, roda na hora) ou quando há pedido da tela. Fica
  fora dos serviços que vigia. Erro fora das verificações vira uma rodada com FALHA "Health check". Log em
  `/var/log/virasana/saude/`.
- **Histórico** no Mongo do virasana (`historico.py`): rodadas, pedidos e batimento; TTL de 180 dias. Cada achado é
  "novo" ou "desde dd/mm" (chave = o texto sem os números que variam; códigos de 7+ dígitos ficam) — trata os
  crônicos sem exceção por recinto (decisão "a" da 1ª rodada).
- **Tela** `/saude/` (só perfil Supervisor, a regra das estatísticas de uso; menu NAVII "Saúde do sistema
  (supervisores)"): estado geral, última e próxima rodada, serviço ativo ou parado, "Rodar agora"; um bloco por
  componente (cor + ícone + texto) com o resumo, "nesse estado desde", a faixa das últimas 14 rodadas e os achados
  marcados; histórico das rodadas e o detalhe de cada uma.
- **Linha de comando** `saude_navi.py` para rodar à mão (0/1/2; `--so`, `--nucleo-desde`, `--grava`).

| Componente | Medida | Alerta | Módulo |
|---|---|---|---|
| Mercante | CE e item mais novos carregados (pela chave primária) | > 6 h atenção; > 24 h falha | `fontes.py` |
| API Recintos | por tipo de evento: posição do extrator (ponteiro ou evento mais novo); por recinto: últimas 24 h × as **mesmas 24 h da semana anterior**; rejeições da carga | extrator > 6 h: falha; recinto ativo (≥ 50) com < 10%: atenção (com o último evento e o ponteiro dele); queda geral < 50% sem recinto que a explique; rejeições ≥ 100 e > 3× | `fontes.py` |
| Imagens (MongoDB) | imagem mais nova (`uploadDate`, UTC); por recinto: últimas 24 h × mesmas 24 h da semana anterior; atraso (chegada − escaneamento); ponteiro '99' do extrator | > 6 h: falha; recinto ativo (≥ 20) com < 10%; queda geral < 50%; atraso mediano > 48 h | `imagens.py` |
| Ciclo — serviço | alertas do acompanhamento (14 dias; `acompanha_ciclo_de_vida.py`, seção 7) | "serviço parado"/sem marcas: falha; outros: atenção | `ciclo.py` |
| Ciclo — invariantes | `audita_ciclos.Auditoria` nos desembarques das últimas 48 h | > 0 | `ciclo.py` |
| Ciclo — refaz | ciclos dos últimos 30 dias processados antes de `--nucleo-desde` | > 0 (com o comando do refaz) | `ciclo.py` |
| Ciclo — cobertura | % com CE e % dos escaneados com imagem, desembarques de 9 a 2 dias atrás × a semana anterior | queda > 10 pontos | `ciclo.py` |
| Pesagens | % possível fraude e % digitação da pesagem principal, 7 dias × 21 anteriores | > 2× (com ≥ 20 pesagens) | `ciclo.py` |
| Risco histórico | marca da extração, IDs do Mercante por ler, fila da pontuação, faixas | marca > 6 h; > 100 mil IDs; fila > 50 mil (só 6 h depois da última versão dos parâmetros) | `aplicacoes.py` |
| Busca por texto | Meilisearch no ar; IDs do Mercante por indexar (precisa de `NAVII_MEILI_CHAVE_ESCRITA`) | fora: falha; > 20 mil: atenção; sem chave: não medido | `aplicacoes.py` |
| NAVI-I | tempo no servidor da 1ª página das pesquisas de 24 h (a coleta de uso grava) | p95 > 10 s ou > 5% lentas (com ≥ 20 pesquisas) | `aplicacoes.py` |
| Fichas de FMA | Ficha de FMA mais nova (`fma_update.py` do Fichas) | > 3 dias | `aplicacoes.py` |

O acompanhamento entra só pelos **alertas** (seção 7): as seções 1–6 (cobertura por semana, situação × fluxo,
escaneamentos por recinto, CE, pendentes do loop antigo) não viram alerta — quem quiser a tabela roda
`acompanha_ciclo_de_vida.py --saida acompanhamento.log`.

Próximos passos possíveis (não priorizados): cruzar, por recinto, inspeções transmitidas × imagens recebidas (precisa
do de-para do `metadata.recinto` para o código do recinto); erros 5xx do NAVI-I (log do gunicorn); espaço em disco e
programas do supervisor.

## 1ª rodada em produção (23/10/2026 10:27 — ATENÇÃO)

- API Recintos: MultiRio (7921303) e ICTSI (7921302) pararam de transmitir embarques/desembarques (0 em 24 h contra
  3.051 e 550 na semana anterior; o extrator pergunta, o recinto não manda) — cobrar os recintos.
- Imagens: BTP3 parou de mandar imagem (10 em 24 h contra 1.382); escaneados com imagem de 90% para 82% na semana.
- Refaz: 2 ciclos de 20/10 (sobra da parte 3 do refaz de 22/10) — refeitos; a 2ª rodada (10:50) mediu 0.
- NAVI-I: 40 pesquisas, mediana 6,1 s, p95 10,5 s só na 1ª página — reforça a contagem assíncrona (BACKLOG P1.6).
- Invariantes: 12.492 ciclos das últimas 48 h, 0 violações (C6/C7/C8 em produção).
- Calibração no mesmo dia: fila da pontuação do risco só conta 6 h depois da última versão dos parâmetros; relógio
  do Mongo segundos adiantado; sem ponteiro '99' das imagens no controle, o resumo não o cita; abandono só dos
  ciclos sem Ficha de FMA, com a composição dos abertos além do prazo.

Crônicos conhecidos (aparecem como "desde dd/mm"):
- ciclos v2 abertos há mais de 93 dias sem Ficha de FMA (98–99 na 1ª rodada; hipótese: MultiRio, Suape e Pecém,
  operadores que não transmitem a saída, fora dos recintos do `fma_update.py`). A composição sai no acompanhamento;
  se sobrarem, conferir `processado_em` (revisitados e mantidos abertos × não revisitados):
  `SELECT c.codigoRecintoDesembarque, c.fluxo, c.motivo, COUNT(*), MIN(c.data_desembarque), MAX(c.processado_em)
  FROM ciclodevida_ciclo_de_vida_importacao c WHERE c.situacao = 'aberto' AND c.versao_heuristica = 'v2' AND
  ((c.data_saida IS NULL AND c.data_entrada_seco IS NULL AND c.data_desembarque < NOW() - INTERVAL 93 DAY) OR
  c.data_entrada_seco < NOW() - INTERVAL 93 DAY) GROUP BY 1, 2, 3 ORDER BY 4 DESC;`
- Pecém (3111401) com 21% dos escaneamentos com imagem: conhecido (imagens com +3 h e escâneres que não mandam ao
  Mongo — "Fora de Santos e pesagens" no `CICLO_DE_VIDA.md`).
