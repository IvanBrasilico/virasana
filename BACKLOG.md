# Backlog — NAVI-I, ciclo de vida, risco e busca (atualizado em 22/10/2026)

Junta as pendências que estavam espalhadas: `virasana/navii/CONTEXTO.md` ("Pendências" e "Próximos passos"),
`virasana/analytic_scripts/ciclo_de_vida/CICLO_DE_VIDA.md`, `virasana/risco_historico/CONTEXTO.md` e as notas das
sessões. O detalhe de cada item continua no documento de origem; aqui ficam o estado, a prioridade e a **decisão que
falta**. Ao fechar um item, mova-o para "Feito" com a data.

**Critério de prioridade**, nesta ordem: (1) dado errado ou perdido em produção; (2) operação e saúde — saber cedo
quando algo quebra; (3) validação com os usuários do que já está no ar; (4) desempenho percebido; (5) funcionalidade
nova.

## Feito (recente)

- **Filtros em linguagem natural** → **Busca por texto** (Meilisearch), aba `/navii/texto`, em produção desde
  19/10/2026. É busca por palavras tolerante a erro de digitação, não semântica.
- Ciclo de vida: porto do desembarque (C1–C3, caso CAAU5545018), C6/C7/C8, corpus de 26 casos reais, invariantes e
  auditoria da produção (`audita_ciclos.py`) — 22/10/2026.
- NAVI-I: modos de exibição (detalhado / simplificado / galeria), filtro de tráfego, atracação prevista no card,
  pesagens P1–P3, imagem pelo contêiner e data — 20 a 22/10/2026.
- Ciclo de vida fora de Santos (MultiRio, Suape, Pecém) e pesagens em produção — 21/10/2026.
- Gate de qualidade: `qualidade_navii.py --alterados` e `tox -e navii` isolado — 22/10/2026.
- **Health check diário v1** (`virasana/saude/saude_ajna.py`, seção abaixo) e **refaz robusto** — feitos no código em
  23/10/2026 (aprovados pelo usuário, com as checagens da carga da API Recintos e das imagens no MongoDB que ele
  pediu); falta publicar e pôr no cron.

## P0 — agora

1. **Terminar o refaz de 22/10.** O `--refaz-desde 2026-09-22` rodou da estação de trabalho e perdeu a rede: 24 de
   41 partes falharam (a parte 3 às 16:22, conexão reiniciada; as partes 19 a 41 às 17:27, rede inacessível).
   Só 85 mil dos 202.692 contêineres foram refeitos. O refaz anda do desembarque mais recente para o mais antigo, e
   **todos** os 118 ciclos com violação na 2ª auditoria (113 "CE principal com a carga saindo do porto" e 9 "TRA
   concluído sem a saída do seco"; 4 com as duas) estão entre 22/09 18:10 e 09/10 03:58 — exatamente as partes que falharam. Nada indica
   defeito do C6/C7/C8. Refazer **no servidor** (rede estável), em sequência, e auditar de novo — esperado: 0.
   Sobrando alguma, extrair com `caso_ciclo.py` e levar ao corpus.
2. ~~Refaz robusto~~ — feito (23/10): a parte que falha é repetida até 3 vezes (30 s e 2 min de espera,
   reconectando); no fim, o log lista o período de desembarque de cada parte não refeita com o comando para
   refazê-la, e o programa sai com código 1.
3. **Health check v1: publicar e pôr no cron** (feito no código em 23/10). Depois de 1 a 2 semanas de relatórios,
   calibrar os limiares (constantes no topo de cada módulo de `virasana/saude/`). *Decisão: onde o resultado aparece
   além do log (e-mail do cron, aba "Saúde").*

## P1 — próximas semanas

4. **Parser da carga do Mercante** (Pendência 5 do `navii/CONTEXTO.md`): `notify`/`cnpjNotify` (77%/53% dos CEs nos
   XMLs) chegam **vazios**; dados complementares do consignatário, observações e nome do embarcador com CNPJ se
   perdem. Os XMLs são apagados depois da carga: **cada dia sem a correção é dado perdido para sempre**. Afeta a
   Entregas por CNPJ e a Busca por texto. Precisa de `ALTER TABLE` (DBA). *Decisão: priorizar agora?*
5. **Revisita de imagens** (proposta de 13/10/2026): ~27% dos "sem imagem" têm a imagem chegando depois ao Mongo, e o
   serviço não volta aos ciclos concluídos. Com a galeria, a falta de imagem ficou muito visível. Proposta: revisitar
   a cada 6 h os ciclos sem imagem dos últimos 10 dias + `--completa-imagens` à mão. *Decisão: aprovar.*
6. **Contagem assíncrona no NAVI-I** (proposta de 02/10/2026): a contagem com NCM leva 18–20 s e segura a 1ª página.
   Mostrar os cards já e a contagem quando chegar. *Decisão: aprovar.*
7. **Validação com os usuários do que está no ar**: Busca por texto (F3), Estatísticas de uso (F3 em validação → F4
   ajustes), modos de exibição, diagnóstico das pesagens (digitação × possível fraude), Abrir Ficha e Fichas em
   acompanhamento. Saída: lista de ajustes priorizada.
8. **Índices e custo das consultas**: medir fora do pico (`navii/diagnostico_sql.py --analyze`, também
   `--so-ciclo` com e sem `--sem-familia`) e decidir com o DBA `ix_ciclo_ce_conteiner` e `ix_itens_ce_conteiner`
   (sugeridos, não criados).

## P2 — depois

9. **Unidade/equipe do usuário**: porto de atracação e recintos padrão pela unidade (hoje `PORTO_ESCALA_PADRAO` fixo
   em BRSSZ). Passa a importar com o ciclo já rodando em MultiRio, Suape e Pecém.
10. **Risco histórico**: F2 (validação no tempo, adiada em 14/10), normalizar o nome do embarcador (vem com
    endereço), ordenação por risco no NAVI-I (medir o cursor composto), F5 (analista desativa parâmetro).
11. **Coleta de uso × galeria**: na galeria vários cards ficam "vistos" de uma vez (a definição é a mesma). Gravar o
    modo de exibição na coleta para separar nas estatísticas? *Decisão.*
12. **Busca por texto**: CE alterado depois de indexado (hoje fica de fora); regra da data de referência (destino ×
    Santos).
13. **C4** (o CE declarado vence a única família plausível não declarada): só se a auditoria mostrar casos.
14. **C7 nos filtros por candidato**: contêiner + "só com desembarque" deixa de fora o BL único que não é o
    principal (atalho mantido por desempenho).
15. **Pendentes do loop antigo**: `servico_ciclo_de_vida.py --reprocessa N`, se a seção 6 do acompanhamento ainda
    mostrar pendentes.

### Ideias de sinais de risco — informação declarada inventada (pedido do usuário, 22/10/2026)

Só registradas. Antes de implementar, cada uma precisa de um **diagnóstico nos dados** (como os F0 dos outros
trabalhos): preenchimento real dos campos, distribuição, quantos CEs seriam marcados e se os marcados batem com as
fichas positivas do risco histórico. Se valerem, viram selo/filtro no NAVI-I (como o diagnóstico das pesagens) e/ou
sinal do risco histórico.

16. **Tipo do contêiner × cubagem × carga × embalagem declaradas.** Inconsistência entre o que o contêiner comporta e
    o que foi declarado (ex.: cubagem acima da capacidade do 20', embalagem que não combina com a mercadoria ou com o
    tipo — granel em dry, carga refrigerada fora de reefer) sugere declaração inventada. Dados: `itensresumo.isoCode`,
    `cubagemM3`, `codigoTipoEmbalagem`, `qtdeItens`, `indicadorUsoParcial`; `ncmitemresumo.codigoTipoEmbalagem`,
    `qtdeVolumes`, `itemEmbaladoMadeira`; `conhecimentosresumo.cubagem` e `descricao`; tamanho/tipo em
    `navii/referencias/tipos_conteiner.json` (faltam as capacidades de referência). O diagnóstico responde: a cubagem
    vem preenchida e com valor real (ou padrão/zero)? Qual a distribuição por tipo de contêiner? Que tabela de
    embalagem o Mercante usa?
17. **Número do BL × origem e companhia declaradas.** O `conhecimentosresumo.numConhecimento` costuma trazer o prefixo
    da companhia (código SCAC, ex.: MAEU, MEDU, COSU) e às vezes o porto/local de emissão; deveria bater com a
    empresa de navegação (`codigoEmpresaNavegacao`), o porto de origem (`portoOrigemCarga`) e o terminal de
    carregamento. Formato estranho para a companhia, ou origem no número diferente da declarada, pode indicar
    informação falsa. O diagnóstico responde: os padrões de numeração por companhia e por tipo de BL (no HBL de
    agente o formato é livre — provavelmente só o MBL/BL serve), a tabela SCAC × empresa e a taxa de divergência
    "normal".
18. **Peso declarado × volume × densidade esperada pelo NCM.** Nos contêineres com **um só NCM**, a densidade
    (peso bruto declarado ÷ cubagem) pode ser comparada com a densidade típica daquele NCM, aprendida da própria base
    (mediana e dispersão por NCM, como os pesos do risco histórico); com mais de um NCM não há a quantidade de cada
    um, então fica de fora (ou só uma faixa possível). Vale cruzar com o **peso medido** das pesagens (P1–P3): a
    pesagem confirma ou derruba o declarado. O diagnóstico responde: quantos contêineres têm um só NCM, com cubagem
    preenchida, quantas amostras por NCM (em que nível — 4, 6 ou 8 dígitos) e quão estreita é a densidade de cada um.

## P3 — estacionados (dependem de terceiros ou de decisão futura)

- ICTSI com D/E trocados: a destroca está pronta e **desligada** — o recinto corrige o envio.
- Escâneres sem imagem no Mongo do AJNA (9101602, 9981403, 9801303, 0301304) e Itaguaí sem acesso/pesagem:
  integração.
- Filtros por sócios/empresa: sem fonte de dados.
- Itens sem contêiner, ciclos sem CE (casar por contêiner + data: risco de contêiner reutilizado), ponte
  recinto ↔ terminal (abandonada).
- Fichas pelo NAVI-I, fase 3 (histórico das encerradas); retirar a tela legada da completude (com aval).
- `tox.ini` do bhadrasana2 (instala o `ajna_commons` de 2019, que passa na frente do vivo).
- Migração Flask/Python e da apresentação para o gov.br: só quando pedidas.

## Health check (v1, feito em 23/10/2026)

`virasana/saude/saude_ajna.py`: **um relatório curto por dia**, OK / ATENÇÃO / FALHA por componente, reaproveitando o
que cada parte já mede. Só leitura; cada consulta SQL com limite de 120 s; uma verificação que quebra vira FALHA
dela e as outras seguem. Sai com 0 (OK), 1 (atenção) ou 2 (falha) — o cron decide quem avisar. `--so` escolhe
verificações; `--nucleo-desde AAAA-MM-DD` mede os ciclos não refeitos depois de uma troca do núcleo. Limiares
provisórios (constantes no topo de cada módulo), a calibrar com as primeiras semanas.

| Componente | Medida | Alerta | Módulo |
|---|---|---|---|
| Mercante | CE e item mais novos carregados (pela chave primária) | > 6 h atenção; > 24 h falha | `fontes.py` |
| API Recintos | por tipo de evento: posição do extrator (ponteiro ou evento mais novo); por recinto: últimas 24 h × as **mesmas 24 h da semana anterior**; rejeições da carga | extrator > 6 h: falha; recinto ativo (≥ 50) com < 10%: atenção (com o último evento e o ponteiro dele); queda geral < 50% sem recinto que a explique; rejeições ≥ 100 e > 3× | `fontes.py` |
| Imagens (MongoDB) | imagem mais nova (`uploadDate`, UTC); por recinto: últimas 24 h × mesmas 24 h da semana anterior; atraso (chegada − escaneamento); ponteiro '99' do extrator | > 6 h: falha; recinto ativo (≥ 20) com < 10%; queda geral < 50%; atraso mediano > 48 h | `imagens.py` |
| Ciclo — serviço | alertas do acompanhamento (14 dias) | "serviço parado"/sem marcas: falha; outros: atenção | `ciclo.py` |
| Ciclo — invariantes | `audita_ciclos.Auditoria` nos desembarques das últimas 48 h | > 0 | `ciclo.py` |
| Ciclo — refaz | ciclos dos últimos 30 dias processados antes de `--nucleo-desde` | > 0 (com o comando do refaz) | `ciclo.py` |
| Ciclo — cobertura | % com CE e % dos escaneados com imagem, desembarques de 9 a 2 dias atrás × a semana anterior | queda > 10 pontos | `ciclo.py` |
| Pesagens | % possível fraude e % digitação da pesagem principal, 7 dias × 21 anteriores | > 2× (com ≥ 20 pesagens) | `ciclo.py` |
| Risco histórico | marca da extração, IDs do Mercante por ler, fila da pontuação, faixas | marca > 6 h; > 100 mil IDs; fila > 50 mil | `aplicacoes.py` |
| Busca por texto | Meilisearch no ar; IDs do Mercante por indexar (precisa de `NAVII_MEILI_CHAVE_ESCRITA`) | fora: falha; > 20 mil: atenção; sem chave: não medido | `aplicacoes.py` |
| NAVI-I | tempo no servidor da 1ª página das pesquisas de 24 h (a coleta de uso grava) | p95 > 10 s ou > 5% lentas (com ≥ 20 pesquisas) | `aplicacoes.py` |
| Fichas de FMA | Ficha de FMA mais nova (`fma_update.py` do Fichas) | > 3 dias | `aplicacoes.py` |

Próximos passos possíveis: cruzar, por recinto, inspeções transmitidas × imagens recebidas (precisa do de-para do
`metadata.recinto` para o código do recinto); erros 5xx do NAVI-I (log do gunicorn); espaço em disco e programas do
supervisor; aba "Saúde" no menu NAVII lendo o último relatório.

## Decisões em aberto (resumo)

1. Refazer agora no servidor as partes que falharam (P0.1).
2. Onde o health check aparece além do log diário: e-mail do cron, aba "Saúde" (P0.3).
3. Priorizar o parser do Mercante já (P1.4): é a única perda irreversível da lista.
4. Aprovar a revisita de imagens (P1.5) e a contagem assíncrona (P1.6), propostas desde 13/10 e 02/10.
5. Gravar o modo de exibição na coleta de uso (P2.11).
6. Quando começar unidade/equipe do usuário (P2.9).
