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

## P0 — agora

1. **Terminar o refaz de 22/10.** O `--refaz-desde 2026-09-22` rodou da estação de trabalho e perdeu a rede: 24 de
   41 partes falharam (a parte 3 às 16:22, conexão reiniciada; as partes 19 a 41 às 17:27, rede inacessível).
   Só 85 mil dos 202.692 contêineres foram refeitos. O refaz anda do desembarque mais recente para o mais antigo, e
   **todos** os 118 ciclos com violação na 2ª auditoria (113 "CE principal com a carga saindo do porto" e 9 "TRA
   concluído sem a saída do seco"; 4 com as duas) estão entre 22/09 18:10 e 09/10 03:58 — exatamente as partes que falharam. Nada indica
   defeito do C6/C7/C8. Refazer **no servidor** (rede estável), em sequência, e auditar de novo — esperado: 0.
   Sobrando alguma, extrair com `caso_ciclo.py` e levar ao corpus.
2. **Refaz robusto** (ajuste pequeno): repetir a parte que falha (reconectando, com espera crescente); no fim,
   listar as partes com erro com o período de desembarque de cada uma e o comando para refazê-las; sair com código
   diferente de zero se sobrar parte com erro. *Decisão: aprovar.*
3. **Health check diário v1** (seção abaixo). *Decisão: aprovar o escopo e onde o resultado aparece.*

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

## Health check (proposta v1)

Objetivo: **um relatório curto por dia** que diga OK / ATENÇÃO / FALHA por componente, reaproveitando o que já mede
cada parte. Rodar no servidor por cron (ex.: 7 h), gravar em log; depois, se valer, uma aba "Saúde" no menu NAVII
para quem administra.

| Componente | Medida | Limite de alerta | Fonte |
|---|---|---|---|
| Ciclo — serviço | idade das marcas d'água; fila de eventos; evento mais novo da API por tabela | > 30 min; > 20 mil; > 6 h | `acompanha_ciclo_de_vida.py` (existe) |
| Ciclo — correção | violações das invariantes nos desembarques dos **últimos 2 dias** | > 0 | `audita_ciclos.py --dias 2` (existe; falta código de saída) |
| Ciclo — refaz | ciclos do período processados **antes** da última troca do núcleo (não refeitos) | > 0 depois de um refaz | **nova** (pelo `processado_em`) |
| Ciclo — cobertura | % com CE, % escaneamentos com imagem por recinto, abertos sem revisita, confiança baixa | queda > 10 p.p. sobre a semana anterior; > 12 h sem revisita | acompanhamento (existe; falta comparar com a semana anterior) |
| Pesagens | % divergência e % digitação da pesagem principal por dia | salto > 2× a média de 7 dias | **nova** (contagem simples) |
| Mercante | atraso da carga (CE mais novo carregado × agora) | > 6 h | **nova** (pelo ID, nunca varrer a tabela) |
| Risco histórico | extração em dia, fila da pontuação, versão, faixas (~1% alto, ~4% médio) | fila > 0 por 2 dias; faixa fora de 0,5–2× | `risco_historico/lotes.py --situacao` (existe) |
| Busca por texto | defasagem do índice (maior CE indexado × Mercante), nº de documentos, Meilisearch no ar | > 1 dia; serviço fora | `navii/indexador_texto.py --situacao` (existe) |
| NAVI-I web | tempo de resposta p50/p95 por dia, % de pesquisas lentas (≥ 10 s), erros | p95 > 10 s; lentas > 5% | **nova** (agrega os tempos que a coleta de uso já grava no Mongo) |
| Fichas | última Ficha de FMA criada pelo `fma_update.py` | > 2 dias úteis | **nova** (consulta simples) |

**Ajustes necessários para o health check funcionar**: refaz robusto (P0.2); `audita_ciclos.py` com código de saída
e modo rápido de 2 dias; medida "não refeitos"; comparação semana a semana no acompanhamento; agregação dos tempos
do NAVI-I; um script `saude_ajna.py` que chama os relatórios existentes (cada parte com o próprio limite de tempo,
uma falha não derruba as outras) e escreve o resumo.

## Decisões em aberto (resumo)

1. Refazer agora no servidor as partes que falharam (P0.1).
2. Aprovar o refaz robusto (P0.2) e o escopo do health check v1 (P0.3) — inclusive onde aparece (log, e-mail, aba).
3. Priorizar o parser do Mercante já (P1.4): é a única perda irreversível da lista.
4. Aprovar a revisita de imagens (P1.5) e a contagem assíncrona (P1.6), propostas desde 13/10 e 02/10.
5. Gravar o modo de exibição na coleta de uso (P2.11).
6. Quando começar unidade/equipe do usuário (P2.9).
