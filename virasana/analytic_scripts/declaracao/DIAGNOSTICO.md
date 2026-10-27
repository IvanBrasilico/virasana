# Sinais de risco da informação declarada (itens 16–18) — histórico do diagnóstico

> **ADIADO (postponed) em 24/10/2026, decisão do usuário** — revisão da 7ª rodada: imagem e pesagem dos selecionados
> coerentes com o declarado. Voltar do começo no futuro. **Lições de método, o que se aprendeu dos dados e as ideias
> para a retomada (rede neural de peso e volume pela imagem, exemplos dos cursos de Aduana): `CONTEXTO.md`.** Este
> arquivo fica como o histórico rodada a rodada.

Saiu do `BACKLOG.md` na limpeza de 23/10/2026 (lá fica só o estado e a decisão que falta). Pedido do usuário de
22/10/2026; plano do piloto (suspenso) em `PLANO_PILOTO.md`.

Antes de implementar, cada uma precisa de um **diagnóstico nos dados** (como os F0 dos outros trabalhos). Código:
`virasana/analytic_scripts/declaracao/` — `regras_declaracao.py` (regras puras) e `diagnostico_declaracao.py` (só
leitura). Plano do passo 0 aprovado em 23/10/2026: um script, janela de 30 dias pela janela de ID; unidade o contêiner
cheio (FCL) na família master/filhote (17: só BL e MBL); referências = fichas positivas, RVFs e pesagens.

**Como validar (usuário, 23/10/2026): as Fichas são evidência FRACA** — são poucas e foram escolhidas por outros
critérios (a análise de risco delas não usava estas técnicas): força nelas não prova fraude e falta de força não
descarta um sinal. A decisão junta: (1) o sinal ser uma inconsistência em si (dado impossível ou copiado); (2) taxa de
marcação útil (0,5% a 5%); (3) corroboração independente quando existe (pesagem medida, peso da RVF); (4) as
referências comparadas com o **controle da mesma época** (vizinhos pelo ID), nunca só com a população recente;
(5) revisão de amostras no NAVI-I (imagens); e, se passar, (6) um piloto no NAVI-I como "sinal em observação",
medindo as Fichas abertas pelo card.

**1ª rodada em produção (23/10/2026, `declaracao.log`/`.csv`, 297 mil CEs, 317 mil contêineres FCL):**
- dados: números do Mercante sempre com ponto e 3 casas ("25040.000"); NCM com só 4 dígitos em 97% das linhas;
  cubagem vazia em 14% dos itens e em números redondos (estimada); embalagem vazia (94% no item; no NCM, 52% vazia e
  33% "99"); `codigoEmpresaNavegacao` = CNPJ da agência (99,8%); tempo: população 2,5 min, referências ~11 min;
- **descartados** como sinal de risco (decisão do usuário): 16a/16b (cubagem acima da capacidade, peso acima da carga
  máxima — erro de digitação, sem força nas referências; ficam contados como qualidade do dado); 16c/16d (pares raros
  carga × tipo e × embalagem — carga fora de gabarito legítima; embalagem inútil); 17b (cidade no número do BL — nenhum
  prefixo bate com a origem em 90%: HLCU 40%, ONEY 46%, MEDU 0%);
- **17a** (prefixo raro) parecia forte (35,8× nas positivas), mas era distorção: 1.054 das 1.471 marcações de uma
  agência que numera com ~100 prefixos (CNPJ 53364616000108; nenhuma referência) e TODO acerto em CEs antigos (2021,
  sobretudo) de empresas cuja numeração mudou — o mapa é dos últimos 30 dias. Agora só vale em empresa de prefixo
  dominante (90%+) e é medido contra o controle da mesma época;
- **18**: só a densidade ACIMA da típica do NCM separou (2,4% marcados; positivas 10,4×, RVF 4,1×, falsa declaração
  8,9×); abaixo, nada. Mas os acertos são todos antigos (27 de 36 positivas de 2021) e a força cai no corte mais alto —
  pode ser época (cubagem declarada de outro jeito), daí o controle. Peso atípico (18a) acompanha a pesagem
  divergente (1,9×; 15,6× com NCM de 8 dígitos): erro de declaração do peso, não fraude.

**2ª rodada (23/10/2026, `declaracao2.log`; controle da mesma época = vizinhos de cada referência pelo ID):**
- **18a+ e 18b+ sobreviveram ao controle**: densidade acima (2,4% marcados) — positivas 36/146 contra 4,6% no controle
  (**5,4×**; contra a população dava 10,4×: metade era época), RVF 3,1×, RVF com peso divergente 4,7×, falsa
  declaração 4,7×; peso acima (0,9%) — positivas 5,7×, contrafação/contrabando ~6× (poucos CEs) e a pesagem confirma
  (divergente ÷ coerente = 5,7×). As direções "abaixo" não separam (≤ 0,7× o controle): peso declarado errado;
- **17a era época**: 389× contra a população, 1,4× contra o controle — descartado;
- **16e** (cubagem copiada) marca 9,4% dos CEs com 2+ contêineres, com cubagens redondas (68, 60, 50, 40 m³): confirma
  o dado inventado (1,8× na pesagem divergente ÷ coerente), mas não o risco (~1× o controle) — fica como qualidade do
  dado. Falha conhecida: NCM sem contêiner vira "mesma mercadoria" para o CE inteiro;
- os marcados vêm em embarques inteiros (dezenas de contêineres do mesmo importador e NCM, com o mesmo valor): o
  "atípico para o NCM" pode ser o padrão daquele importador — refinamento possível: comparar também com o histórico
  do importador.

**3ª rodada (23/10/2026, da estação de trabalho — `declaracao3.log`, ~3 min de população):** toda a força do 18b+
está na **cubagem em m³ inteiro** (estimada): densidade acima com cubagem redonda (1,2% marcados) — positivas 32/146
contra 2,3% no controle (**9,5×**), RVF 5,8×, RVF com peso divergente 6,4×, falsa declaração 6,3×; com cubagem com
fração (1,2%) — positivas 1,1×, RVF 0,8×. Ou seja: cubagem estimada E mercadoria pesada demais para ela (o dado
inventado do 16e só separa junto com a incoerência física). 18+ (peso ou densidade acima): positivas 5,4×, RVF 2,9×.
A lista de revisão saiu com erros de digitação no topo (peso acima da carga máxima; 73 "kg" num 40', em toneladas).

**4ª rodada (23/10/2026):** sem os pesos impossíveis, o 18b+ com cubagem redonda ficou igual (positivas 9,5×, RVF
5,8×, RVF com peso divergente 6,4×). O controle da combinação não saiu (bug: código da combinação sem o prefixo "18",
zero avaliáveis — corrigido, com teste). A lista de revisão mostrou cubagem SIMBÓLICA (1 m³ num 40', 6 m³) e NCM 9999.

**5ª rodada (23/10/2026) — o candidato está definido:** densidade acima da típica do NCM com cubagem em m³ inteiro e
plausível (10%+ da capacidade): 1,18% dos CEs (1.867 em 30 dias), positivas 32/146 contra 2,1% no controle (**10,4×**),
RVF 6,0×, falsa declaração 6,3×, RVF com peso divergente 6,4×. Os controles mostram que é a COMBINAÇÃO: a cubagem
redonda sozinha (56% dos contêineres) dá 1,3×; densidade acima com cubagem com fração, 1,1×; cubagem simbólica, 0
positivas. Leitura: o peso é real, a cubagem foi inventada e a densidade não combina com o NCM declarado — a mercadoria
pode não ser a declarada. Lista de revisão (`revisao5.csv`) limpa.

**Revisão do usuário (23/10/2026) — suspende o piloto:** nos CEs de Santos da `revisao5.csv` (com pesagem e
imagem; todos de 20'), o peso declarado confere com o produto e a imagem: a anomalia é a CUBAGEM. O típico de
densidade não separa 20' de 40' (o de peso separa) e muitos 40' declaram a capacidade (68/76 m³) como cubagem — quem
declara o volume real num 20' parece "denso" (cerveja a 996 kg/m³ é física). 16 dos 30 da lista são de 20' (~17%
dos contêineres). Conclusão provisória: o sinal mede o jeito de declarar a cubagem, não a mercadoria; os 10× nas
positivas podem ser o perfil do declarante. Também (usuário): o NAVI-I não mostra a cubagem declarada, nem no card do
CE nem no dos contêineres.

**6ª rodada (23/10/2026, opção A do usuário — `declaracao6.log`, `declaracao6.csv`, `revisao6.csv`) — o candidato
NÃO se sustenta.** Típico da densidade por NCM E tamanho do contêiner (como o do peso); concentração por importador
(raiz do CNPJ) e empresa de navegação; tamanho dos marcados.

- **A força era, na maior parte, a mistura de tamanhos.** O candidato (18b+ redonda plausível) marca 1,05% dos CEs
  (1.667; eram 1.867), mas os acertos caem: positivas 10/146 (eram 32), contra 1,15% no controle (**6,0×**; era
  10,4×); RVF 73/1840 (eram 151), **4,0×** (era 6,0×); RVF com peso divergente 2,4×. Os 20' agora são 14,9% dos
  marcados (22,0% dos contêineres).
- **Não é concentração de declarante:** 463 importadores (o maior com 22 CEs; sem CNPJ: 620, 37% dos marcados
  contra 28% dos avaliáveis); empresas de navegação entre 0,6% e 1,8% de marcados cada. Acertos: positivas de 7
  importadores (um com 3), RVF de 56.
- **O que sobrava das RVF era bagagem** (análise do CSV): NCM **9797** (bagagem desacompanhada, `bagagens_manager`)
  responde por 49 dos 101 acertos de RVF do 18b+ — a bagagem tem RVF de rotina (não é seleção por risco) e cubagem
  própria; um contêiner de mudanças consolidadas (8 CEs) conta 8 vezes. Na população são só 22 CEs.
- **Sem a bagagem, densidade acima com peso NORMAL para o NCM não separa:** RVF 1,52% contra 1,62% no controle
  (0,9×); positivas 6/146 (2,5×, poucos casos). É o jeito de declarar a cubagem (revisão do usuário: o peso confere).
- **O que resta é o PESO:** peso por contêiner acima do típico do NCM no mesmo tamanho E densidade acima (a cubagem
  não acompanha) — "18a+ e 18b+": 0,61% dos CEs (961 em 30 dias, ~32/dia); positivas 6 (7,2× o controle), RVF 22
  (2,2×), contrafação 3/37 (18,6×), contrabando 4/36 (19,6×), falsa declaração 2/19 (9,5×) — poucos casos, mas nas
  infrações esperadas (capítulos 85 e 64 nos acertos: eletrônicos e calçados com 14–28 t num 40', típico de 4–13 t).
  Peso acima sem densidade acima (314 CEs) não acerta quase nada. Na população, o 18a+ é dominado pelos capítulos 84
  e 87 (máquinas e veículos, NCM de 4 dígitos heterogêneo).
- `revisao6.csv`: 21 dos 30 são "só densidade" (sal, papel, leite em pó, couro, tabaco, algodão com 10–30 m³ num 40'
  — cubagem abaixo do volume físico da mercadoria); 9 são de peso (8418, 8539, 7020, 8709, 8422, 8451, 4415, 7311,
  8704, com a cubagem perto da capacidade).

**Conclusão:** o candidato da cubagem está fechado ("não vale": era a mistura de tamanhos, a bagagem e o jeito de
declarar). Sobra uma hipótese, que não depende da cubagem: **carga pesada demais para a mercadoria declarada**
(18a+ com 18b+). Decisão do usuário (23/10/2026): **seguir com o piloto** — dela.

**7ª rodada (23/10/2026 — `declaracao7.log`, `declaracao7.csv`, `revisao7.csv`) — a força do peso se mantém sem a
bagagem.** Bagagem (NCM 9797) fora do 18; candidato "18a+ e 18b+"; concentração por capítulo; lista de revisão com
os CEs com pesagem primeiro.

- **A bagagem era 1/3 das referências:** sem ela, as fichas positivas avaliáveis caem de 146 para 91 e as RVF de
  1.840 para 1.260.
- **Candidato:** 959 CEs (0,61%); positivas 6/91 (**11,4×** o controle), RVF 20/1.260 (**2,9×**), contrafação 3/37
  (18,6×), contrabando 4/35 (20,0×), falsa declaração 2/16 (11,1×). Pesagem: 1,93% dos "divergência" contra 0,29%
  dos "coerente" — parte pode ser peso declarado errado.
- **Máquinas e veículos (84 e 87) são ruído:** 571 dos 959 marcados, 1 acerto de RVF, 0 positivas (abaixo do
  controle). **Sem eles** (análise do CSV): 388 CEs (**0,25%**, ~13/dia), com todos os acertos de positivas (6),
  contrafação (3) e contrabando (4) e 19 das 20 RVF — ~5× o controle nas RVF (15 dos 28 controles marcados ficam).
  Acertos de eletrônicos (85), calçados (64), vidro (70) e do 96.
- **Concentração:** 254 importadores, os 10 maiores com 48,7% (um com 82 CEs, 52% dos dele; outro com 28, 72%):
  embarques inteiros. Os acertos vêm de importadores diferentes (positivas: 3; RVF: 12) — e muitos de CEs sem CNPJ
  ou CPF do consignatário (positivas 4 de 6, RVF 9 de 20, contra 25% dos marcados): outro fator, a olhar à parte.
  94% dos marcados são de 40'.
- **Defeito (meu):** o candidato era por CE — em 38 CEs da janela, um contêiner com o peso acima e outro com a
  densidade acima; na `revisao7.csv`, as linhas 8703 e 6910 misturam contêineres (vale o contêiner e o motivo do
  18a+).

**8ª versão (23/10/2026), a rodar quando der** (não bloqueia o P1): candidato por CONTÊINER (peso e densidade acima
no mesmo contêiner; a revisão leva o contêiner e os motivos dele); validação também do candidato sem 84 e 87 — os
números exatos da regra proposta e a linha de base da carga do P1.
`virasana-venv/Scripts/python.exe virasana/analytic_scripts/declaracao/diagnostico_declaracao.py --saida
declaracao8.log --csv declaracao8.csv --revisao revisao8.csv`.

**Cubagem no card do NAVI-I — publicada (23/10/2026, pedido do usuário):** cubagem declarada
do CE e de cada contêiner, com ocupação e densidade (`navii/CONTEXTO.md`, item 23).

**Plano do piloto (reescrito em 23/10/2026 para o PESO, para aprovação):** `virasana/analytic_scripts/declaracao/PLANO_PILOTO.md` — selo "carga pesada para a mercadoria declarada — em observação"; módulo `virasana/declaracao/` no caminho do risco histórico (tabelas novas que guardam as medidas, etapa no serviço do ciclo, selo e filtro no NAVI-I, medição pela coleta de uso), fases P0–P4 e 8 decisões. A 1ª versão (cubagem) foi suspensa e derrubada pela 6ª rodada.

**Revisão do usuário da 7ª rodada (24/10/2026) — ADIADO:** as seleções não fazem sentido; na imagem e na pesagem, os
contêineres são coerentes com o declarado. Diagnóstico do usuário: o pipeline não separou sinal de ruído; usar as
Fichas como rótulo positivo foi provavelmente um erro (não foram escolhidas por análise de peso nem de inconsistência
no BL; a maioria não era falsa declaração). A própria pesagem tem problemas de qualidade (MULTILOG, ECOPORTO e outros:
muita variação, quase sem tara do conjunto; talvez a pesagem da tara venha com o contêiner da viagem). Voltar do
começo no futuro — ideias e lições em `CONTEXTO.md`. A 8ª versão do código fica pronta, sem rodar.

- **16. Tipo do contêiner × cubagem × carga × embalagem declaradas.** Inconsistência entre o que o contêiner comporta e
    o que foi declarado (ex.: cubagem acima da capacidade do 20', embalagem que não combina com a mercadoria ou com o
    tipo — granel em dry, carga refrigerada fora de reefer) sugere declaração inventada. Dados: `itensresumo.isoCode`,
    `cubagemM3`, `codigoTipoEmbalagem`, `qtdeItens`, `indicadorUsoParcial`; `ncmitemresumo.codigoTipoEmbalagem`,
    `qtdeVolumes`, `itemEmbaladoMadeira`; `conhecimentosresumo.cubagem` e `descricao`; tamanho/tipo em
    `navii/referencias/tipos_conteiner.json` (faltam as capacidades de referência). O diagnóstico responde: a cubagem
    vem preenchida e com valor real (ou padrão/zero)? Qual a distribuição por tipo de contêiner? Que tabela de
    embalagem o Mercante usa?
- **17. Número do BL × origem e companhia declaradas.** O `conhecimentosresumo.numConhecimento` costuma trazer o prefixo
    da companhia (código SCAC, ex.: MAEU, MEDU, COSU) e às vezes o porto/local de emissão; deveria bater com a
    empresa de navegação (`codigoEmpresaNavegacao`), o porto de origem (`portoOrigemCarga`) e o terminal de
    carregamento. Formato estranho para a companhia, ou origem no número diferente da declarada, pode indicar
    informação falsa. O diagnóstico responde: os padrões de numeração por companhia e por tipo de BL (no HBL de
    agente o formato é livre — provavelmente só o MBL/BL serve), a tabela SCAC × empresa e a taxa de divergência
    "normal".
- **18. Peso declarado × volume × densidade esperada pelo NCM.** Nos contêineres com **um só NCM**, a densidade
    (peso bruto declarado ÷ cubagem) pode ser comparada com a densidade típica daquele NCM, aprendida da própria base
    (mediana e dispersão por NCM, como os pesos do risco histórico); com mais de um NCM não há a quantidade de cada
    um, então fica de fora (ou só uma faixa possível). Vale cruzar com o **peso medido** das pesagens (P1–P3): a
    pesagem confirma ou derruba o declarado. O diagnóstico responde: quantos contêineres têm um só NCM, com cubagem
    preenchida, quantas amostras por NCM (em que nível — 4, 6 ou 8 dígitos) e quão estreita é a densidade de cada um.

