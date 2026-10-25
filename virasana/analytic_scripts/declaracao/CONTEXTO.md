# Sinais da informação declarada (itens 16–18) — CONTEXTO para a retomada

> **ADIADO (postponed) em 24/10/2026, decisão do usuário.** Nada disto está em produção (só a cubagem declarada no
> card do NAVI-I, que é informação, não sinal). A análise precisa **voltar do começo**, com outro método e outros
> rótulos. Este arquivo junta o que se aprendeu em 7 rodadas (23–24/10/2026) para não repetir os erros. O histórico
> rodada a rodada está em `DIAGNOSTICO.md`; o plano do piloto (adiado) em `PLANO_PILOTO.md`.

## 1. Por que parou

Na revisão da 7ª rodada, o usuário olhou a imagem e a pesagem dos contêineres selecionados (`revisao7.csv`, sinal
"carga pesada para a mercadoria declarada") e **as seleções não faziam sentido: imagem e pesagem coerentes com o
declarado**. O mesmo já tinha acontecido na 5ª rodada com o sinal da cubagem (o peso conferia; a anomalia era só o
jeito de declarar a cubagem). Leitura do usuário: o pipeline não separou sinal de ruído — e usar as Fichas como
rótulo positivo foi, provavelmente, o erro de base.

## 2. Lições de método (o que deu errado)

1. **Fichas e RVF não servem de rótulo positivo para estes sinais.** Não foram selecionadas por análise de peso,
   volume ou inconsistência no BL; a maioria não era falsa declaração; são poucas (sem a bagagem: 91 fichas
   positivas e 1.260 RVF em 30 dias de população); **um terço era bagagem** (NCM 9797, RVF de rotina). A força
   "contra as Fichas" mede a semelhança com o perfil de seleção delas (NCM, importador, operação), não fraude.
2. **Caminho dos garfos (overfitting ao rótulo).** Cada rodada testou ~15 combinações e escolheu a de maior razão
   sobre 6 a 30 acertos. Todo candidato "validado" com 6–10× caiu na verificação seguinte (mistura de tamanhos de
   contêiner, bagagem, defeito por CE, revisão do usuário). Com tão poucos acertos, alguma combinação sempre dá 10×.
3. **O controle da mesma época só controla a época** (vizinhos pelo ID): não controla NCM, importador, recinto nem o
   tipo de operação. Ajudou a derrubar o 17a (era época e agência), mas não protege do resto.
4. **A revisão qualitativa (imagem + pesagem, pelo usuário) foi a única evidência confiável** — e veio por último.
   Na retomada, ela vem primeiro: partir de casos conhecidos e olhar exemplos antes de medir razões.
5. **A unidade é o contêiner**, não o CE: na 7ª rodada o candidato era por CE e misturava contêineres (corrigido na
   8ª versão do código, não rodada).

**Para a retomada:** hipótese e critério de sucesso escritos **antes** de olhar os rótulos; um período para
descobrir e **outro para confirmar** (sem mexer na regra entre os dois); número mínimo de acertos; rótulos melhores
(seção 5); revisão de exemplos no NAVI-I logo no início.

## 3. O que se aprendeu dos dados (vale para qualquer retomada)

**Mercante (`conhecimentosresumo`, `itensresumo`, `ncmitemresumo`):**

- Números em texto com ponto e 3 casas ("25040.000" = 25.040 kg). NCM com **4 dígitos em 97%** das linhas: posições
  heterogêneas. NCM fora dos capítulos 01–97 (9999, 0000) não é NCM.
- **Família master/filhote**: o peso e a cubagem do contêiner vêm do item do master; sem ele, a soma dos filhotes
  (`pesagens_ciclo.peso_da_familia`); até 3 níveis (MBL > MHBL > HBL). Contêiner de uso parcial (LCL) fica de fora.
- **Cubagem é pouco informativa:** 56% dos contêineres com cubagem em m³ inteiro; muitos 40' declaram a
  **capacidade** (68, 76 m³) como cubagem; há cubagem simbólica (1 m³ num 40'); cubagem acima da capacidade (16a) e
  peso acima da carga máxima (16b) são digitação. Em 9,4% dos CEs com 2+ contêineres da mesma mercadoria, a cubagem
  é copiada entre contêineres de pesos diferentes (16e) — dado inventado, sem relação com risco (~1× o controle).
- **Peso e densidade "típicos do NCM"** só fazem sentido **por tamanho do contêiner** (20' e 40' têm padrões de
  declaração diferentes). Máquinas e veículos (capítulos **84 e 87**) têm variância enorme de peso na mesma posição:
  60% dos marcados, nenhum acerto.
- **NCM 9797 = bagagem desacompanhada**: pesos e volumes "inventados", RVF de rotina — tirar de qualquer análise de
  carga comercial (e das referências).
- **Número do BL (17a)**: prefixo tipo SCAC raro para a empresa de navegação era época e agência (1,4× o controle);
  no HBL o número é do agente; só algumas empresas têm prefixo dominante.
- **Consignatário sem CNPJ/CPF**: 28% dos CEs avaliáveis; apareceu muito entre os acertos (4 de 6 fichas, 9 de 20
  RVF) — fator a olhar à parte, se um dia interessar.
- Capacidades por tipo de contêiner em `navii/referencias/tipos_conteiner.json` (`capacidades`; fontes
  Hapag-Lloyd/Maersk, a conferir).
- Leitura sempre pela **janela de ID** (`create_date` = última alteração); a população de 30 dias leva ~3 min da
  estação de trabalho.

**Pesagem "real" (`ciclodevida_pesagem`) — ainda não é verdade de campo (usuário, 24/10/2026):**

- Parte dos recintos — o usuário viu **MULTILOG e ECOPORTO, entre outros** — tem muita variação na pesagem e quase
  não pesa a **tara do conjunto** (caminhão + contêiner vazio).
- Suspeita: transmitem a **pesagem da tara com o `numeroConteiner` da viagem preenchido**, o que gera pares
  "pesagem × declarado" falsos.
- Antes de usar a pesagem como rótulo ou para treinar modelo: diagnóstico de qualidade por recinto (tem tara? a
  variação é plausível? há pesagens duplicadas ou de tara com contêiner?). Regra do projeto: recinto que transmite
  errado é **cobrado** — nada de exceção por recinto no código; no máximo, escolher para o treino os recintos cujos
  dados passam no diagnóstico.

## 4. Resultados, em uma tabela (detalhe em `DIAGNOSTICO.md`)

| Rodada | O que mudou | O que se viu |
|---|---|---|
| 1ª–2ª | sinais 16, 17, 18; controle da mesma época | 17a era época/agência; 16a/16b/16e só qualidade do dado; 18b+ (densidade acima) separa |
| 3ª–5ª | cubagem redonda × fração; sem pesos impossíveis; cubagem simbólica | "densidade acima com cubagem redonda plausível": positivas 10,4×, RVF 6,0× |
| revisão 5ª | usuário, casos de Santos | peso confere; a anomalia era o jeito de declarar a cubagem; típico misturava 20' e 40' |
| 6ª | típico por tamanho | positivas 32 → 10, RVF 151 → 73; metade das RVF restantes era bagagem; sobrou o peso |
| 7ª | sem bagagem; candidato "peso E densidade acima" | positivas 11,4×, RVF 2,9×; 84/87 ruído (sem eles: 0,25% dos CEs) |
| revisão 7ª | usuário, imagem e pesagem | seleções sem sentido: imagem e pesagem coerentes → **adiado** |

## 5. Ideias para a retomada (usuário, 24/10/2026)

**Medida independente do contêiner pela imagem.** Treinar uma rede neural que estime o **peso** e o **volume
ocupado** do contêiner pela imagem do escâner e gravar no banco; comparar com o declarado (e com a pesagem, onde ela
for confiável). Isso dá uma medida que não depende das Fichas nem da declaração.

- **Precedente no legado (AJNA-PADMA), verificar antes de treinar outro:**
  - `virasana/scripts/tfservingupdate.py`: modelos `peso` e `vazio` via TF Serving. Entrada do `peso` 288×144 sobre
    o recorte do contêiner (`bbox`); saída reescalada `pred * 7558.96 + 18204.96`. Grava em
    `metadata.predictions[].peso` (Mongo, `fs.files`). Último commit em 2021.
  - `virasana/integracao/padma.py`, `integracao/carga.py` (`diferencapeso`/`alertapeso`: imagem × declarado;
    `diferencapeso2`/`alertapeso2`: balança × declarado), `scripts/pesocargaupdate.py`,
    `scripts/pesocargabalancaupdate.py`, e as telas da auditoria (`models/auditoria.py`, filtros 03 e 04).
  - Perguntas: ainda roda? Que fração das imagens tem `predictions.peso`? Qual o erro contra pesagens confiáveis?
- **Treino:** pares imagem × peso de pesagem confiável (recintos que passam no diagnóstico da seção 3), por tipo de
  contêiner e escâner; o volume ocupado é um modelo novo (não há rótulo pronto — talvez segmentação da carga na
  imagem).

**Exemplos dos cursos de Aduana** — hipóteses a testar **uma a uma**, partindo de casos conhecidos:

- **40' com carga muito densa** que normalmente vai em 20' (a mercadoria atinge o limite de peso do 20' em pouco
  volume): medir a proporção 20'/40' por NCM e marcar a escolha incomum de tamanho;
- **mercadoria ou volume incompatíveis com o tipo de contêiner** (reefer sem carga refrigerada, open top/flat rack
  sem carga fora de gabarito, granel em dry);
- **BL, manifesto, mercadoria e siglas de companhias incompatíveis com a rota** (prefixo do BL × empresa de
  navegação × porto de origem e escalas);
- **volumes arredondados** (ex.: 68 CBM): sozinhos não separam (56% dos contêineres) — no máximo como confirmação.

**Rótulos melhores que as Fichas:** RVF com divergência medida (peso, quantidade, mercadoria diferente da
declarada), TG e perdimento por falsa declaração, apreensões confirmadas — e a medida pela imagem, quando existir.

## 6. O que existe no código (para não refazer)

- `regras_declaracao.py` — regras puras: capacidade e carga máxima por tipo (16a/16b), cubagem copiada (16e),
  prefixo do BL (17a), típicos de peso e densidade do NCM por tamanho (18a/18b; mediana e MAD em log, níveis 8/6/4,
  30+ amostras, piso de dispersão), cubagem redonda e simbólica; bagagem fora do 18. Testes:
  `tests/test_regras_declaracao.py`.
- `diagnostico_declaracao.py` — 8ª versão (**não rodada**): população pela janela de ID, famílias, referências
  (fichas, RVF por infração, RVF × declarado, pesagens), controle da mesma época, validação, concentração (importador,
  empresa, tamanho, capítulo), lista de revisão com os CEs com pesagem primeiro; candidato por contêiner. Testes:
  `tests/test_diagnostico_declaracao.py` (SQLite).
- `navii/referencias/tipos_conteiner.json` — `capacidades` (m³ e carga máxima por tipo).
- NAVI-I: **cubagem declarada no card** (do CE e de cada contêiner, com ocupação e densidade) — publicada em
  23/10/2026 (`navii/CONTEXTO.md`, item 23).
- Saídas das rodadas (`declaracao3`–`7.log`/`.csv`, `revisao*.csv`) **só na estação do usuário**, na raiz do
  virasana: o `.gitignore` ignora `*.log` e `*.csv`.
