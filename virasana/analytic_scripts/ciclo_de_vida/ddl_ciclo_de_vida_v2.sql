-- =====================================================================================================
-- Ciclo de Vida v2 — etapa 4 do plano (CICLO_DE_VIDA.md): DDL ADITIVA — PROPOSTA, NÃO EXECUTADA.
-- Revisão e execução pelo usuário/DBA (MariaDB 10.11), de preferência primeiro em homologação.
--
-- Tudo é aditivo e aceita NULL: o loop antigo (manager_ciclo_de_vida.py) e as telas que leem as
-- tabelas (NAVI-I, completude de escaneamentos) continuam funcionando sem mudança. Só o serviço novo
-- (etapa 5) preenche as colunas novas.
--
-- Bloqueio: ADD COLUMN no fim da tabela é INSTANT no MariaDB 10.11 (não copia a tabela); os índices vão
-- num ALTER separado, INPLACE e sem LOCK — a tabela segue em uso.
-- Rollback no fim do arquivo.
-- =====================================================================================================

-- 1. Ciclo: situação calculada, dados do contêiner e do vazio -----------------------------------------
ALTER TABLE ciclodevida_ciclo_de_vida_importacao
    ADD COLUMN situacao VARCHAR(12) NULL COMMENT 'aberto, concluido, incompleto, abandono',
    ADD COLUMN fluxo VARCHAR(12) NULL COMMENT 'operador, tra, passagem, baldeacao, indefinido',
    ADD COLUMN motivo VARCHAR(200) NULL COMMENT 'motivo da situação (ex.: entregue pelo operador)',
    ADD COLUMN confianca_ce VARCHAR(5) NULL COMMENT 'alta, media, baixa — do CE principal (filhote)',
    ADD COLUMN vazio TINYINT(1) NULL COMMENT 'desembarque de contêiner vazio (manifesto do Mercante)',
    ADD COLUMN numeroManifesto CHAR(15) NULL COMMENT 'vazio: manifesto em que consta (conteinervazioresumo)',
    ADD COLUMN escala CHAR(15) NULL COMMENT 'escala declarada no desembarque',
    ADD COLUMN tipoConteiner CHAR(4) NULL COMMENT 'tipo ISO 6346 informado nos eventos',
    ADD COLUMN oog TINYINT(1) NULL COMMENT 'potencial OOG: open top ou plataforma/flat rack',
    ADD COLUMN peso_balanca DECIMAL(9, 2) NULL COMMENT 'peso bruto da balança no desembarque',
    ADD COLUMN peso_manifesto DECIMAL(11, 2) NULL COMMENT 'peso bruto de manifesto no desembarque',
    ADD COLUMN tara DECIMAL(9, 2) NULL,
    ADD COLUMN peso_menos_tara DECIMAL(11, 2) NULL COMMENT 'balança se pesou; senão, manifesto',
    ADD COLUMN embarque_id BIGINT NULL COMMENT 'passagem/baldeação: embarque que fechou o ciclo',
    ADD COLUMN data_embarque DATETIME NULL,
    ADD COLUMN entrada_seco_implicita_de VARCHAR(10) NULL
        COMMENT 'inspecao/pesagem: entrada no seco não informada, deduzida (entrada_seco_id = NULL)',
    ADD COLUMN versao_heuristica VARCHAR(10) NULL COMMENT 'NULL = gravado pelo loop antigo',
    ADD COLUMN processado_em DATETIME NULL,
    ALGORITHM = INSTANT;

ALTER TABLE ciclodevida_ciclo_de_vida_importacao
    ADD INDEX ix_cdv_situacao (situacao),
    ADD INDEX ix_cdv_fluxo (fluxo),
    ADD INDEX ix_cdv_vazio (vazio),
    ADD INDEX ix_cdv_manifesto (numeroManifesto),
    ADD INDEX ix_cdv_oog (oog),
    ADD INDEX ix_cdv_processado (processado_em),
    ALGORITHM = INPLACE, LOCK = NONE;

-- 2. Escaneamentos: confiança, motivo e recinto de cada ligação ---------------------------------------
ALTER TABLE ciclodevida_ciclo_de_vida_importacao_escaneamentos
    ADD COLUMN confianca VARCHAR(5) NULL COMMENT 'alta, media, baixa',
    ADD COLUMN motivo VARCHAR(200) NULL COMMENT 'ex.: antes da entrada (scanner no gate)',
    ADD COLUMN codigoRecinto CHAR(7) NULL COMMENT 'recinto que informou a inspeção',
    ADD COLUMN fora TINYINT(1) NULL COMMENT 'escaneamento fora (passagem antes do TRA / terceirizado)',
    ALGORITHM = INSTANT;

-- 3. Ciclo <-> CE: todos os CEs da família (master e filhotes) com o contêiner nos itens --------------
--    O NAVI-I passa a achar o ciclo pelo card do master E do filhote (pendência 3a do CONTEXTO.md).
CREATE TABLE IF NOT EXISTS ciclodevida_ciclo_ce (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    ciclo_de_vida_id BIGINT NOT NULL,
    numeroCEmercante CHAR(15) NOT NULL,
    papel VARCHAR(8) NOT NULL COMMENT 'filhote, master, simples',
    principal TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 = o CE gravado em numeroCEmercante do ciclo',
    confianca VARCHAR(5) NULL,
    create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID),
    UNIQUE KEY uq_ciclo_ce (ciclo_de_vida_id, numeroCEmercante),
    KEY ix_ciclo_ce_ce (numeroCEmercante),
    CONSTRAINT fk_ciclo_ce_ciclo FOREIGN KEY (ciclo_de_vida_id)
        REFERENCES ciclodevida_ciclo_de_vida_importacao (ID) ON DELETE CASCADE
);

-- 4. Marcas d'água do serviço por eventos (último id lido de cada tabela da API) ----------------------
CREATE TABLE IF NOT EXISTS ciclodevida_marca_dagua (
    tabela VARCHAR(60) NOT NULL,
    ultimo_id BIGINT NOT NULL,
    atualizado_em DATETIME NOT NULL,
    PRIMARY KEY (tabela)
);

-- =====================================================================================================
-- ROLLBACK (só se for preciso desfazer; apaga os dados das colunas/tabelas novas)
-- =====================================================================================================
-- DROP TABLE IF EXISTS ciclodevida_marca_dagua;
-- DROP TABLE IF EXISTS ciclodevida_ciclo_ce;
-- ALTER TABLE ciclodevida_ciclo_de_vida_importacao_escaneamentos
--     DROP COLUMN confianca, DROP COLUMN motivo, DROP COLUMN codigoRecinto, DROP COLUMN fora;
-- ALTER TABLE ciclodevida_ciclo_de_vida_importacao
--     DROP INDEX ix_cdv_situacao, DROP INDEX ix_cdv_fluxo, DROP INDEX ix_cdv_vazio, DROP INDEX ix_cdv_manifesto,
--     DROP INDEX ix_cdv_oog, DROP INDEX ix_cdv_processado,
--     DROP COLUMN situacao, DROP COLUMN fluxo, DROP COLUMN motivo, DROP COLUMN confianca_ce, DROP COLUMN vazio,
--     DROP COLUMN numeroManifesto, DROP COLUMN escala, DROP COLUMN tipoConteiner, DROP COLUMN oog,
--     DROP COLUMN peso_balanca, DROP COLUMN peso_manifesto, DROP COLUMN tara, DROP COLUMN peso_menos_tara,
--     DROP COLUMN embarque_id, DROP COLUMN data_embarque, DROP COLUMN entrada_seco_implicita_de,
--     DROP COLUMN versao_heuristica, DROP COLUMN processado_em;
