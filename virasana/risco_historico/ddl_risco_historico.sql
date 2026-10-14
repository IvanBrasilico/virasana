-- =====================================================================================================
-- Risco histórico por CE — F1 (virasana/risco_historico/CONTEXTO.md): DDL — PROPOSTA, NÃO EXECUTADA.
-- Revisão e execução pelo usuário/DBA (MariaDB 10.11), de preferência primeiro em homologação.
--
-- Só tabelas NOVAS: nada existente é alterado. Quem grava: virasana/risco_historico/lotes.py (e, no F3,
-- o serviço do ciclo de vida). Quem lê: a tela Risco histórico (Parâmetros em uso) e, no F4, o NAVI-I.
-- As colunas batem com virasana/risco_historico/tabelas.py (teste test_risco_historico_lotes).
-- Rollback no fim do arquivo.
-- =====================================================================================================

-- 1. Um CE (não exportação) desde a marca inicial: sinais extraídos do Mercante e, no F3, os pontos ------
CREATE TABLE IF NOT EXISTS risco_historico_ce (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    numeroCEmercante CHAR(15) NOT NULL,
    ce_id BIGINT NOT NULL COMMENT 'conhecimentosresumo.ID',
    dataEmissao VARCHAR(10) NULL,
    sinais TEXT NOT NULL COMMENT 'JSON {dimensão: [valores]}',
    ce_alterado_em DATETIME NULL COMMENT 'create_date do CE quando lido (= última alteração no Mercante)',
    ncm_id BIGINT NOT NULL COMMENT 'maior ncmitemresumo.ID usado nos sinais (0 = sem NCM)',
    criado_em DATETIME NOT NULL,
    sinais_em DATETIME NOT NULL,
    pontos DECIMAL(7, 3) NULL COMMENT 'F3: soma das parcelas',
    faixa VARCHAR(10) NULL COMMENT 'F3: alto, medio, baixo, sem_sinal',
    versao_id BIGINT NULL COMMENT 'F3: versão dos parâmetros usada',
    calculado_em DATETIME NULL,
    PRIMARY KEY (ID),
    UNIQUE KEY uq_risco_ce (numeroCEmercante),
    KEY ix_risco_ce_ce_id (ce_id),
    KEY ix_risco_ce_criado_em (criado_em),
    KEY ix_risco_ce_versao (versao_id),
    KEY ix_risco_ce_faixa_pontos (faixa, pontos)
);

-- 2. Versões dos parâmetros ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risco_historico_versao (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    criado_em DATETIME NOT NULL,
    motivo VARCHAR(60) NOT NULL COMMENT 'primeira versão, fichas positivas mudaram, versão vencida, forçada',
    assinatura CHAR(64) NOT NULL COMMENT 'sha256 das fichas positivas (id:peso)',
    fichas INT NOT NULL,
    total_k DECIMAL(10, 2) NOT NULL,
    total_n INT NOT NULL COMMENT 'CEs na população',
    populacao_desde DATETIME NOT NULL,
    constantes TEXT NOT NULL COMMENT 'JSON: m, regras por dimensão, grupos',
    PRIMARY KEY (ID)
);

-- 3. Peso de cada valor numa versão (memória de cálculo) ---------------------------------------------
CREATE TABLE IF NOT EXISTS risco_historico_parametro (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    versao_id BIGINT NOT NULL,
    dimensao VARCHAR(20) NOT NULL,
    valor VARCHAR(255) NOT NULL,
    descricao VARCHAR(200) NULL,
    fichas INT NOT NULL,
    importadores INT NOT NULL,
    k DECIMAL(10, 2) NOT NULL,
    n INT NOT NULL COMMENT 'CEs da população com o valor',
    vezes DECIMAL(14, 2) NOT NULL COMMENT 'quantas vezes a média',
    peso DECIMAL(6, 3) NOT NULL,
    motivo VARCHAR(60) NULL COMMENT 'por que o peso é zero',
    fichas_ids TEXT NOT NULL,
    PRIMARY KEY (ID),
    UNIQUE KEY uq_risco_parametro (versao_id, dimensao, valor),
    CONSTRAINT fk_risco_parametro_versao FOREIGN KEY (versao_id)
        REFERENCES risco_historico_versao (ID) ON DELETE CASCADE
);

-- 4. Marcas d'água da extração ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risco_historico_marca (
    nome VARCHAR(60) NOT NULL,
    ultimo_id BIGINT NULL,
    ultima_data DATETIME NULL,
    atualizado_em DATETIME NOT NULL,
    PRIMARY KEY (nome)
);

-- =====================================================================================================
-- ROLLBACK (só se for preciso desfazer; apaga os dados do risco por CE)
-- DROP TABLE risco_historico_parametro;
-- DROP TABLE risco_historico_versao;
-- DROP TABLE risco_historico_ce;
-- DROP TABLE risco_historico_marca;
-- =====================================================================================================
