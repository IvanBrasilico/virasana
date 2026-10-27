-- =====================================================================================================
-- Pesagens do Ciclo de Vida — P1 a P3 (CICLO_DE_VIDA.md, "Fora de Santos e pesagens"): DDL ADITIVA.
-- Revisão e execução pelo usuário/DBA (MariaDB 10.11), PRIMEIRO em homologação.
--
-- Tabela NOVA, só lida pelo NAVI-I e gravada pelo serviço do ciclo de vida (servico_ciclo_de_vida.py). Nenhuma
-- tabela existente muda. Sem ela, o serviço não grava pesagens e o NAVI-I não mostra o bloco nem aceita os filtros
-- de pesagem — o resto segue igual. O serviço confere a tabela AO INICIAR: depois de criá-la, reiniciar o serviço.
--
-- Uma linha por pesagem (apirecintos_pesagensveiculo) do contêiner dentro do ciclo: a da retirada no operador do
-- desembarque e a da chegada ao seco são comparadas com o peso declarado do contêiner; as outras ficam como dado
-- bruto. A PRINCIPAL (principal = 1, no máximo uma por ciclo) é a do card e a dos filtros.
-- Rollback no fim do arquivo.
-- =====================================================================================================

CREATE TABLE IF NOT EXISTS ciclodevida_pesagem (
    ID BIGINT NOT NULL AUTO_INCREMENT,
    ciclo_de_vida_id BIGINT NOT NULL,
    pesagem_id BIGINT NOT NULL COMMENT 'apirecintos_pesagensveiculo.id',
    data DATETIME NOT NULL COMMENT 'dataHoraOcorrencia da pesagem',
    codigoRecinto CHAR(7) NULL,
    placa VARCHAR(10) NULL,
    semirreboque VARCHAR(10) NULL,
    operacao VARCHAR(60) NOT NULL COMMENT 'retirada no operador, chegada ao seco ou outra pesagem no ciclo',
    principal TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1 = a pesagem do card e dos filtros do NAVI-I',
    peso_bruto DECIMAL(11, 2) NULL COMMENT 'pesoBrutoBalanca',
    captura_automatica TINYINT(1) NULL,
    tara_pesagem_id BIGINT NULL COMMENT 'a pesagem do conjunto sem o contêiner (par na visita)',
    tara_conjunto DECIMAL(11, 2) NULL,
    origem_tara_conjunto VARCHAR(25) NULL COMMENT 'par na visita ou taraConjunto da API',
    tara_conteiner DECIMAL(9, 2) NULL,
    origem_tara_conteiner VARCHAR(20) NULL COMMENT 'informada ou típica do tamanho',
    peso_medido DECIMAL(11, 2) NULL COMMENT 'bruto - tara do conjunto - tara do contêiner',
    peso_declarado DECIMAL(11, 2) NULL COMMENT 'do contêiner na família de CEs (Item.pesoBruto)',
    origem_declarado VARCHAR(30) NULL COMMENT 'item do master, soma de N filhotes, item do CE/filhote',
    numeroCEmercante CHAR(15) NULL COMMENT 'CE principal do ciclo',
    diferenca_kg DECIMAL(11, 2) NULL,
    diferenca_pct DECIMAL(12, 2) NULL,
    razao DECIMAL(12, 4) NULL COMMENT 'medido / declarado (ordem de grandeza)',
    classificacao VARCHAR(60) NULL,
    diagnostico VARCHAR(20) NULL COMMENT 'código da classificação (pesagens_ciclo.DIAGNOSTICOS)',
    detalhe VARCHAR(120) NULL,
    confirmacao VARCHAR(80) NULL COMMENT 'principal: a outra pesagem comparada do ciclo confirma o medido?',
    divisao SMALLINT NULL COMMENT 'divisão da balança do recinto (kg)',
    fora_da_divisao TINYINT(1) NULL COMMENT 'peso que a balança não mostraria: digitado',
    create_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID),
    UNIQUE KEY uq_pesagem_ciclo (ciclo_de_vida_id, pesagem_id),
    KEY ix_pesagem_principal (ciclo_de_vida_id, principal),
    KEY ix_pesagem_diagnostico (diagnostico, principal),
    CONSTRAINT fk_pesagem_ciclo FOREIGN KEY (ciclo_de_vida_id)
        REFERENCES ciclodevida_ciclo_de_vida_importacao (ID) ON DELETE CASCADE
);

-- =====================================================================================================
-- ROLLBACK (só se for preciso desfazer; apaga as pesagens gravadas — dado derivado, o serviço recria)
-- =====================================================================================================
-- DROP TABLE IF EXISTS ciclodevida_pesagem;
