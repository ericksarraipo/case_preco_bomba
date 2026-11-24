-- DDL: silver_ipca_mensal
-- Camada: Prata (Parquet particionado por ano)
-- Grao: 1 linha por mes de referencia (ano_mes)
-- Fonte: Banco Central SGS Serie 433 (IPCA variacao mensal %)

CREATE EXTERNAL TABLE IF NOT EXISTS silver_ipca_mensal (
    ano_mes STRING COMMENT 'Mes de referencia no formato YYYY-MM (chave primaria)',
    data_referencia DATE COMMENT 'Data de referencia retornada pelo BCB',
    variacao_mensal_pct DECIMAL(10, 4) COMMENT 'Variacao percentual mensal do IPCA (serie 433)',
    fator_mensal DECIMAL(18, 10) COMMENT '1 + (variacao_mensal_pct / 100)',
    indice_acumulado_base DECIMAL(18, 10) COMMENT 'Indice acumulado normalizado em relacao ao mes-base definido no job',
    mes_base_ipca STRING COMMENT 'Mes-base explicito usado para o indice acumulado (YYYY-MM)',
    data_ingestao_utc TIMESTAMP COMMENT 'Timestamp UTC da ingestao na Prata',
    ano INT COMMENT 'Ano de referencia para particionamento'
)
USING PARQUET
PARTITIONED BY (ano);