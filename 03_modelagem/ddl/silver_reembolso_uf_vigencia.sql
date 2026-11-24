-- DDL: silver_reembolso_uf_vigencia
-- Camada: Prata (Parquet particionado por uf)
-- Grao: 1 linha por versao/registro de vigencia de teto por UF
-- Regra: teto vigente na data do abastecimento; registrar lacunas/sobreposicoes sem imputar

CREATE EXTERNAL TABLE IF NOT EXISTS silver_reembolso_uf_vigencia (
    uf STRING COMMENT 'Sigla da UF (2 caracteres)',
    versao INT COMMENT 'Numero da versao da tabela de reembolso',
    teto_litro_reembolso DECIMAL(10, 4) COMMENT 'Valor maximo reembolsavel por litro (R$)',
    vigencia_inicio DATE COMMENT 'Data de inicio da vigencia',
    vigencia_fim DATE COMMENT 'Data de fim da vigencia (ou data limite do periodo quando em aberto)',
    vigencia_fim_original_nula BOOLEAN COMMENT 'Indica se vigencia_fim estava vazia/nula na origem',
    flag_anomalia_vigencia STRING COMMENT 'SEM_ANOMALIA, LACUNA_ANTERIOR ou SOBREPOSICAO_ANTERIOR',
    detalhe_anomalia STRING COMMENT 'Descricao da lacuna ou sobreposicao identificada (ex.: BA, PR)',
    data_ingestao_utc TIMESTAMP COMMENT 'Timestamp UTC da ingestao na Prata'
)
USING PARQUET
PARTITIONED BY (uf);