-- DDL: silver_municipios_ibge
-- Camada: Prata (Parquet particionado por uf)
-- Grao: 1 linha por municipio (codigo_ibge)

CREATE EXTERNAL TABLE IF NOT EXISTS silver_municipios_ibge (
    codigo_ibge INT COMMENT 'Codigo oficial do municipio no IBGE (chave primaria)',
    nome_municipio STRING COMMENT 'Nome oficial do municipio',
    codigo_uf INT COMMENT 'Codigo IBGE da UF',
    sigla_uf STRING COMMENT 'Sigla da UF (2 caracteres)',
    nome_uf STRING COMMENT 'Nome da Unidade Federativa',
    codigo_regiao INT COMMENT 'Codigo IBGE da Grande Regiao',
    sigla_regiao STRING COMMENT 'Sigla da regiao (N, NE, SE, S, CO)',
    nome_regiao STRING COMMENT 'Nome da Grande Regiao',
    mesorregiao STRING COMMENT 'Nome da mesorregiao geografica',
    microrregiao STRING COMMENT 'Nome da microrregiao geografica',
    data_ingestao_utc TIMESTAMP COMMENT 'Timestamp UTC da ingestao na Prata',
    uf STRING COMMENT 'Coluna de particao (sigla da UF)'
)
USING PARQUET
PARTITIONED BY (uf);