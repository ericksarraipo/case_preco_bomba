-- DDL: silver_precos_combustiveis
-- Camada: Prata (Parquet particionado por ano_mes)
-- Grao: 1 linha por registro de coleta da ANP (posto, produto, data_coleta)
-- Periodo alvo: 2024-01 a 2026-10 | Produtos: gasolina comum e etanol

CREATE EXTERNAL TABLE IF NOT EXISTS silver_precos_combustiveis (
    codigo_ibge_municipio INT COMMENT 'Codigo IBGE do municipio (7 digitos), quando mapeado',
    uf STRING COMMENT 'Sigla da UF (2 caracteres)',
    municipio_anp STRING COMMENT 'Nome do municipio conforme base ANP',
    bairro STRING COMMENT 'Bairro do posto coletado',
    cep STRING COMMENT 'CEP do posto normalizado',
    revenda STRING COMMENT 'Razao social / nome do posto revendedor',
    cnpj_revenda STRING COMMENT 'CNPJ do posto revendedor (14 digitos)',
    bandeira STRING COMMENT 'Bandeira do posto (inclui BRANCA quando aplicavel)',
    produto STRING COMMENT 'Produto padronizado (GASOLINA COMUM, ETANOL, etc.)',
    data_coleta DATE COMMENT 'Data da pesquisa semanal da ANP',
    valor_venda DECIMAL(10, 4) COMMENT 'Preco de venda ao consumidor final (R$/litro)',
    valor_compra DECIMAL(10, 4) COMMENT 'Preco de compra/distribuicao quando informado (R$/litro)',
    unidade_medida STRING COMMENT 'Unidade de medida (ex.: R$ / litro)',
    arquivo_origem STRING COMMENT 'Nome do arquivo CSV de origem na Bronze',
    data_ingestao_utc TIMESTAMP COMMENT 'Timestamp UTC da ingestao na Prata',
    ano_mes STRING COMMENT 'Chave de particao no formato YYYY-MM'
)
USING PARQUET
PARTITIONED BY (ano_mes);