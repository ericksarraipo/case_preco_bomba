# Case Preço de Bomba - Distribuidora Veredas

Pipeline analítico idempotente em arquitetura Medallion (Bronze / raw -> Prata -> Ouro) em Python (pandas + pyarrow), sem dependência de Java/JVM no Windows, para apoiar a revisão da política de reembolso de combustível de ~2.400 vendedores externos no período de janeiro de 2024 a outubro de 2026 (34 meses).

## Como Executar

O run_pipeline.ps1 já executa automaticamente as cargas da camada Bronze (IBGE/BCB, ANP e Reembolso UF a partir de data/raw/tabela_reembolso_uf.csv) em sequência antes das etapas Prata, Ouro, validação e geração do relatório HTML.

### No PowerShell (na raiz do projeto)

.\run_pipeline.ps1

# Opcional: rodar sem abrir o navegador ao final
.\run_pipeline.ps1 -NoBrowser

### No Prompt de Comando (CMD)

cd /d C:\Users\ADM\projetos\case_preco_bomba
powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1

# Opcional: rodar sem abrir o navegador ao final
powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1 -NoBrowser
