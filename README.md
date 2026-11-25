# Case Preço de Bomba - Distribuidora Veredas

Pipeline analitico idempotente em arquitetura Medallion (Bronze / raw -> Prata -> Ouro) em Python (pandas + pyarrow), sem dependencia de Java/JVM no Windows, para apoiar a revisao da politica de reembolso de combustivel de ~2.400 vendedores externos no periodo de janeiro de 2024 a outubro de 2026 (34 meses).

## Como Executar

No PowerShell, na raiz do projeto:

```powershell
.\run_pipeline.ps1

# Opcional: rodar sem abrir o navegador ao final
.\run_pipeline.ps1 -NoBrowser
```
## Perguntas e Respostas - Case Pre?o de Bomba

### 1. A nossa tabela de tetos esta alinhada com o preco de bomba? Em quais UFs e em quais meses o teto ficou acima ou abaixo do preco praticado, e quanto, em R$ por litro?
- **Criterio de comparacao:** A comparacao principal e **teto x gasolina comum** (base de definicao do teto), usando o **teto vigente na data do abastecimento** (data_coleta). Mudancas no meio do mes dividem o periodo pela data de troca (antes segue a versao anterior; a partir da troca, a nova).
- **Impacto ao vendedor:** Quando o teto fica **abaixo** do preco de bomba, a diferenca em **R$/L** recai sobre o vendedor; quando fica **acima**, ha folga frente ao teto.
- **O que os dados mostram:** Ha pressao recorrente em estados como **AM** (+R$ 0,969 / +16,16% de delta medio, 34 meses), **RR** (+R$ 0,840 / +14,00%, 34 meses) e **AC** (+R$ 0,840 / +12,90%, 34 meses), alem de ocorrencias em outros estados e meses especificos detalhados no relatorio HTML (reports/relatorio_distribuidora_veredas.html).
- **Anomalias e excecoes auditadas (sem imputacao artificial):**
  - **BA:** lacuna de vigencia de 31/12/2024 a 11/01/2025 (2025-01-01 a 2025-01-10 apos a versao 2, por aprovacao atrasada no comite).
  - **PR:** sobreposicao a partir de 15/06/2025 com vigencia anterior ate 30/06/2025 (versao anterior 3, por reajuste antecipado a pedido da regional Sul).
  - **Vigencia fim nula:** registros em aberto mantidos rastreaveis para auditoria.

### 2. Onde compensa abastecer com etanol? Como orientar por municipio e epoca do ano?
- **Regra da paridade (70%):** Compensa abastecer com **etanol** quando o preco do etanol e **ate 70%** do preco da gasolina comum (preco_etanol / preco_gasolina_comum <= 0,70).
- **Orientacao por municipio e sazonalidade:** A recomendacao deve ser avaliada por municipio (codigo_ibge / uf) e mes (ano_mes), capturando a **sazonalidade** (como safra e entressafra da cana-de-acucar).
- **Municipios sem coleta da ANP:** Nao recebem estimativa ou imputacao; sao exibidos como **sem cobertura**, e as agregacoes estaduais sao explicitas como **amostrais**.

### 3. O combustivel subiu mais que a inflacao? Como fica a evolucao real descontada a inflacao (IPCA)?
- **Metodologia:** Os valores nominais (preco de bomba e tetos de reembolso) sao deflacionados pelo **IPCA (BCB SGS serie 433)** a partir de um **mes-base explicito** (2024-01), permitindo comparar a evolucao real dos combustiveis e a evolucao real dos tetos antes de aprovar reajustes.
- **Leitura na serie:** Na visao consolidada do relatorio, o preco nominal ponderado saiu de **R$ 4,759** (2024-01) para **R$ 5,546** (2026-10), enquanto o preco real deflacionado pelo IPCA evoluiu de **4,759** para **4,895** (com picos reais em 2025-02 em **5,236** e 2026-04 em **5,282**).

### 4. Escolher o posto faz diferenca? Quanto varia entre postos e bandeiras no mesmo municipio, e vale orientar buscar bandeira branca?
- **Variacao entre postos e bandeiras:** Dentro de um mesmo municipio e mes, a dispersao entre postos e entre bandeiras impacta diretamente o custo por litro e o risco de estourar o teto da UF.
- **Criterio minimo de representatividade:** Para evitar distorcoes em pracas com poucas observacoes, a comparacao entre postos e bandeiras exige um **numero minimo de postos** por municipio/bandeira no periodo; abaixo desse piso, a leitura e tratada como de baixa representatividade.
- **Orientacao sobre bandeira branca:** Vale orientar a busca por **bandeira branca** apenas onde houver **vantagem de preco consistente e amostra minima**, mantendo ressalvas de governanca: conferencia de procedencia/qualidade, conformidade fiscal (NF/CNPJ) e avaliacao do impacto de desvio de rota.

## O Que os Dados Nao Respondem / Limites da Base
- **Cobertura amostral da ANP:** Nem todos os municipios ou postos tem coleta em todos os meses; municipios sem coleta ficam como **sem cobertura** e medias por UF sao amostrais.
- **Variaveis fora do escopo da ANP:** A base nao informa descontos corporativos ou cartao-frota negociados pela Veredas, convenios com redes especificas, custo/tempo de deslocamento extra na rota do vendedor ou qualidade/adulteracao pontual na bomba.
