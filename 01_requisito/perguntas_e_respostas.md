# Perguntas e Respostas — Case Preço de Bomba (Distribuidora Veredas)

**Contexto:** Revisão da política de reembolso de combustível para cerca de 2.400 vendedores externos (carro próprio), no período de **janeiro de 2024 a outubro de 2026**[cite: 2, 3].

## 1. Vigência da tabela de tetos
- **Pergunta:** Como funciona a vigência na data do abastecimento, mudanças no meio do mês e eventuais lacunas ou sobreposições na `tabela_reembolso_uf.csv`[cite: 2]?
- **Resposta da área de negócio:** Vale o teto vigente na data do abastecimento. Mudanças no meio do mês dividem o mês pela data de troca (antes segue a versão antiga; a partir da troca, a nova). Não há regra de negócio para lacunas ou sobreposições: devemos listar os casos encontrados, propor um tratamento documentado e reportar para correção na tabela de origem.
  - *Achado preliminar na base:* `BA` (lacuna de `31/12/2024` a `11/01/2025` entre versões 2 e 3) e `PR` (sobreposição de `15/06/2025` a `30/06/2025` entre versões 3 e 4).

## 2. Teto × combustível
- **Pergunta:** Qual combustível deve ser priorizado na comparação com o teto por UF[cite: 2]?
- **Resposta da área de negócio:** A comparação principal é **teto × gasolina comum** (base de definição do teto)[cite: 2]. O etanol pode aparecer de forma complementar, mantendo a resposta da pergunta 1 focada na gasolina comum[cite: 2].

## 3. Municípios sem coleta da ANP
- **Pergunta:** Como tratar municípios sem coleta na pesquisa da ANP e as agregações por UF?
- **Resposta da área de negócio:** Não fazer estimativa para municípios sem coleta; exibi-los como **sem cobertura**. Nas agregações por UF, considerar os municípios coletados deixando explícito que o resultado é amostral.

## 4. Paridade do etanol
- **Pergunta:** Como orientar o abastecimento com etanol por município e época do ano[cite: 2]?
- **Resposta da área de negócio:** Definir e justificar o cálculo da paridade e a visão de sazonalidade que deixem mais clara a recomendação por município e período do ano sobre onde e quando compensa abastecer com etanol[cite: 2].

## 5. Preço × inflação
- **Pergunta:** Como aplicar o IPCA (SGS série 433) e incluir a evolução real dos tetos[cite: 2, 3]?
- **Resposta da área de negócio:** Utilizar o IPCA (série 433) com **mês-base explícito** no painel[cite: 3], apresentando a evolução real dos combustíveis e também a evolução real dos tetos em formato de fácil comparação.

## 6. Variação entre postos e bandeiras
- **Pergunta:** Qual métrica usar para variação entre postos e bandeiras e qual critério mínimo de representatividade aplicar[cite: 2]?
- **Resposta da área de negócio:** Mostrar de forma direta quanto o vendedor economiza por litro ao escolher bem o posto ou a bandeira, definindo e explicando no painel um critério mínimo de quantidade de postos para evitar comparações pouco representativas.