# Questão 3

## importação dos arquivos

importei os arquivos nos caminhos 

`fluent-limiter-425719-e7.case_spec.application_record_local` 
`fluent-limiter-425719-e7.case_spec.application_record_gcp`

Importante se atentar ao caminho quando for rodar o arquivo validacao_base_gcp.sql

## Resolução

criação de colunas no formato texto, mesclando com informações identificadas como diferenças nos arquivos.
Adicionei uma coluna de comentário com possíveis soluções e causas

## Erros encontrados 
- 4051 não encontrados na tabela do GCP	
- A coluna AMT_INCOME_TOTAL está multiplicada por 100	
- Falta a informação de "Lower secondary" na coluna "NAME_EDUCATION_TYPE" na tabela GCP
- retirar sinal "-" para a diferença da coluna "DAYS_BIRTH" ficar 0
- 315489 casos da coluna "FLAG_WORK_PHONE" estão vazios na tabela do GCP

