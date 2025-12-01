Create or replace procedure `fluent-limiter-425719-e7.case_spec.valid_data_gcp` ()

begin

select concat(count(*),' não encontrados na tabela do GCP'), 
'Realizar upload das informações que faltam no GCP' comentarios
from `fluent-limiter-425719-e7.case_spec.application_record_local` a
left join `fluent-limiter-425719-e7.case_spec.application_record_gcp` b
on a.id = b.id
where b.id is null

union all

select 
distinct
concat('A coluna AMT_INCOME_TOTAL está multiplicada por ', b.AMT_INCOME_TOTAL/a.AMT_INCOME_TOTAL),
'Identificar o que causou essa multiplicação' comentarios 

from `fluent-limiter-425719-e7.case_spec.application_record_local` a
left join `fluent-limiter-425719-e7.case_spec.application_record_gcp` b
on a.id = b.id
--and a.CODE_GENDER = case when lower(b.CODE_GENDER) = 'male' then 'M' else 'F' end
where b.id is not null

union all

select 
concat('Falta a informação de "',
a.NAME_EDUCATION_TYPE, '" na coluna "NAME_EDUCATION_TYPE" na tabela GCP'),
'O imput dos 4051 casos que faltam irão ajustar esse erro' comentarios

from `fluent-limiter-425719-e7.case_spec.application_record_local` a
left join `fluent-limiter-425719-e7.case_spec.application_record_gcp` b
on a.id = b.id
where case when a.NAME_EDUCATION_TYPE = b.NAME_EDUCATION_TYPE then 0 else 1 end = 1
group by all

union all

select 
concat('retirar sinal "-" para a diferença da coluna "DAYS_BIRTH" ficar ',
sum(a.DAYS_BIRTH - (b.DAYS_BIRTH*-1) )),
'ajustar a diferença entre as colunas DAYS_BIRTH' comentarios

from `fluent-limiter-425719-e7.case_spec.application_record_local` a
left join `fluent-limiter-425719-e7.case_spec.application_record_gcp` b
on a.id = b.id

where case when a.DAYS_BIRTH = b.DAYS_BIRTH then 0 else 1 end = 1
group by all

union all

select
concat(sum(case when a.FLAG_WORK_PHONE is not null and b.FLAG_WORK_PHONE is not null then 0 else 1 end), ' casos da coluna "FLAG_WORK_PHONE" estão vazios na tabela do GCP'),
'A conversão do inteiro para float pode ter influenciado' comentarios
from `fluent-limiter-425719-e7.case_spec.application_record_local` a
left join `fluent-limiter-425719-e7.case_spec.application_record_gcp` b
on a.id = b.id
group by all;
end
