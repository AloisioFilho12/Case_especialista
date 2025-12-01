select concat(count(*),' não encontrados na tabela do GCP'), 
'Realizar upload das informações que faltam no GCP' comentarios
from `fluent-limiter-425719-e7.case_spec.application_record_local` a
left join `fluent-limiter-425719-e7.case_spec.application_record_gcp` b
on a.id = b.id
where b.id is null