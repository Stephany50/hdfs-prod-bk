insert into TMP.KYC_TT_NOMAD_DATA
select b.* from (
select *,row_number() over(partition by telephone order by ORIGINAL_FILE_DATE desc nulls last) as rn
from (
select telephone,ORIGINAL_FILE_DATE,(CASE
WHEN trim(last_update_date) IS NULL OR trim(last_update_date) = '' THEN NULL
WHEN trim(last_update_date) like '%/%'
THEN  cast(translate(SUBSTR(trim(last_update_date), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(last_update_date) like '%-%' THEN  cast(SUBSTR(trim(last_update_date), 1, 19) AS TIMESTAMP)
ELSE NULL END) last_update_date,majle,expiration,etat,etatdexportglobal,typedecontrat
from cdr.spark_it_nomad_client_directory_30J
union all
select telephone,ORIGINAL_FILE_DATE,(CASE
WHEN trim(last_update_date) IS NULL OR trim(last_update_date) = '' THEN NULL
WHEN trim(last_update_date) like '%/%'
THEN  cast(translate(SUBSTR(trim(last_update_date), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(last_update_date) like '%-%' THEN  cast(SUBSTR(trim(last_update_date), 1, 19) AS TIMESTAMP)
ELSE NULL END) last_update_date,majle,expiration,etat,etatdexportglobal,typedecontrat
from cdr.spark_it_nomad_client_directory
union all
select telephone,ORIGINAL_FILE_DATE,(CASE
WHEN trim(last_update_date) IS NULL OR trim(last_update_date) = '' THEN NULL
WHEN trim(last_update_date) like '%/%'
THEN  cast(translate(SUBSTR(trim(last_update_date), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(last_update_date) like '%-%' THEN  cast(SUBSTR(trim(last_update_date), 1, 19) AS TIMESTAMP)
ELSE NULL END) last_update_date,majle,expiration,etat,etatdexportglobal,typedecontrat
from cdr.spark_it_nomad_client_directory_dwh
) aa )b where rn=1