insert into TMP.tt_zsmart2
select
msisdn,
(CASE
WHEN trim(A.DATE_CHANGEMENT_STATUT) IS NULL OR trim(A.DATE_CHANGEMENT_STATUT) = '' THEN NULL
WHEN trim(A.DATE_CHANGEMENT_STATUT) like '%/%'
THEN  cast(translate(SUBSTR(trim(A.DATE_CHANGEMENT_STATUT), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(A.DATE_CHANGEMENT_STATUT) like '%-%'
THEN  cast(SUBSTR(trim(A.DATE_CHANGEMENT_STATUT), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_CHANGEMENT_STATUT,
statut,
raison_statut
from CDR.SPARK_IT_KYC_ZSMART A
where original_file_date=date_add('###SLICE_VALUE###',1)