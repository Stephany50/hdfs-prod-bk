insert into TMP.tt_zsmart2
select
msisdn,
(CASE
WHEN trim(A.DATE_VALIDATION_BO) IS NULL OR trim(A.DATE_VALIDATION_BO) = '' THEN NULL
WHEN trim(A.DATE_VALIDATION_BO) like '%/%'
THEN  from_unixtime(unix_timestamp(translate(SUBSTR(trim(A.DATE_VALIDATION_BO), 1, 19),'/','-'),'yyyy-MM-dd HH:mm:ss'))
WHEN trim(A.DATE_VALIDATION_BO) like '%-%' THEN  from_unixtime(unix_timestamp(SUBSTR(trim(A.DATE_VALIDATION_BO), 1, 19),'yyyy-MM-dd HH:mm:ss'))
ELSE NULL
END) AS DATE_VALIDATION_BO,
(CASE
WHEN trim(A.DATE_ACTIVATION) IS NULL OR trim(A.DATE_ACTIVATION) = '' THEN NULL
WHEN trim(A.DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(A.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(A.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(A.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION,
(CASE
WHEN trim(A.DATE_CHANGEMENT_STATUT) IS NULL OR trim(A.DATE_CHANGEMENT_STATUT) = '' THEN NULL
WHEN trim(A.DATE_CHANGEMENT_STATUT) like '%/%'
THEN  cast(translate(SUBSTR(trim(A.DATE_CHANGEMENT_STATUT), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(A.DATE_CHANGEMENT_STATUT) like '%-%'
THEN  cast(SUBSTR(trim(A.DATE_CHANGEMENT_STATUT), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_CHANGEMENT_STATUT,
statut_validation_bo,
statut
from cdr.spark_it_bdi_zsmart A
where original_file_date=date_add('###SLICE_VALUE###',1)