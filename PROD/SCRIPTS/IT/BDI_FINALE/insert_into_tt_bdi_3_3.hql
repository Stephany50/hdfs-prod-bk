insert into TMP.TT_BDI3_3
select c.*
from (
select a.*,
nvl((CASE
    WHEN trim(a.DATE_ACTIVATION) IS NULL OR trim(a.DATE_ACTIVATION) = '' THEN NULL
    WHEN trim(a.DATE_ACTIVATION) like '%/%'
    THEN  cast(translate(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
    WHEN trim(a.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
    ELSE NULL
    END),
    (CASE
    WHEN trim(a.DATE_SOUSCRIPTION) IS NULL OR trim(a.DATE_SOUSCRIPTION) = '' THEN NULL
    WHEN trim(a.DATE_SOUSCRIPTION) like '%/%'
    THEN  cast(translate(SUBSTR(trim(a.DATE_SOUSCRIPTION), 1, 19),'/','-') AS TIMESTAMP)
    WHEN trim(a.DATE_SOUSCRIPTION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_SOUSCRIPTION), 1, 19) AS TIMESTAMP)
    ELSE NULL
END)) as date_activation2
from TMP.TT_BDI3_2 a
join (select *
from cdr.spark_it_bdi_zsmart
where original_file_date='###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
) c
left join (select *
from CDR.SPARK_IT_BDI_LIGNE_FLOTTE
where original_file_date='###SLICE_VALUE###') d
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(d.msisdn))
where d.msisdn is null