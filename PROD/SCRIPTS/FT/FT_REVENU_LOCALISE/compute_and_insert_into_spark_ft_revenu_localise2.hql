
-- insert info msc


INSERT INTO TMP.SPARK_FT_REVENU_LOCALISE_2

SELECT

    nvl(B.SITE_NAME, A.SITE_NAME) SITE_NAME,
    nvl(B.SUBSCRIBER_TYPE, A.OFFER_PROFILE_CODE) OFFER_PROFILE_CODE,
    nvl(B.DESTINATION_TYPE, A.DESTINATION_TYPE) DESTINATION_TYPE,
    nvl(B.DESTINATION, A.DESTINATION) DESTINATION,
    A.RATED_TEL_TOTAL_COUNT RATED_TEL_TOTAL_COUNT,
    A.RATED_SMS_TOTAL_COUNT RATED_SMS_TOTAL_COUNT,
    A.RATED_DURATION RATED_DURATION,
    A.VOICE_MAIN_RATED_AMOUNT VOICE_MAIN_RATED_AMOUNT,
    A.VOICE_PROMO_RATED_AMOUNT VOICE_PROMO_RATED_AMOUNT,
    A.SMS_MAIN_RATED_AMOUNT SMS_MAIN_RATED_AMOUNT,
    A.SMS_PROMO_RATED_AMOUNT SMS_PROMO_RATED_AMOUNT,
    A.REVENU_DATA REVENU_DATA,
    A.MBYTES_USED MBYTES_USED,
    IF(A.event_date IS NULL OR B.SDATE IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.SUBSCRIBER_TYPE) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , 'MSC|', A.src_table||'MSC|') SRC_TABLE,
    nvl(B.SITE_CODE, A.SITE_CODE) SITE_CODE,
    IF(A.event_date IS NULL OR B.SDATE IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.SUBSCRIBER_TYPE) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.IN_CALL_COUNT, B.NBre_appel), B.NBre_appel) IN_CALL_COUNT,
    IF(A.event_date IS NULL OR B.SDATE IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.SUBSCRIBER_TYPE) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.IN_DURATION, B.Duree), B.Duree) IN_DURATION,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(B.SDATE, A.EVENT_DATE) EVENT_DATE

FROM
MON.SPARK_FT_IMEI_TRANSACTION  A
FULL OUTER JOIN
(
select sdate, upper(site_code) site_code, upper(site_name) site_name
, case when type_abonne = 'PREP' then 'PREPAID PLENTY'    --Forcer aux valeurs les plus preponderante du fait que granularite moindre inexistante dans AG_INTERCO
when type_abonne = 'POST' then  'PREPAID FLEX PLUS'
else 'PREPAID PLENTY'
end subscriber_type
,( case when trunck_in in ('Zain Tchad', 'Zain Gabon', 'Orange CI', 'BELG','FTLD', 'IBGAP' ) then 'INTERNATIONAL'
when trunck_in in ('CAMTEL', 'MTN', 'LMT', 'VIETTEL' ) then 'OFFNET'
else 'ONNET'
end
) destination_type
, upper( CASE
WHEN  trunck_in IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_in
WHEN  trunck_in = 'CAMTEL' THEN 'Camtel National'
WHEN  trunck_in = 'Zain Gabon' THEN  'Zain Gabon'
WHEN  trunck_in = 'Zain Tchad' THEN  'Zain Tchad'
WHEN  trunck_in = 'Orange CI' THEN  'Orange CI'
--
WHEN  NVL (trunck_in, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
ELSE 'Orange'
END ) Destination
, sum(CRA_COUNT) NBre_appel
, sum(DURATION) Duree
from agg.spark_ft_ag_interco
,(select * from DIM.SPARK_DT_GSM_CELL_CODE where technologie in ('2G', '3G')) --Du fait de la présence des doublons en ajoutant les ci de la 4G.
where sdate = TO_DATE(s_slice_value, 'yyyymmdd') --'01/08/2019'  --x --'11/08/2019' -- '10/08/2019'
--and sdate <= '31/08/2019'
and USAGE_APPEL = 'TEL'
and substr(msc_location,14,5)=to_char(ci(+))
group by sdate, upper(site_code), upper(site_name)
, case when type_abonne = 'PREP' then 'PREPAID PLENTY'    --Forcer aux valeurs les plus preponderante du fait que granularite moindre inexistante dans AG_INTERCO
when type_abonne = 'POST' then  'PREPAID FLEX PLUS'
else 'PREPAID PLENTY'
end
,( case when trunck_in in ('Zain Tchad', 'Zain Gabon', 'Orange CI', 'BELG','FTLD', 'IBGAP' ) then 'INTERNATIONAL'
when trunck_in in ('CAMTEL', 'MTN', 'LMT', 'VIETTEL' ) then 'OFFNET'
else 'ONNET'
end
)
, upper( CASE
WHEN  trunck_in IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_in
WHEN  trunck_in = 'CAMTEL' THEN 'Camtel National'
WHEN  trunck_in = 'Zain Gabon' THEN  'Zain Gabon'
WHEN  trunck_in = 'Zain Tchad' THEN  'Zain Tchad'
WHEN  trunck_in = 'Orange CI' THEN  'Orange CI'
--
WHEN  NVL (trunck_in, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
ELSE 'Orange'
END )

) B

ON (A.EVENT_DATE = B.SDATE  AND upper(A.SITE_CODE) = upper(B.SITE_CODE) AND upper(A.SITE_NAME) = upper(B.SITE_NAME) AND upper(a.offer_profile_code) = upper(b.SUBSCRIBER_TYPE) AND upper(a.destination_type) = upper(b.destination_type) AND upper(a.destination) = upper(b.destination))


