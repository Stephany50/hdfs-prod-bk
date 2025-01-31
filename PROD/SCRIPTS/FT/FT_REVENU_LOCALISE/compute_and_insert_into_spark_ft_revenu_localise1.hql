
-- insert info voix


INSERT INTO TMP.SPARK_FT_REVENU_LOCALISE_1

SELECT

    nvl(B.SITE_NAME, A.SITE_NAME) SITE_NAME,
    nvl(B.OFFER_PROFILE_CODE, A.OFFER_PROFILE_CODE) OFFER_PROFILE_CODE,
    nvl(B.DESTINATION_TYPE, A.DESTINATION_TYPE) DESTINATION_TYPE,
    nvl(B.DESTINATION, A.DESTINATION) DESTINATION,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.rated_tel_total_count, B.rated_tel_total_count), B.rated_tel_total_count) RATED_TEL_TOTAL_COUNT,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.rated_sms_total_count, B.rated_sms_total_count), B.rated_sms_total_count) RATED_SMS_TOTAL_COUNT,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(a.rated_duration, b.rated_duration), B.rated_duration) RATED_DURATION,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.voice_main_rated_amount, B.voice_main_rated_amount), B.voice_main_rated_amount) VOICE_MAIN_RATED_AMOUNT,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.voice_promo_rated_amount, B.voice_promo_rated_amount), B.voice_promo_rated_amount) VOICE_PROMO_RATED_AMOUNT,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.sms_main_rated_amount, B.sms_main_rated_amount), B.sms_main_rated_amount) SMS_MAIN_RATED_AMOUNT,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.sms_main_rated_amount, B.sms_promo_rated_amount), B.sms_promo_rated_amount) SMS_PROMO_RATED_AMOUNT,
    A.REVENU_DATA REVENU_DATA,
    A.MBYTES_USED MBYTES_USED,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.offer_profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , 'VOIX|', a.src_table||'VOIX|') SRC_TABLE,
    nvl(B.SITE_CODE, A.SITE_CODE) SITE_CODE,
    A.IN_CALL_COUNT IN_CALL_COUNT,
    A.IN_DURATION IN_DURATION,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(B.EVENT_DATE, A.EVENT_DATE) EVENT_DATE

FROM
MON.SPARK_FT_IMEI_TRANSACTION  A
FULL OUTER JOIN
(
SELECT distinct TRANSACTION_DATE AS EVENT_DATE,
upper(SITE_CODE) SITE_CODE,
upper(SITE_NAME) SITE_NAME,
upper(OFFER_PROFILE_CODE) OFFER_PROFILE_CODE,
upper(CASE
WHEN DESTINATION = 'Orange' THEN 'OnNet'
WHEN DESTINATION IN ('MTN', 'Camtel', 'NEXTTEL')  THEN 'OffNet'
WHEN DESTINATION = 'International' THEN 'International'
END) AS DESTINATION_TYPE,
upper(DESTINATION) DESTINATION,
SUM(RATED_TEL_TOTAL_COUNT) AS RATED_TEL_TOTAL_COUNT,
SUM(RATED_SMS_TOTAL_COUNT) AS RATED_SMS_TOTAL_COUNT,
SUM(RATED_DURATION) AS RATED_DURATION,
SUM(VOICE_MAIN_RATED_AMOUNT) AS VOICE_MAIN_RATED_AMOUNT,
SUM(VOICE_PROMO_RATED_AMOUNT) AS VOICE_PROMO_RATED_AMOUNT,
SUM(SMS_MAIN_RATED_AMOUNT) AS SMS_MAIN_RATED_AMOUNT,
SUM(SMS_PROMO_RATED_AMOUNT) AS SMS_PROMO_RATED_AMOUNT

FROM
(
select   transaction_date
, SITE_CODE
,nvl(vdci.SITE_NAME, LPAD(fn_hex2deci(upper(NSL_CI)),5,0))  AS site_name
,CASE WHEN  b.dest_short='Orange' THEN 'Orange'
WHEN  b.dest_short='MTN'   THEN 'MTN'
WHEN  b.dest_short='International' THEN 'International'
WHEN  b.dest_short='SVA' THEN 'SVA'
WHEN  b.dest_short LIKE '%MVNO%' THEN 'Orange'
WHEN  b.dest_short = 'CTPhone' THEN 'Camtel'
WHEN  b.dest_short LIKE '%Roam%' THEN 'roaming'
WHEN  b.dest_short = 'NEXTTEL' THEN 'NEXTTEL'
ELSE 'AUTRES' END destination
, OFFER_PROFILE_CODE
,sum(CASE WHEN service_code='VOI_VOX' THEN rated_total_count ELSE 0 end) RATED_TEL_TOTAL_COUNT
,sum(CASE WHEN service_code='NVX_SMS' THEN rated_total_count ELSE 0 end) RATED_SMS_TOTAL_COUNT
,sum(CASE WHEN service_code='VOI_VOX' THEN rated_duration ELSE 0 END ) RATED_DURATION
,sum(CASE WHEN service_code='VOI_VOX' THEN promo_rated_amount ELSE 0 end) VOICE_PROMO_RATED_AMOUNT
,sum(CASE WHEN service_code='NVX_SMS' THEN main_rated_amount ELSE 0 end) SMS_MAIN_RATED_AMOUNT
,sum(CASE WHEN service_code='VOI_VOX' THEN main_rated_amount ELSE 0 end) VOICE_MAIN_RATED_AMOUNT
,sum(CASE WHEN service_code='NVX_SMS' THEN promo_rated_amount ELSE 0 end) SMS_PROMO_RATED_AMOUNT
from MON.SPARK_FT_GSM_LOCATION_REVENUE_DAILY,
( select * from DIM.SPARK_DT_GSM_CELL_CODE where technologie in ('2G', '3G')) vdci, --VW_CI_LAC_SITE_AMN
(select dest_id, dest_short from dim.dt_destinations ) b
where TRANSACTION_DATE = TO_DATE(s_slice_value, 'yyyymmdd') --'11/09/2019'  -- '01/06/2019'   --TO_DATE(s_slice_value, 'yyyymmdd')
AND LPAD(fn_hex2deci(upper(NSL_CI)),5,0) =vdci.CI(+)
AND b.dest_id(+)=destination
group BY transaction_date
, SITE_CODE
,nvl(vdci.SITE_NAME, LPAD(fn_hex2deci(upper(NSL_CI)),5,0))   --vdci.SITE_NAME
,CASE WHEN  b.dest_short='Orange' THEN 'Orange'
WHEN  b.dest_short='MTN'   THEN 'MTN'
WHEN  b.dest_short='International' THEN 'International'
WHEN  b.dest_short='SVA' THEN 'SVA'
WHEN  b.dest_short LIKE '%MVNO%' THEN 'Orange'
WHEN  b.dest_short = 'CTPhone' THEN 'Camtel'
WHEN  b.dest_short LIKE '%Roam%' THEN 'roaming'
WHEN  b.dest_short = 'NEXTTEL' THEN 'NEXTTEL'
ELSE 'AUTRES' END
, OFFER_PROFILE_CODE
)
WHERE DESTINATION IN ('Orange', 'International', 'MTN', 'Camtel', 'NEXTTEL')
GROUP BY TRANSACTION_DATE,
SITE_CODE,
SITE_NAME,
DESTINATION,
OFFER_PROFILE_CODE,
CASE
WHEN DESTINATION = 'Orange' THEN 'OnNet'
WHEN DESTINATION IN ('MTN', 'Camtel', 'NEXTTEL') THEN 'OffNet'
WHEN DESTINATION = 'International' THEN 'International'
END

) B

ON (A.EVENT_DATE = B.EVENT_DATE  AND upper(A.SITE_CODE) = upper(B.SITE_CODE) AND upper(A.SITE_NAME) = upper(B.SITE_NAME) AND upper(a.offer_profile_code) = upper(b.offer_profile_code) AND upper(a.destination_type) = upper(b.destination_type) AND upper(a.destination) = upper(b.destination))


