
-- insert info data


INSERT INTO MON.SPARK_FT_REVENU_LOCALISE

SELECT

    nvl(B.SITE_NAME, A.SITE_NAME) SITE_NAME,
    nvl(B.profile_code, A.OFFER_PROFILE_CODE) OFFER_PROFILE_CODE,
    nvl(B.DESTINATION_TYPE, A.DESTINATION_TYPE) DESTINATION_TYPE,
    nvl(B.DESTINATION, A.DESTINATION) DESTINATION,
    A.RATED_TEL_TOTAL_COUNT RATED_TEL_TOTAL_COUNT,
    A.RATED_SMS_TOTAL_COUNT RATED_SMS_TOTAL_COUNT,
    A.RATED_DURATION RATED_DURATION,
    A.VOICE_MAIN_RATED_AMOUNT VOICE_MAIN_RATED_AMOUNT,
    A.VOICE_PROMO_RATED_AMOUNT VOICE_PROMO_RATED_AMOUNT,
    A.SMS_MAIN_RATED_AMOUNT SMS_MAIN_RATED_AMOUNT,
    A.SMS_PROMO_RATED_AMOUNT SMS_PROMO_RATED_AMOUNT,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.REVENU_DATA, B.amount_data), B.amount_data) REVENU_DATA,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , nvl(A.MBYTES_USED, B.MBYTES_USED), B.MBYTES_USED) MBYTES_USED,
    IF(A.event_date IS NULL OR B.event_date IS NULL OR upper(A.SITE_CODE) IS NULL OR upper(B.SITE_CODE) IS NULL OR upper(A.SITE_NAME) IS NULL OR upper(B.SITE_NAME) IS NULL OR upper(A.offer_profile_code) IS NULL OR upper(B.profile_code) IS NULL OR upper(A.destination_type) IS NULL OR upper(B.destination_type) IS NULL OR upper(A.destination) IS NULL OR upper(B.destination) , 'DATA|', a.src_table||'DATA|') SRC_TABLE,
    nvl(B.SITE_CODE, A.SITE_CODE) SITE_CODE,
    A.IN_CALL_COUNT IN_CALL_COUNT,
    A.IN_DURATION IN_DURATION,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(B.EVENT_DATE, A.EVENT_DATE) EVENT_DATE

FROM
MON.SPARK_FT_IMEI_TRANSACTION  A
FULL OUTER JOIN
(
select distinct event_date, upper(a.site_code) site_code, upper(a.site_name) site_name, upper(profile_code) profile_code, upper(Destination_type) Destination_type, upper(Destination) Destination, MBytes_used, (Mbytes_used/Mbytes_all)*AMOUNT_DATA AMOUNT_DATA
from
(
select event_date, upper(site_code) site_code, upper(site_name) site_name, upper(PROFILE_CODE) PROFILE_CODE
, 'OnNet' Destination_type, 'Orange' Destination,  sum(BYTES_SENT+BYTES_RECEIVED)/(1024*1024) MBytes_used--, sum(Main_cost) Main_cost
from FT_GPRS_SITE_REVENU_DAILY
where event_date = TO_DATE(s_slice_value, 'yyyymmdd')   --'03/10/2019'
group by event_date, upper(site_code), upper(site_name), upper(profile_code)
) a
,
(
select distinct site_code, site_name, (sum(BYTES_SENT+BYTES_RECEIVED)over(partition by event_date))/(1024*1024) Mbytes_All
from FT_GPRS_SITE_REVENU_DAILY
where event_date = TO_DATE(s_slice_value, 'yyyymmdd')   -- '03/10/2019'
) b
,
(
select  transaction_date, sum(AMOUNT_DATA) AMOUNT_DATA
from AGG.SPARK_FT_A_SUBSCRIPTION
where transaction_date = TO_DATE(s_slice_value, 'yyyymmdd') --'03/10/2019'
group by transaction_date
)
where a.site_name = b.site_name(+)
and a.event_date = transaction_date(+)
) B

ON (A.EVENT_DATE = B.EVENT_DATE  AND upper(A.SITE_CODE) = upper(B.SITE_CODE) AND upper(A.SITE_NAME) = upper(B.SITE_NAME) AND upper(a.offer_profile_code) = upper(b.profile_code) AND upper(a.destination_type) = upper(b.destination_type) AND upper(a.destination) = upper(b.destination))


