INSERT  INTO MON.SPARK_TMP_LAST_DATE_USAGE_PRE
SELECT 
MSISDN,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(last_date_out_voice_sms) AS string), CAST(TO_DATE(last_date_inc_voice_sms) AS string)), 1  , 'DESC','\\|')) last_date_telco,
last_date_out_voice_sms,
last_date_out_voice,
last_date_out_sms,
last_date_data,
last_date_inc_voice_sms,
last_date_inc_voice,
last_date_inc_sms,
last_date_subscription,
last_date_om
-- ,CURRENT_TIMESTAMP INSERT_DATE, "###SLICE_VALUE###" EVENT_DATE 
FROM (
SELECT T.MSISDN MSISDN, 
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(MAX(last_date_out_voice)) AS string), CAST(TO_DATE(MAX(last_date_out_sms)) AS string)), 1  , 'DESC','\\|')) last_date_out_voice_sms, 
MAX(last_date_out_voice) last_date_out_voice,
MAX(last_date_out_sms) last_date_out_sms,
MAX(last_date_data) last_date_data,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(MAX(last_date_inc_voice)) AS string), CAST(TO_DATE(MAX(last_date_inc_sms)) AS string)), 1  , 'DESC','\\|')) last_date_inc_voice_sms,
MAX(last_date_inc_voice) last_date_inc_voice,
MAX(last_date_inc_sms) last_date_inc_sms,
MAX(last_date_subscription) last_date_subscription,
MAX(last_date_om) last_date_om
FROM (
SELECT
S.MSISDN MSISDN,
S.OG_CALL last_date_out_voice,
S.OG_SMS last_date_out_sms,
NULL last_date_data,
S.IC_CALL last_date_inc_voice,
S.IC_SMS last_date_inc_sms,
NULL last_date_subscription,
NULL last_date_om
FROM (
SELECT
CASE 
WHEN LENGTH(SERVED_MSISDN) = 13 AND substr(SERVED_MSISDN,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(SERVED_MSISDN) IN ('MTN','VIETTEL','OCM')
THEN SUBSTR(SERVED_MSISDN,-9)
ELSE SERVED_MSISDN
END MSISDN,
MAX (
CASE WHEN TRANSACTION_DIRECTION = 'Sortant' and TRANSACTION_TYPE like '%TEL%'  THEN TRANSACTION_DATE
ELSE NULL
END
)  OG_CALL,
MAX (
CASE WHEN TRANSACTION_DIRECTION = 'Sortant' and TRANSACTION_TYPE like '%SMS%'  THEN TRANSACTION_DATE
ELSE NULL
END
)  OG_SMS,
MAX (
CASE WHEN TRANSACTION_DIRECTION = 'Entrant' and TRANSACTION_TYPE like '%TEL%'  THEN TRANSACTION_DATE
ELSE NULL
END
) IC_CALL,
MAX (
CASE WHEN TRANSACTION_DIRECTION = 'Entrant' and TRANSACTION_TYPE like '%SMS%' 
-- and service_centre not like '%694000018%' 
and service_centre not like '%694000017%' and other_party > 10000000   THEN TRANSACTION_DATE
ELSE NULL
END
) IC_SMS,
COUNT (
DISTINCT (CASE
WHEN
TRANSACTION_DIRECTION = 'Entrant' THEN CONCAT(transaction_date,transaction_time)
ELSE NULL
END)
) IC_CALL_COUNT
FROM MON.SPARK_FT_MSC_TRANSACTION a
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
--  AND (CASE
-- WHEN OLD_CALLING_NUMBER IS NULL THEN 1
-- WHEN OLD_CALLING_NUMBER IN ('23799900929', '99900929','237699900929','699900929') THEN 0
-- WHEN OLD_CALLING_NUMBER rlike '^[\+]*[0-9]+$'   THEN 1
-- ELSE 0
-- END) = 1

-- AND OTHER_PARTY NOT IN ('937','938','924')
AND FN_NNP_SIMPLE_DESTINATION (SERVED_MSISDN) IN ('OCM','SET')
GROUP BY
CASE 
WHEN LENGTH(SERVED_MSISDN) = 13 AND substr(SERVED_MSISDN,1,3) = '160' AND FN_GET_NNP_MSISDN_SIMPLE_DESTN(SERVED_MSISDN) IN ('MTN','VIETTEL','OCM') 
THEN SUBSTR(SERVED_MSISDN,-9) 
ELSE SERVED_MSISDN 
END

-- comment    
) S
WHERE S.MSISDN IS NOT NULL
UNION
SELECT
fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN) MSISDN,
NULL last_date_out_voice,
NULL last_date_out_sms,
NULL last_date_data,
NULL last_date_inc_voice,
NULL last_date_inc_sms,
MAX(TRANSACTION_DATE) last_date_subscription,
NULL last_date_om
FROM MON.SPARK_FT_SUBSCRIPTION
WHERE TRANSACTION_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',3) and '###SLICE_VALUE###'
AND RATED_AMOUNT > 0
GROUP BY fn_format_msisdn_to_9digits(SERVED_PARTY_MSISDN)
UNION
select
SERVED_PARTY_MSISDN msisdn,
NULL last_date_out_voice,
NULL last_date_out_sms,
MAX(session_DATE) last_date_data,
NULL last_date_inc_voice,
NULL last_date_inc_sms,
NULL last_date_subscription,
NULL last_date_om
from MON.SPARK_ft_cra_gprs
where session_date BETWEEN DATE_SUB('###SLICE_VALUE###',3) and '###SLICE_VALUE###'
and service_code not like '%FREE%'
and bytes_sent + bytes_received + total_cost + session_duration > 0
and SERVED_PARTY_MSISDN is not null
group by SERVED_PARTY_MSISDN
UNION

-- select
-- SERVED_PARTY_MSISDN msisdn,
-- NULL last_date_out_voice,
-- MAX (TRANSACTION_DATE) last_date_out_sms,
-- NULL last_date_data,
-- NULL last_date_inc_voice,
-- NULL last_date_inc_sms,
-- NULL last_date_subscription,
-- NULL last_date_om
-- from MON.SPARK_FT_SMSC_TRANSACTION_A2P
-- where transaction_billing_date BETWEEN DATE_SUB('###SLICE_VALUE###',3) and '###SLICE_VALUE###'
-- and SERVED_PARTY_MSISDN is not null
-- group by SERVED_PARTY_MSISDN
-- UNION
-- select
-- other_party_msisdn msisdn,
-- NULL last_date_out_voice,
-- NULL last_date_out_sms,
-- NULL last_date_data,
-- NULL last_date_inc_voice,
-- MAX (TRANSACTION_DATE) last_date_inc_sms, 
-- NULL last_date_subscription,
-- NULL last_date_om
-- from MON.SPARK_FT_SMSC_TRANSACTION_A2P
-- where transaction_billing_date BETWEEN DATE_SUB('###SLICE_VALUE###',3) and '###SLICE_VALUE###' 
-- and other_party_msisdn is not null
-- group by other_party_msisdn
-- UNION

select
sender_msisdn msisdn,
NULL last_date_out_voice,
NULL last_date_out_sms,
NULL last_date_data,
NULL last_date_inc_voice,
NULL last_date_inc_sms, 
NULL last_date_subscription,
MAX (transfer_datetime) last_date_om
from CDR.spark_IT_OMNY_TRANSACTIONS
where transfer_datetime BETWEEN DATE_SUB('###SLICE_VALUE###',3) and '###SLICE_VALUE###'
-- utiliser transfer_datetime ou file_date ??
and sender_msisdn is not null
group by sender_msisdn
UNION
select
receiver_msisdn msisdn,
NULL last_date_out_voice,
NULL last_date_out_sms,
NULL last_date_data,
NULL last_date_inc_voice,
NULL last_date_inc_sms, 
NULL last_date_subscription,
MAX (transfer_datetime) last_date_om
from CDR.spark_IT_OMNY_TRANSACTIONS
where transfer_datetime BETWEEN DATE_SUB('###SLICE_VALUE###',3) and '###SLICE_VALUE###' 
-- utiliser transfer_datetime ou file_date ??
and receiver_msisdn is not null
group by receiver_msisdn
) T
GROUP BY T.MSISDN)


