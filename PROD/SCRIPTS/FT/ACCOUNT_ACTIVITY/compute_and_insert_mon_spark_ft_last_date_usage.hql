
INSERT INTO MON.SPARK_FT_LAST_DATE_USAGE PARTITION(EVENT_DATE)
SELECT
fn_format_msisdn_to_9digits(NVL(A.MSISDN,B.MSISDN)) MSISDN,
NVL(A.last_date_telco,B.last_date_telco) last_date_telco,
NVL(A.last_date_out_voice_sms,B.last_date_out_voice_sms) last_date_out_voice_sms,
NVL(A.last_date_out_voice,B.last_date_out_voice) last_date_out_voice,
NVL(A.last_date_out_sms,B.last_date_out_sms) last_date_out_sms,
NVL(A.last_date_data,B.last_date_data) last_date_data,
NVL(A.last_date_inc_voice_sms,B.last_date_inc_voice_sms) last_date_inc_voice_sms,
NVL(A.last_date_inc_voice,B.last_date_inc_voice) last_date_inc_voice,
NVL(A.last_date_inc_sms, B.last_date_inc_sms) last_date_inc_sms,
NVL(A.last_date_subscription, B.last_date_subscription) last_date_subscription,
NVL(A.last_date_om, B.last_date_om) last_date_om,
CURRENT_TIMESTAMP INSERT_DATE, 
'###SLICE_VALUE###' EVENT_DATE
FROM (SELECT * FROM MON.SPARK_FT_LAST_DATE_USAGE WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) A
FULL OUTER JOIN MON.SPARK_TMP_LAST_DATE_USAGE_PRE B ON (A.MSISDN = B.MSISDN)
WHERE A.MSISDN iS NULL or B.MSISDN IS NULL
UNION ALL
SELECT
fn_format_msisdn_to_9digits(D.MSISDN) MSISDN ,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_telco) AS string), CAST(TO_DATE(S.last_date_telco) AS string)), 1  , 'DESC','\\|'))  last_date_telco,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_out_voice_sms) AS string), CAST(TO_DATE(S.last_date_out_voice_sms) AS string)), 1  , 'DESC','\\|'))  last_date_out_voice_sms,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_out_voice) AS string), CAST(TO_DATE(S.last_date_out_voice) AS string)), 1  , 'DESC','\\|'))  last_date_out_voice,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_out_sms) AS string), CAST(TO_DATE(S.last_date_out_sms) AS string)), 1  , 'DESC','\\|'))  last_date_out_sms,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_data) AS string), CAST(TO_DATE(S.last_date_data) AS string)), 1  , 'DESC','\\|'))  last_date_data,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_inc_voice_sms) AS string), CAST(TO_DATE(S.last_date_inc_voice_sms) AS string)), 1  , 'DESC','\\|'))  last_date_inc_voice_sms,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_inc_voice) AS string), CAST(TO_DATE(S.last_date_inc_voice) AS string)), 1  , 'DESC','\\|'))  last_date_inc_voice,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_inc_sms) AS string), CAST(TO_DATE(S.last_date_inc_sms) AS string)), 1  , 'DESC','\\|'))  last_date_inc_sms,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_subscription) AS string), CAST(TO_DATE(S.last_date_subscription) AS string)), 1  , 'DESC','\\|'))  last_date_subscription,
TO_DATE(fn_get_sorted_list_item (CONCAT_WS('|',CAST(TO_DATE(D.last_date_om) AS string), CAST(TO_DATE(S.last_date_om) AS string)), 1  , 'DESC','\\|'))  last_date_om,
current_timestamp INSERT_DATE,
'###SLICE_VALUE###'  EVENT_DATE
FROM MON.SPARK_FT_LAST_DATE_USAGE D, MON.SPARK_TMP_LAST_DATE_USAGE_PRE S
WHERE D.EVENT_DATE = (DATE_SUB('###SLICE_VALUE###',1))
AND fn_format_msisdn_to_9digits(D.MSISDN) = S.MSISDN
