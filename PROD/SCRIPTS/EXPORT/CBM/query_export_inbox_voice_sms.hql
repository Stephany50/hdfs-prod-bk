SELECT
    event_date
    , msisdn
    , sms_count
    , tel_count
    , tel_duration
    , billed_sms_count
    , billed_tel_count
    , billed_tel_duration
    , main_call_cost
    , promotional_call_cost
    , onnet_sms_count
    , (NATIONAL_TOTAL_COUNT - MTN_TOTAL_COUNT - CAMTEL_TOTAL_COUNT - SET_TOTAL_COUNT - NVL(NEXTTEL_TOTAL_COUNT,0)) onnet_total_count
    , onnet_duration
    , onnet_main_conso
    , mtn_sms_count
    , mtn_total_count
    , mtn_duration
    , mtn_main_conso
    , NVL(NEXTTEL_SMS_COUNT,0) nexttel_sms_count
    , NVL(NEXTTEL_TOTAL_COUNT,0) nexttel_total_count
    , NVL(NEXTTEL_DURATION,0) nexttel_duration
    , NVL(NEXTTEL_MAIN_CONSO,0) nexttel_main_conso
    , camtel_sms_count
    , camtel_total_count
    , camtel_duration
    , camtel_main_conso
    , set_sms_count
    , set_total_count
    , set_duration
    , set_main_conso
    , international_sms_count
    , international_total_count
    , international_duration
    , international_main_conso
FROM MON.SPARK_FT_CONSO_MSISDN_DAY
WHERE EVENT_DATE = '###SLICE_VALUE###'
