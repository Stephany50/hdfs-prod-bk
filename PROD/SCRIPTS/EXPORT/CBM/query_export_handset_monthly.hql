SELECT
    period
    , msisdn
    , imei
    , total_days_count
    , REPLACE(REGION, ';', '_') region
    , REPLACE(TOWN, ';', '_') town
    , REPLACE(QUARTER, ';', '_') quarter
    , REPLACE(PH_BRAND, ';', '_') ph_brand
    , REPLACE(PH_MODEL, ';', '_') ph_model
    , data_2g
    , data_2_5g
    , data_2_75g
    , data_3g
    , data_4g
    , FROM_UNIXTIME(UNIX_TIMESTAMP(ACTIVATION_DATE, 'yyyy-mm-dd'), 'dd/mm/yyyy') activation_date
    , lang_id
    , last_location_month
FROM MON.SPARK_FT_CBM_HANDSET_MONTHLY
WHERE PERIOD = '###SLICE_VALUE###'