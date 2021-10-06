SELECT
    period
    ,msisdn
    ,da_id
    ,da_unit
    ,da_type
    ,activity_type
    ,used_amt
    ,service_class
FROM MON.SPARK_FT_CBM_DA_USAGE_DAILY
WHERE PERIOD='###SLICE_VALUE###'