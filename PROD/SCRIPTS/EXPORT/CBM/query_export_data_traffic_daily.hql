SELECT 
    regexp_replace(from_unixtime(unix_timestamp(TRANSACTION_DATE ,'yyyy-MM-dd'), 'dd-MM-yyyy'),'-','/')  AS transaction_date,
    msisdn,
    appli_type,
    idapn,
    nbytesdn,
    nbytest,
    nbytesup,
    radio_access_techno,
    roaming,
    terminal_brand,
    terminal_model,
    terminal_type
FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'