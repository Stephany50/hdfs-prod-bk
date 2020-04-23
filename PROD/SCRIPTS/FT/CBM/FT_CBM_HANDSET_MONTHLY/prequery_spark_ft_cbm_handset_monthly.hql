SELECT IF(
    A.FT_EXIST = 0
    AND B.FT_CONTRACT_SNAPSHOT_EXIST > 0
    AND C.FT_IMEI_TRAFFIC_MONTHLY_EXIST > 0
    AND D.FT_CLIENT_LAST_SITE_LOCATION_EXIST > 0
    , "OK"
    , "NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CBM_HANDSET_MONTHLY WHERE PERIOD = '###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_CONTRACT_SNAPSHOT_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = LAST_DAY('###SLICE_VALUE###'||'-01')) B,
(SELECT COUNT(*) FT_IMEI_TRAFFIC_MONTHLY_EXIST FROM MON.SPARK_FT_IMEI_TRAFFIC_MONTHLY WHERE SMONTH = SUBSTRING('###SLICE_VALUE###', 1, 4)||SUBSTRING('###SLICE_VALUE###', 6, 2)) C,
(SELECT COUNT(*) FT_CLIENT_LAST_SITE_LOCATION_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION WHERE EVENT_MONTH = '###SLICE_VALUE###') D