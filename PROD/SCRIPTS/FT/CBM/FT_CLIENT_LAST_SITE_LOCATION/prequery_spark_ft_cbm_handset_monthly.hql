SELECT IF(
    A.FT_EXIST = 0
    AND B.FT_PREV_EXIST > 0
    AND C.FT_CLIENT_SITE_TRAFFIC_MONTH_EXIST > 0
    , "OK"
    , "NOK"
) FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION WHERE EVENT_MONTH = '###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_PREV_EXIST FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION WHERE EVENT_MONTH = SUBSTRING(ADD_MONTHS('###SLICE_VALUE###' || '-01', -1), 1, 7)) B,
(SELECT COUNT(*) FT_CLIENT_SITE_TRAFFIC_MONTH_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_MONTH WHERE EVENT_MONTH = '###SLICE_VALUE###') C