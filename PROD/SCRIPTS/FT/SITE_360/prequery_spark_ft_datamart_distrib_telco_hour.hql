SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 1
    AND T_3.FT_EXIST > 1
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_REFILL WHERE REFILL_DATE='###SLICE_VALUE###') T_3