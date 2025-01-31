SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 0
    , 'OK'
    , 'NOK'
    
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE='###SLICE_VALUE###' and kpi_name in ('REVENU_VOIX_PYG')) T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2