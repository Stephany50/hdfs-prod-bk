SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 0
    , 'OK'
    , 'NOK'
    
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE='###SLICE_VALUE###' and kpi_name in ('PARC_GROUPE')) T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2