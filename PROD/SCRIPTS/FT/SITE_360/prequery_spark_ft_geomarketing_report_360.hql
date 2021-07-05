SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 0
    AND T_3.FT_EXIST > 0
    AND T_4.FT_EXIST > 0
    AND T_5.FT_EXIST > 0
    AND T_6.FT_EXIST > 0
    AND T_7.FT_EXIST > 0
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_MSISDN_BAL_USAGE_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_4,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_5,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_6,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_7