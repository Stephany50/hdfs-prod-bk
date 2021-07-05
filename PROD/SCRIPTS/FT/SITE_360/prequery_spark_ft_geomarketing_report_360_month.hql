SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.NB_EXPORT + datediff(concat('###SLICE_VALUE###', '-01'), last_day(concat('###SLICE_VALUE###', '-01'))) - 1 = 0
    AND T_3.NB_EXPORT + datediff(concat('###SLICE_VALUE###', '-01'), last_day(concat('###SLICE_VALUE###', '-01'))) - 1 = 0
    AND T_4.NB_EXPORT + datediff(concat('###SLICE_VALUE###', '-01'), last_day(concat('###SLICE_VALUE###', '-01'))) - 1 = 0
    AND T_5.NB_EXPORT + datediff(concat('###SLICE_VALUE###', '-01'), last_day(concat('###SLICE_VALUE###', '-01'))) - 1 = 0
    AND T_6.NB_EXPORT + datediff(concat('###SLICE_VALUE###', '-01'), last_day(concat('###SLICE_VALUE###', '-01'))) - 1 = 0
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MONTH WHERE EVENT_MONTH='###SLICE_VALUE###') T_1,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_2,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_3,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_4,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_5,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_6

