SELECT IF(
    T_1.FT_EXIST = 0
    , 'OK'
    , 'NOK'
    
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_TECHNO_TRAFIC_GEO_REPORT_360_WEEK WHERE EVENT_WEEK= concat(year('###SLICE_VALUE###'), 'S', case when length(WEEKOFYEAR('###SLICE_VALUE###')) = 1 then concat('0', WEEKOFYEAR('###SLICE_VALUE###')) else WEEKOFYEAR('###SLICE_VALUE###') end)) T_1
-- (SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###') T_2
