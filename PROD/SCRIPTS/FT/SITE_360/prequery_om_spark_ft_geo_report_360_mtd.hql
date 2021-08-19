SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.NB_EXPORT + datediff(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), '###SLICE_VALUE###') - 1 = 0 
    AND T_3.NB_EXPORT + datediff(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), '###SLICE_VALUE###') - 1 = 0 
    , 'OK'
    , 'NOK'
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD WHERE EVENT_DATE='###SLICE_VALUE###' and kpi_name in ('POS_OM', 'PARC_ACTIF_OM')) T_1,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###') T_2,
(SELECT COUNT(DISTINCT event_date) NB_EXPORT FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###') T_3

