SELECT IF(
    T_1.FT_EXIST = 0
    AND T_2.FT_EXIST > 0
    AND T_3.FT_EXIST > 0
    AND T_4.FT_EXIST > 0
    , 'OK'
    , 'NOK'
    
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE='###SLICE_VALUE###' and kpi_name in ('CALL_BOX', 'RECHARGES', 'CASHIN', 'CASHOUT', 'REVENU_OM', 'PARC_ACTIF_OM', 'POS_OM') ) T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR WHERE EVENT_DATE='###SLICE_VALUE###') T_4