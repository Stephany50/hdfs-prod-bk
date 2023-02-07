SELECT 
    IF(
        A.FT_EXIST = 0 and 
        B.FT_GEO_REPORT_360_EXIST>=26 AND 
        C.FT_SITE_360_EXISTS>0  AND 
        D.FT_SUBSCRIPTION_EXISTS>0 AND
        E.FT_CRA_GPRS_EXISTS>0 AND
        F.FT_CLIENT_LAST_SITE_DAY_EXISTS>0 AND
        G.FT_ACCOUNT_ACTIVITY_EXISTS>0 AND
        H.IT_OMNY_TRANSACTION_EXISTS>0 
        ,"OK","NOK"
    ) 
FROM
    (SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_MONITORING_RURAL_DAILY WHERE EVENT_DATE='###SLICE_VALUE###') A,
    (SELECT COUNT(distinct kpi_name) FT_GEO_REPORT_360_EXIST FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 WHERE EVENT_DATE= '###SLICE_VALUE###') B,
    (SELECT COUNT(*) FT_SITE_360_EXISTS FROM mon.spark_ft_site_360 WHERE EVENT_DATE='###SLICE_VALUE###') C,
    (SELECT COUNT(*) FT_SUBSCRIPTION_EXISTS FROM MON.SPARK_FT_SUBSCRIPTION  WHERE TRANSACTION_DATE='###SLICE_VALUE###') D,
    (SELECT COUNT(*) FT_CRA_GPRS_EXISTS FROM MON.SPARK_FT_CRA_GPRS  WHERE session_date='###SLICE_VALUE###') E,
    (SELECT COUNT(*) FT_CLIENT_LAST_SITE_DAY_EXISTS FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY  WHERE EVENT_DATE='###SLICE_VALUE###') F,
    (SELECT COUNT(*) FT_ACCOUNT_ACTIVITY_EXISTS FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  WHERE EVENT_DATE='###SLICE_VALUE###') G,
    (SELECT COUNT(*) IT_OMNY_TRANSACTION_EXISTS FROM cdr.spark_it_omny_transactions  WHERE transfer_datetime='###SLICE_VALUE###') H

    