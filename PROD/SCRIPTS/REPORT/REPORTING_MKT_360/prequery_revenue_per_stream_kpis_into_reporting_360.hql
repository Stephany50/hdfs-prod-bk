SELECT IF(
            T1.KPI_IS_LOAD=0 AND 
            T2.COUNT_SRC_EXISTS = 13 AND
            T3.FT_EXISTS > 0 AND
            T4.FT_EXISTS > 0 AND
            T5.FT_EXISTS > 0
        ,'OK','NOK')
 FROM
(SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_A_REPORTING_360 WHERE PROCESSING_DATE='###SLICE_VALUE###' AND KPI_GROUP_NAME='REVENUE PER STREAM')T1,
(
    SELECT COUNT(DISTINCT SOURCE_TABLE) COUNT_SRC_EXISTS FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG 
    WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND 
        SOURCE_TABLE IN 
        (
            'FT_EMERGENCY_DATA',
            'FT_GSM_TRAFFIC_REVENUE_DAILY', 
            'IT_ZTE_ADJUSTMENT', 
            'FT_DATA_TRANSFER', 
            'FT_OVERDRAFT', 
            'FT_A_SUBSCRIPTION', 
            'FT_CONTRACT_SNAPSHOT', 
            'FT_CREDIT_TRANSFER', 
            'FT_SUBS_RETAIL_ZEBRA', 
            'REGIONAL_DOM_DASHBOARD', 
            'FT_A_GPRS_ACTIVITY',
            'FT_USERS_DAY',
            'FT_USERS_DATA_DAY'
        )
) T2,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE PERIOD='###SLICE_VALUE###') T3,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE='###SLICE_VALUE###') T4,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###') T5

