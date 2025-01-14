SELECT IF(
    T1.KPI_IS_LOAD=0 
    and T2.FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_INSERTED_SOURCES = 8
    and T3.FT_EXISTS>0
    and T4.FT_EXISTS > 0
    -- and T4.FT_EXISTS=31
    -- and T5.FT_EXISTS = datediff('###SLICE_VALUE###', concat(SUBSTRING('###SLICE_VALUE###', 1, 7),'-01')) + 1
    , 'OK'
    , 'NOK'
)
FROM
(
    SELECT COUNT(*) KPI_IS_LOAD 
    FROM AGG.SPARK_FT_A_REPORTING_360 
    WHERE 
        PROCESSING_DATE='###SLICE_VALUE###' 
        AND KPI_GROUP_NAME='SUBSCRIBERS'
        AND KPI_NAME IN 
            (
                'PARC_ART', 
                'PARC_GROUP',
                'PARC_OM_DAILY',
                'PARC_OM_MTD',
                'PARC_OM_30Jrs',
                'DATA_USERS_30Jrs',
                'DATA_USERS_DAILY',
                'DATA_USERS_30Jrs_IN',
                'DATA_USERS_DAILY_IN',
                'DAILY_BASE',
                'CHARGED_BASE',
                'GROSS_ADD',
                'CHARGED_BASE_LIGHT',
                'GROSS_ADDS_MTD',
                'DATA_USERS_MTD',
                'DATA_USERS_MTD_IN'
            )
) T1,
(
    SELECT COUNT(distinct source_table) FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_INSERTED_SOURCES
    FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW
    WHERE TRANSACTION_DATE='###SLICE_VALUE###'
        and source_table in 
        (
            'FT_COMMERCIAL_SUBSCRIB_SUMMARY', 
            'FT_GROUP_SUBSCRIBER_SUMMARY', 
            'FT_OTARIE_USERS_TRAFFIC', 
            'FT_OTARIE_DATA_TRAFFIC_DAY', 
            'IT_OMNY_TRANSACTIONS', 
            'FT_ACCOUNT_ACTIVITY',
            'FT_USERS_DATA_DAY',
            'FT_USERS_DAY'
        )
) T2,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE=date_add('###SLICE_VALUE###', 1)) T3,
(SELECT COUNT(*) FT_EXISTS FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE PERIOD = '###SLICE_VALUE###') T4
-- (SELECT COUNT(DISTINCT PERIOD) FT_EXISTS FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE PERIOD BETWEEN DATE_SUB('###SLICE_VALUE###', 30) AND '###SLICE_VALUE###') T4 --,
-- (
--     select count(distinct transaction_date) FT_EXISTS
--     from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW
--     where transaction_date BETWEEN concat(SUBSTRING('###SLICE_VALUE###', 1, 7), '-01')  AND '###SLICE_VALUE###'
-- ) T5