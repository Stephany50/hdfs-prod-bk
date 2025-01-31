SELECT 
    IF(
        A.FT_EXIST = 0 AND 
        B.FT_REPORT_360_MTD > 0 AND 
        C.FT_SUBSCRIPTION_COUNT = NB_DAYS AND
        D.FT_CRA_GPRS_COUNT = NB_DAYS AND
        E.FT_CLIENT_LAST_SITE_DAY_COUNT = NB_DAYS AND
        F.FT_ACCOUNT_ACTIVITY_EXISTS>0 AND
        G.IT_OMNY_TRANSACTION_COUNT = NB_DAYS AND
        H.FT_CLIENT_SITE_TRAFFIC_DAY_COUNT = NB_DAYS
        ,"OK","NOK"
    ) 
FROM
    (SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_MONITORING_RURAL_MONTHLY WHERE EVENT_MONTH='###SLICE_VALUE###') A,
    (SELECT COUNT(*) FT_REPORT_360_MTD FROM MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD WHERE to_date(EVENT_DATE) = to_date(last_day(concat("###SLICE_VALUE###",'-01')))) B,
    (SELECT COUNT(DISTINCT TRANSACTION_DATE) FT_SUBSCRIPTION_COUNT FROM MON.SPARK_FT_SUBSCRIPTION  WHERE to_date(TRANSACTION_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))) C,
    (SELECT COUNT(DISTINCT SESSION_DATE) FT_CRA_GPRS_COUNT FROM MON.SPARK_FT_CRA_GPRS  WHERE to_date(SESSION_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))) D,
    (SELECT COUNT(DISTINCT EVENT_DATE) FT_CLIENT_LAST_SITE_DAY_COUNT FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY  WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))) E,
    (SELECT COUNT(*) FT_ACCOUNT_ACTIVITY_EXISTS FROM MON.SPARK_FT_ACCOUNT_ACTIVITY  WHERE to_date(EVENT_DATE) =  to_date(last_day(concat("###SLICE_VALUE###",'-01')))) F,
    (SELECT COUNT(DISTINCT TRANSFER_DATETIME) IT_OMNY_TRANSACTION_COUNT FROM CDR.SPARK_IT_OMNY_TRANSACTIONS  WHERE to_date(TRANSFER_DATETIME) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))) G,
    (SELECT COUNT(DISTINCT EVENT_DATE) FT_CLIENT_SITE_TRAFFIC_DAY_COUNT FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY  WHERE to_date(EVENT_DATE) BETWEEN to_date(concat("###SLICE_VALUE###",'-01'))  AND to_date(last_day(concat("###SLICE_VALUE###",'-01')))) H,
    (SELECT DATEDIFF(to_date(last_day(concat("###SLICE_VALUE###",'-01'))), to_date(concat("###SLICE_VALUE###",'-01'))) + 1 NB_DAYS) I