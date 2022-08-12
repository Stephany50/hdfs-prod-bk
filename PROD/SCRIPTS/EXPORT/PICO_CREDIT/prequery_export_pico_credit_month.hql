SELECT IF( 
    T1.NB_DISTINCT_KPIS >= 30 AND
    T2.IT_EXIST > 0 AND 
    T3.FT_EXIST > 0 AND 
    T_6.NB_EXPORT < 1 ,"OK","NOK")
FROM
(
    SELECT SUM(KPI) NB_DISTINCT_KPIS
    FROM
    (
        SELECT 
            EVENT_MONTH,
            COUNT(distinct KPI) KPI
        FROM MON.SPARK_TT_PICO_KPIS 
        WHERE EVENT_MONTH IN 
        (
            '###SLICE_VALUE###',
            SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -1), 0, 7),
            SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -2), 0, 7),
            SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -3), 0, 7),
            SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -4), 0, 7),
            SUBSTR(ADD_MONTHS('###SLICE_VALUE###-01', -5), 0, 7)
        ) 
        AND KPI IN ('TELCO_LOANS_DELAY', 'ATC_LOANS_AMT', 'OM_DEPOSIT_AMT', 'CALLS_ON_N', 'OM_BANK_TRF_AMT')
        GROUP BY EVENT_MONTH
    ) A
) T1,
(SELECT COUNT(*) IT_EXIST FROM cdr.spark_it_omny_account_snapshot_new WHERE ORIGINAL_FILE_DATE=ADD_MONTHS('###SLICE_VALUE###-01', 1)) T2,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE=ADD_MONTHS('###SLICE_VALUE###-01', 1)) T3,
(select COUNT(*) NB_EXPORT FROM (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T WHERE T.STATUS = 'OK') M) T_6
