SELECT 
IF
(
    A_1.EXIST > 0
    AND A_2.EXIST > 0
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) EXIST FROM mon.spark_ft_omny_account_snapshot_new WHERE EVENT_DATE='###SLICE_VALUE###') A_1,
(SELECT COUNT(*) EXIST FROM cdr.spark_it_kyc_bdi_full WHERE original_file_date=DATE_ADD('###SLICE_VALUE###',1)) A_2,
(
    SELECT COUNT(*) NB_EXPORT 
    FROM
    (
        SELECT 
        * 
        FROM 
        (
            SELECT 
            * 
            FROM MON.EXPORT_HISTORY 
            WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='${hivevar:job_instanceid}'  
            ORDER BY INSERT_DATE DESC LIMIT 1
        )  T where T.STATUS = 'OK'
    ) M
) B
