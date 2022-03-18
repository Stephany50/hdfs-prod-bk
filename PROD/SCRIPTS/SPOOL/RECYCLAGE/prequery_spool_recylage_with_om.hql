SELECT 
IF
(
    A.EXIST > 0
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) EXIST FROM SPOOL.SPOOL_RECYCLAGE_OM WHERE EVENT_DATE ='###SLICE_VALUE###') A,
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

