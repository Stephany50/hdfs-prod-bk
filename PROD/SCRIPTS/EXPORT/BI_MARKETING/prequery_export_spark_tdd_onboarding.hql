SELECT 
IF
(
    A_1.EXIST > 0
    AND B.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) EXIST FROM MON.SPARK_FT_TDD_PARC WHERE event_date = '###SLICE_VALUE###') A_1,
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
