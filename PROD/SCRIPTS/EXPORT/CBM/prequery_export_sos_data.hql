SELECT IF(
    T_1.FT_EXIST > 0
    AND T_2.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_EMERGENCY_DATA WHERE TRANSACTION_DATE  ='###SLICE_VALUE###') T_1,
(
    SELECT COUNT(*) NB_EXPORT FROM
    (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2
