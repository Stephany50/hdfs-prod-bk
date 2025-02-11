SELECT IF(
    T_1.FT_EXIST = 7
    AND T_2.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(DISTINCT PERIOD_START_DATE) FT_EXIST FROM CDR.SPARK_IT_SC_OSS_2G WHERE CONCAT(YEAR(PERIOD_START_DATE) , LPAD(WEEKOFYEAR(PERIOD_START_DATE), 2, 0)) = CONCAT(YEAR('###SLICE_VALUE###') , LPAD(WEEKOFYEAR('###SLICE_VALUE###'), 2, 0))) T_1,
(
    SELECT COUNT(*) NB_EXPORT FROM
    (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE=CONCAT(YEAR('###SLICE_VALUE###') , LPAD(WEEKOFYEAR('###SLICE_VALUE###'), 2, 0)) AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2
