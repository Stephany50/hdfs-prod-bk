SELECT IF(
    T_1.COUNT_CLSD > 0
    AND T_2.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) COUNT_CLSD FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE=DATE_SUB(CURRENT_DATE(), 3)) T_1,
(
    SELECT COUNT(*) NB_EXPORT FROM
    (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2