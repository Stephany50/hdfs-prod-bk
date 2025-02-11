SELECT IF(
    T_1.IT_ZTE_SUBS_EXTRACT_EXIST > 0
    AND T_2.IT_ZTE_BAL_EXTRACT_EXIST > 0
    AND T_3.NB_EXPORT < 1
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) IT_ZTE_SUBS_EXTRACT_EXIST FROM CDR.SPARK_IT_ZTE_SUBS_EXTRACT WHERE original_file_date  ='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) IT_ZTE_BAL_EXTRACT_EXIST FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT WHERE original_file_date  ='###SLICE_VALUE###') T_2,
(
    SELECT COUNT(*) NB_EXPORT FROM
    (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_3
