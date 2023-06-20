SELECT IF(
    T_1.AGG_EXIST > 0 AND
    T_2.NB_EXPORT < 1
    ,"OK","NOK")
FROM
(
    SELECT COUNT(*) AS AGG_EXIST FROM MON.SPARK_FT_CONTRACT_SNAPSHOT 
    WHERE event_date = DATE_ADD('###SLICE_VALUE###', 1)
) T_1,
(
    SELECT COUNT(*) NB_EXPORT FROM 
    (
        SELECT * FROM 
        (
            SELECT * FROM MON.EXPORT_HISTORY WHERE event_date = '###SLICE_VALUE###'
            AND JOB_INSTANCEID = 'LOAD_EXPORT_TDD_ONBOARDING' 
            ORDER BY INSERT_DATE DESC LIMIT 1
        )  T
        WHERE T.STATUS = 'OK'
    ) M
) T_2