SELECT IF(
    T_1.AGG_EXIST > 0 AND
    T_2.NB_EXPORT < 1
    ,"OK","NOK")
FROM
(
    SELECT COUNT(*) FROM MON.SPARK_FT_SUBSCRIPTION 
    WHERE transaction_date = '###SLICE_VALUE###'
) T_1,
(
    SELECT COUNT(*) NB_EXPORT FROM 
    (
        SELECT * FROM 
        (
            SELECT * FROM MON.EXPORT_HISTORY WHERE event_date = '###SLICE_VALUE###'
            AND JOB_INSTANCEID = 'LOAD_EXPORT_TDD_CX_REVENUE' 
            ORDER BY INSERT_DATE DESC LIMIT 1
        )  T
        WHERE T.STATUS = 'OK'
    ) M
) T_2