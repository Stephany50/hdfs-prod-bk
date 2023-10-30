SELECT IF(
    T_1.FT_EXIST > 0
    AND T_2.NB_EXPORT < 1
    AND T_4.MISSING_FILES = 0
    , "OK"
    , "NOK"
)
FROM
(SELECT COUNT(*) FT_EXIST FROM CDR.SPARK_IT_ZTE_LOAN_CDR WHERE original_file_date  ='###SLICE_VALUE###') T_1,
(
    SELECT COUNT(*) NB_EXPORT FROM
    (SELECT * FROM (SELECT * FROM MON.EXPORT_HISTORY WHERE EVENT_DATE='###SLICE_VALUE###' AND JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2,
(
    SELECT COUNT(C.FILE_NAME) MISSING_FILES 
    FROM
        (
            SELECT A.FILE_NAME
            FROM
            (
                select replace( replace (replace (CDR_NAME, 'LoanCdr', 'in_pr'), "cdr", "csv"), 'pla_', '') file_name FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE FILE_DATE = '###SLICE_VALUE###' and CDR_DATE = '###SLICE_VALUE###' AND CDR_NAME like "%ipp%"  and CDR_TYPE = 'UNKNOWN VALUE'
                UNION
                SELECT replace( replace (replace (file_name, 'LoanCdr', 'in_pr'), "cdr", "csv"), 'pla_', '') file_name FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND  FILE_NAME like "%ipp%"   AND FILE_TYPE = 'UNKNOWN VALUE'
            ) A
        )C
    WHERE
    NOT EXISTS
        (
            SELECT 1 FROM CDR.SPARK_IT_ZTE_LOAN_CDR B
            WHERE 
                b.original_file_date ='###SLICE_VALUE###'  and  B.ORIGINAL_FILE_NAME = C.FILE_NAME
        )
) T_4
