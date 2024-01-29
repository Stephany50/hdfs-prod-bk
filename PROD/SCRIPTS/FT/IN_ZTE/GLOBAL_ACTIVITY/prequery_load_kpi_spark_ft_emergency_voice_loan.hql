SELECT IF(T1.KPI_IS_LOAD=0 AND T2.FT_EXISTS>0 AND T_4.MISSING_FILES = 0,'OK','NOK')
FROM
    (SELECT COUNT(*) KPI_IS_LOAD FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY  WHERE TRANSACTION_DATE='###SLICE_VALUE###' AND SOURCE_DATA in ('ZTE_LOAN_CDR'))T1,
    (SELECT COUNT(*) FT_EXISTS FROM CDR.SPARK_IT_ZTE_LOAN_CDR WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###')T2,
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
