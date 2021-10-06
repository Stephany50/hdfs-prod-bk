SELECT IF(T_1.CHECK_LOAN_ACCOUNT > 0 AND T_2.MISSING_FILES = 0, 'OK','NOK')
FROM
(SELECT COUNT(REPLACE(FILE_NAME, 'pla_', '')) CHECK_LOAN_ACCOUNT FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' and file_name like '%LoanCdr_ipp%') T_1,
(
    SELECT COUNT(C.FILE_NAME) MISSING_FILES
    FROM
    (
        SELECT
            A.FILE_NAME
        FROM
        (
            select replace(CDR_NAME, 'pla_', '') file_name FROM CDR.SPARK_IT_ZTE_CHECK_FILE where CDR_DATE = '###SLICE_VALUE###' and CDR_NAME like '%in_pr_ipp%'
        ) A
    )C
    WHERE
    NOT EXISTS
    (
        SELECT 1  
        FROM CDR.SPARK_IT_ZTE_LOAN_CDR B
        WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###' AND  B.ORIGINAL_FILE_NAME = C.FILE_NAME
    )
) T_2