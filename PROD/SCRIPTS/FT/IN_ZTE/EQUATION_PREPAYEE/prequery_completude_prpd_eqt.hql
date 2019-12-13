SELECT IF( (A.CHECK_FILE_COUNT >=24 and B.CHECK_FILE_ALL_EXIST > 0) AND T1.MISSING_FILES = 0 AND T3.MISSING_FILES = 0 AND T5.MISSING_FILES = 0 AND T6.MISSING_FILES = 0 AND T7.MISSING_FILES = 0 AND T8.MISSING_FILES = 0 AND T9.MISSING_FILES = 0 AND T10.MISSING_FILES = 0 AND T11.MISSING_FILES = 0 , 'OK','NOK') COMPLETUDE
FROM
(SELECT COUNT(distinct original_file_name) CHECK_FILE_COUNT FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' and original_file_name not like '%POST%'
 and FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(original_file_name, -14, 8),'yyyyMMdd'), 'yyyy-MM-dd')='###SLICE_VALUE###' GROUP BY SUBSTRING(original_file_name, -14, 8)) A,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' ) B,
(SELECT  COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_DATA_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_DATA_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_DATA B WHERE FILE_DATE= '###SLICE_VALUE###' ) B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T1,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_VOICE_SMS_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_VOICE_SMS_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_VOICE_SMS B WHERE  START_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',30) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T3,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_ADJUSTMENT_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_ADJUSTMENT_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_ADJUSTMENT B WHERE CREATE_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',7) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T5,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_BALRESET_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_BALRESET_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_BALANCE_RESET B WHERE BAL_RESET_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T6,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_RECHARGE_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_RECHARGE_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_RECHARGE B WHERE  PAY_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',7) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T7,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_TRANSFER_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_TRANSFER_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_TRANSFER B WHERE PAY_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T8,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_SUBSCRIPTION_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_SUBSCRIPTION_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_SUBSCRIPTION B WHERE CREATEDDATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T9,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_ED_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_ED_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_EMERGENCY_DATA B WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T10,
(SELECT COUNT(C.FILE_NAME) MISSING_FILES
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_EC_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_EC_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_EMERGENCY_CREDIT B WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T11;
