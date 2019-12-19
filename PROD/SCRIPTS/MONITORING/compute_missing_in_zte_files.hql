
INSERT INTO  MON.MISSING_FILES
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_DATA_CDR' FLUX_NAME, C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_DATA B WHERE FILE_DATE = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_DATA_POST_CDR' FLUX_NAME, C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_DATA_POST_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_DATA_POST_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_DATA_POST B WHERE START_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',90) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_VOICE_SMS_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_VOICE_SMS B WHERE  START_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',30) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_VOICE_SMS_POST_CDR' FLUX_NAME, C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_VOICE_SMS_POST_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_VOICE_SMS_POST_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_VOICE_SMS_POST B WHERE START_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',30) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_ADJUSTMENT_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_ADJUSTMENT B WHERE CREATE_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',7) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_BALRESET_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_RECHARGE_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_RECHARGE B WHERE  PAY_DATE BETWEEN DATE_SUB('###SLICE_VALUE###',7) AND '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_TRANSFER_CDR' FLUX_NAME, C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_TRANSFER B WHERE PAY_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_SUBSCRIPTION_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION B WHERE CREATEDDATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_ED_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_EMERGENCY_DATA B WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT 'IN' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZTE_EC_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
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
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_EMERGENCY_CREDIT B WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and TO_DATE(ORIGINAL_FILE_DATE) = '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'CHECKFILE' FLUX_TYPE,'ZTE_CHECKFILE_ALL' FLUX_NAME,CONCAT('IN_ZTE_ALL_CHECK_FILELIST','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,27,8) = SUBSTRING(C.FILE_NAME,27,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT DISTINCT 'IN' TABLE_SOURCE,'CHECKFILE' FLUX_TYPE,'ZTE_CHECKFILE' FLUX_NAME, FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
FROM (
    SELECT
        CONCAT('IN_ZTE_CHECK_FILELIST','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),LPAD(CAST(SEQUENCE AS STRING),2,'0'),'.','csv') FILE_NAME
    FROM (
        SELECT GENERATE_SEQUENCE_FROM_INTERVALE(0,23) SEQ
     )C
    LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###'  AND ORIGINAL_FILE_NAME NOT LIKE '%POST%') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,10) = SUBSTRING(C.FILE_NAME,-12,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT DISTINCT 'IN' TABLE_SOURCE,'CHECKFILE' FLUX_TYPE,'ZTE_CHECKFILE_POST' FLUX_NAME, FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE
FROM (
    SELECT
        CONCAT('IN_ZTE_CHECK_FILELIST_POSTPAID','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),LPAD(CAST(SEQUENCE AS STRING),2,'0'),'.','csv') FILE_NAME
    FROM (
        SELECT GENERATE_SEQUENCE_FROM_INTERVALE(0,23) SEQ
     )C
    LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###'  AND ORIGINAL_FILE_NAME LIKE '%POST%') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,10) = SUBSTRING(C.FILE_NAME,-12,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
