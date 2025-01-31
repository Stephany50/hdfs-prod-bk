SELECT IF( (A.CHECK_FILE_COUNT >=23 and B.CHECK_FILE_ALL_EXIST > 0) AND T1.MISSING_DATA = 0 AND T3.MISSING_VOICESMS = 0 AND T5.MISSING_ADJUSTMENT = 0 AND T6.MISSING_BALRESET = 0
    AND T7.MISSING_RECHARGE = 0 AND T8.MISSING_TRANSFER = 0 AND T9.MISSING_SUBSCRIPTION = 0 AND T10.MISSING_ED = 0 AND T11.MISSING_EC= 0 AND T12.MISSING_RECURRING=0 , 'OK', IF('###SLICE_VALUE###' in ('2020-02-29', '2020-02-06', '2020-02-07', '2020-06-26', '2021-11-17'), 'OK','NOK')) COMPLETUDE
FROM
(SELECT COUNT(distinct original_file_name) CHECK_FILE_COUNT FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' and original_file_name not like '%POST%'
 and FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(original_file_name, -14, 8),'yyyyMMdd'), 'yyyy-MM-dd')='###SLICE_VALUE###' GROUP BY SUBSTRING(original_file_name, -14, 8)) A,
(SELECT COUNT(*) CHECK_FILE_ALL_EXIST FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' ) B,
(SELECT  COUNT(C.FILE_NAME) MISSING_DATA
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_DATA B WHERE FILE_DATE= '###SLICE_VALUE###' ) B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T1,
(SELECT COUNT(C.FILE_NAME) MISSING_VOICESMS
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_VOICE_SMS B WHERE  FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T3,
(SELECT COUNT(C.FILE_NAME) MISSING_ADJUSTMENT
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
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_ADJUSTMENT B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T5,
(SELECT COUNT(C.FILE_NAME) MISSING_BALRESET
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
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_BALANCE_RESET B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T6,
(SELECT COUNT(C.FILE_NAME) MISSING_RECHARGE
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_RECHARGE B WHERE  FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T7,
(SELECT COUNT(C.FILE_NAME) MISSING_TRANSFER
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_TRANSFER B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T8,
(SELECT COUNT(C.FILE_NAME) MISSING_SUBSCRIPTION
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T9,
(SELECT COUNT(C.FILE_NAME) MISSING_ED
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
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_EMERGENCY_DATA B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T10,
(SELECT COUNT(C.FILE_NAME) MISSING_EC
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
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_EMERGENCY_CREDIT B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T11,
(SELECT COUNT(A.FILE_NAME) MISSING_RECURRING
FROM (
    SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.SPARK_IT_ZTE_CHECK_FILE WHERE CDR_DATE = '###SLICE_VALUE###' AND CDR_TYPE = 'ZTE_RECURR_CDR'
    UNION
    SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.SPARK_IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '###SLICE_VALUE###' AND FILE_TYPE = 'ZTE_RECURR_CDR'
) A
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_RECURRING B WHERE FILE_DATE= '###SLICE_VALUE###') B ON B.ORIGINAL_FILE_NAME = A.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
)T12
