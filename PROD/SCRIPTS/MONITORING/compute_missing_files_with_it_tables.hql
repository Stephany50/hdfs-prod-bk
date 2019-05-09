
add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
create temporary function GENERATE_SEQUENCE_FROM_INTERVALE as 'cm.orange.bigdata.udf.GenerateSequenceFromIntervale';

--INSERT INTO MISSING_FILES
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_DATA_CDR' FLUX_NAME, C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_DATA_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_DATA_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_DATA B WHERE START_DATE BETWEEN DATE_SUB('2019-05-08',90) AND '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_DATA_POST_CDR' FLUX_NAME, C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_DATA_POST_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_DATA_POST_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_DATA_POST B WHERE START_DATE BETWEEN DATE_SUB('2019-05-08',90) AND '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_VOICE_SMS_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_VOICE_SMS_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_VOICE_SMS_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_VOICE_SMS B WHERE  START_DATE BETWEEN DATE_SUB('2019-05-08',30) AND '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_VOICE_SMS_POST_CDR' FLUX_NAME, C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_VOICE_SMS_POST_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_VOICE_SMS_POST_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_VOICE_SMS_POST B WHERE START_DATE BETWEEN DATE_SUB('2019-05-08',30) AND '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_ADJUSTMENT_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_ADJUSTMENT_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_ADJUSTMENT_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_ADJUSTMENT B WHERE CREATE_DATE BETWEEN DATE_SUB('2019-05-08',7) AND '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_BALRESET_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_BALRESET_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_BALRESET_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_BALANCE_RESET B WHERE BAL_RESET_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_RECHARGE_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_RECHARGE_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_RECHARGE_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_RECHARGE B WHERE  PAY_DATE BETWEEN DATE_SUB('2019-05-08',7) AND '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_TRANSFER_CDR' FLUX_NAME, C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_TRANSFER_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_TRANSFER_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_TRANSFER B WHERE PAY_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_SUBSCRIPTION_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_SUBSCRIPTION_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_SUBSCRIPTION_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_SUBSCRIPTION B WHERE CREATEDDATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_ED_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_ED_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_ED_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_EMERGENCY_DATA B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_EC_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT REPLACE(CDR_NAME, 'pla_', '') FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE WHERE CDR_DATE = '2019-05-08' AND CDR_TYPE = 'ZTE_EC_CDR'
        UNION
        SELECT REPLACE(FILE_NAME, 'pla_', '') FILE_NAME  FROM CDR.IT_ZTE_CHECK_FILE_ALL WHERE FILE_DATE = '2019-05-08' AND FILE_TYPE = 'ZTE_EC_CDR'
     ) A
)C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_EMERGENCY_CREDIT B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON B.ORIGINAL_FILE_NAME = C.FILE_NAME
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_SUBS_EXTRACT' FLUX_NAME,CONCAT('Data_etract_SUBS','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_SUBS_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_PROD_SPEC_EXTRACT' FLUX_NAME,CONCAT('Data_etract_PROD_SPEC','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_PROD_SPEC_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_PROD_EXTRACT' FLUX_NAME,CONCAT('Data_etract_PROD','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_PROD_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_PRICE_PLAN_EXTRACT' FLUX_NAME,CONCAT('Data_etract_PRICE_PLAN','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_PRICE_PLAN_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_CUST_EXTRACT' FLUX_NAME,CONCAT('Data_etract_CUST','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_CUST_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_BAL_EXTRACT' FLUX_NAME,CONCAT('Data_etract_BAL','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_BAL_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_ACCT_EXTRACT' FLUX_NAME,CONCAT('Data_etract_ACCT','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_ACCT_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_ACC_NBR_EXTRACT' FLUX_NAME,CONCAT('Data_etract_ACC_NBR','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_ACC_NBR_EXTRACT B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_DEL_EXPBAL' FLUX_NAME,CONCAT('Del_ExpBal','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_DEL_EXPBAL B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_BAL_SNAP' FLUX_NAME,CONCAT('bal_pr-ocs21','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','dat') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_BAL_SNAP B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_PROFILE_CDR' FLUX_NAME,CONCAT('Profile','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_PROFILE B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_PROFILE_CDR' FLUX_NAME,CONCAT('Profile','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_PROFILE B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_CHECKFILE_ALL' FLUX_NAME,CONCAT('IN_ZTE_ALL_CHECK_FILELIST','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE_ALL B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,27,8) = SUBSTRING(C.FILE_NAME,27,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT DISTINCT '2019-05-08' ORIGINAL_FILE_DATE,'IN' TABLE_SOURCE,'ZTE_CHECKFILE' FLUX_NAME, FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM (
    SELECT
        CONCAT('IN_ZTE_CHECK_FILELIST','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),LPAD(CAST(SEQUENCE AS STRING),2,'0'),'.','csv') FILE_NAME
    FROM (
        SELECT GENERATE_SEQUENCE_FROM_INTERVALE(0,23) SEQ
     )C
    LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZTE_CHECK_FILE B WHERE ORIGINAL_FILE_DATE='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,10) = SUBSTRING(C.FILE_NAME,-12,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'ZEBRA' TABLE_SOURCE,'ZEBRA_MASTER_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT  DISTINCT reverse(split(reverse(CDR_NAME), '[/]')[0]) FILE_NAME FROM CDR.IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '2019-05-08' AND CDR_NAME LIKE '%master%'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZEBRA_MASTER B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON split(B.ORIGINAL_FILE_NAME, '_')[0] = split(C.FILE_NAME, '\\.')[0]
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-05-08' ORIGINAL_FILE_DATE,'ZEBRA' TABLE_SOURCE,'ZEBRA_TRANSAC_CDR' FLUX_NAME,C.FILE_NAME MISSING_FILES, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT DISTINCT  reverse(split(reverse(CDR_NAME), '[/]')[0]) FILE_NAME FROM CDR.IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '2019-05-08' AND CDR_NAME LIKE '%channelTransaction%'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZEBRA_TRANSAC B WHERE TRANSFER_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON split(B.ORIGINAL_FILE_NAME, '_')[0] = split(C.FILE_NAME, '\\.')[0]
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OTARIE' TABLE_SOURCE,'OTARIE_TRAFFIC_CUST' FLUX_NAME,CONCAT('traffic_customer','_','2019-05-08','_','*.csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_TRAFFIC_CUST_OTARIE B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08' ) B ON SUBSTRING(B.ORIGINAL_FILE_NAME,18,10) = SUBSTRING(C.FILE_NAME,18,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OTARIE' TABLE_SOURCE,'OTARIE_TRAFFIC_CUST_CELL' FLUX_NAME,CONCAT('traffic_customer_cell','_','2019-05-08','_','*.csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_TRAFFIC_CUST_CELL_OTARIE B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,23,10) = SUBSTRING(C.FILE_NAME,23,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OTARIE' TABLE_SOURCE,'OTARIE_CUSTOMER_SP' FLUX_NAME,CONCAT('customers_sp_day','_','2019-05-08','_','*.csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_CUSTOMER_SP_OTARIE B WHERE EVENT_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,18,10) = SUBSTRING(C.FILE_NAME,18,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'OM_ALL_BALANCE' FLUX_NAME,CONCAT('AllBalance','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_ALL_BALANCE B WHERE ORIGINAL_FILE_DATE ='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'OM_APGL' FLUX_NAME,CONCAT('GL_TRX_INTERFACE','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_APGL B WHERE TRANSACTION_DATE =  '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'OM_COMMISSIONS' FLUX_NAME,CONCAT('CommissionsDetails','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_COMMISSIONS B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'OM_SERVICES_CHARGES' FLUX_NAME,CONCAT('ServicesChargesDetails','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_SERVICES_CHARGES B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'OM_SUBSCRIBERS' FLUX_NAME,CONCAT('Subscribers','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_SUBSCRIBERS B WHERE MODIFICATION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'OM_TRANSACTIONS' FLUX_NAME,CONCAT('Transactions','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_TRANSACTIONS B WHERE TRANSACTION_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'BDI' TABLE_SOURCE,'BDI' FLUX_NAME,CONCAT('bdi','_',DATE_FORMAT('2019-05-08','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_BDI B WHERE ORIGINAL_FILE_DATE ='2019-05-08') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,5,8) = SUBSTRING(C.FILE_NAME,5,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-05-08' ORIGINAL_FILE_DATE,'P2P' TABLE_SOURCE,'P2P_LOG' FLUX_NAME,CONCAT('p2pCommands','.','2019-05-08','.','log') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_P2P_LOG B WHERE START_DATE = '2019-05-08' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-05-08' ) B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-14,10) = SUBSTRING(C.FILE_NAME,13,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT
'2019-05-08' ORIGINAL_FILE_DATE,'MVAS' TABLE_SOURCE,'IT_SMSC_MVAS_A2P' FLUX_NAME,CONCAT('prm20190312_010300','',CAST(SEQUENCE AS STRING),'_','MAKMOneSMAPP2.unl') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX)  SEQ FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MVAS_SOURCE ORDER BY INDEX) PREVIOUS,INDEX FROM (
            SELECT
                DISTINCT
                CAST(SUBSTRING(original_file_name,19,5) AS INT) INDEX,
                substr(original_file_name,25,13) MVAS_SOURCE
           from CDR.IT_SMSC_MVAS_A2P
           where WRITE_DATE = '2019-05-08' AND TO_DATE(ORIGINAL_FILE_DATE)='2019-05-08'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
UNION
SELECT
'2019-05-08' ORIGINAL_FILE_DATE,'MSC' TABLE_SOURCE,'CRA_MSC_HUAWEI' FLUX_NAME,SEQUENCE FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX)  SEQ FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
            SELECT
                DISTINCT
                CAST(SUBSTRING(SOURCE,11,9) AS INT) INDEX,
                SUBSTRING(SOURCE,5,11) MSC_TYPE
            FROM CDR.IT_CRA_MSC_HUAWEI
            WHERE CALLDATE = '2019-05-08' AND TO_DATE(ORIGINAL_FILE_DATE)='2019-05-08'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
