INSERT INTO  MISSING_FILES
SELECT '2019-08-05' ORIGINAL_FILE_DATE,'ZEBRA' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZEBRA_MASTER_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT  DISTINCT reverse(split(reverse(CDR_NAME), '[/]')[0]) FILE_NAME FROM CDR.IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '2019-08-05' AND CDR_NAME LIKE '%master%'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZEBRA_MASTER B WHERE TRANSACTION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON split(B.ORIGINAL_FILE_NAME, '_')[0] = split(C.FILE_NAME, '\\.')[0]
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT '2019-08-05' ORIGINAL_FILE_DATE,'ZEBRA' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'ZEBRA_TRANSAC_CDR' FLUX_NAME,C.FILE_NAME FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM
(
    SELECT
     A.FILE_NAME
    FROM
     (
        SELECT DISTINCT  reverse(split(reverse(CDR_NAME), '[/]')[0]) FILE_NAME FROM CDR.IT_ZEBRA_CHECKFILE WHERE CDR_DATE = '2019-08-05' AND CDR_NAME LIKE '%channelTransaction%'
     ) A
)C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZEBRA_TRANSAC B WHERE TRANSFER_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON split(B.ORIGINAL_FILE_NAME, '_')[0] = split(C.FILE_NAME, '\\.')[0]
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'ZEBRA' TABLE_SOURCE,'CHECKFILE' FLUX_TYPE,'ZEBRA_CHECKFILE' FLUX_NAME,CONCAT('DWHTrans_Stat','_',DATE_FORMAT('2019-08-05','ddMMYY'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_ZEBRA_CHECKFILE B WHERE CDR_DATE='2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,27,8) = SUBSTRING(C.FILE_NAME,27,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OTARIE' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OTARIE_TRAFFIC_CUST' FLUX_NAME,CONCAT('traffic_customer','_','2019-08-05','_','*.csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_TRAFFIC_CUST_OTARIE B WHERE TRANSACTION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05' ) B ON SUBSTRING(B.ORIGINAL_FILE_NAME,18,10) = SUBSTRING(C.FILE_NAME,18,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OTARIE' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OTARIE_TRAFFIC_CUST_CELL' FLUX_NAME,CONCAT('traffic_customer_cell','_','2019-08-05','_','*.csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_TRAFFIC_CUST_CELL_OTARIE B WHERE TRANSACTION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,23,10) = SUBSTRING(C.FILE_NAME,23,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OTARIE' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OTARIE_CUSTOMER_SP' FLUX_NAME,CONCAT('customers_sp_day','_','2019-08-05','_','*.csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_CUSTOMER_SP_OTARIE B WHERE EVENT_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,18,10) = SUBSTRING(C.FILE_NAME,18,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OM_ALL_BALANCE' FLUX_NAME,CONCAT('AllBalance','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_ALL_BALANCE B WHERE ORIGINAL_FILE_DATE ='2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OM_APGL' FLUX_NAME,CONCAT('GL_TRX_INTERFACE','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_APGL B WHERE TRANSACTION_DATE =  '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OM_COMMISSIONS' FLUX_NAME,CONCAT('CommissionsDetails','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_COMMISSIONS B WHERE TRANSACTION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OM_SERVICES_CHARGES' FLUX_NAME,CONCAT('ServicesChargesDetails','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_SERVICES_CHARGES B WHERE TRANSACTION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OM_SUBSCRIBERS' FLUX_NAME,CONCAT('Subscribers','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_SUBSCRIBERS B WHERE MODIFICATION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'OM' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'OM_TRANSACTIONS' FLUX_NAME,CONCAT('Transactions','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_OM_TRANSACTIONS B WHERE TRANSACTION_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'BDI' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'BDI' FLUX_NAME,CONCAT('bdi','_',DATE_FORMAT('2019-08-05','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_BDI B WHERE ORIGINAL_FILE_DATE ='2019-08-05') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,5,8) = SUBSTRING(C.FILE_NAME,5,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT '2019-08-05' ORIGINAL_FILE_DATE,'P2P' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'P2P_LOG' FLUX_NAME,CONCAT('p2pCommands','.','2019-08-05','.','log') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.IT_P2P_LOG B WHERE START_DATE = '2019-08-05' and TO_DATE(ORIGINAL_FILE_DATE) = '2019-08-05' ) B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-14,10) = SUBSTRING(C.FILE_NAME,13,10)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT
'2019-08-05' ORIGINAL_FILE_DATE,'MVAS' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'IT_SMSC_MVAS_A2P' FLUX_NAME,CONCAT(FILE_PREFIX,LPAD(SEQUENCE, 5, "0"),'_',MVAS_SOURCE,'.unl') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ, MVAS_SOURCE,FILE_PREFIX FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MVAS_SOURCE ORDER BY INDEX) PREVIOUS,INDEX, MVAS_SOURCE,FILE_PREFIX FROM (
            SELECT
                DISTINCT
                substr(original_file_name,0,18) FILE_PREFIX,
                CAST(SUBSTRING(original_file_name,19,5) AS INT) INDEX,
                substr(original_file_name,25,13) MVAS_SOURCE
           from CDR.IT_SMSC_MVAS_A2P
           where WRITE_DATE = '2019-08-05' AND TO_DATE(ORIGINAL_FILE_DATE)='2019-08-05'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
UNION
SELECT
'2019-08-05' ORIGINAL_FILE_DATE,'MSC' TABLE_SOURCE,'NORMAL' FLUX_TYPE,'CRA_MSC_HUAWEI' FLUX_NAME,SEQUENCE FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE
FROM (
    SELECT GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1)  SEQ FROM (
        SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
            SELECT
                DISTINCT
                CAST(SUBSTRING(SOURCE,11,9) AS INT) INDEX,
                SUBSTRING(SOURCE,5,11) MSC_TYPE
            FROM CDR.IT_CRA_MSC_HUAWEI
            WHERE CALLDATE = '2019-08-05' --AND TO_DATE(ORIGINAL_FILE_DATE)='2019-08-05'
        )A
    )D WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE
