INSERT INTO CDR.IT_TRAFFIC_CUST_OTARIE
SELECT
    IDCUST
    ,IDAPPLI
    ,IDAPN
    ,IDTAC
    ,IDRAT
    ,IDGGSN
    ,ROAMING
    ,NBYTESDN
    ,NBYTESUP
    ,NBYTEST
    ,TERMINAL_TYPE
    ,TERMINAL_BRAND
    ,TERMINAL_MODEL
    ,TOFIND_1
    ,TOFIND_2
    ,TOFIND_3
    ,TOFIND_4
    ,TOFIND_5
    ,ORIGINAL_FILE_NAME
    ,TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
    ,ORIGINAL_FILE_SIZE
    ,ORIGINAL_FILE_LINE_COUNT
    ,CURRENT_TIMESTAMP
    ,TRANSACTION_DATE
FROM CDR.TT_TRAFFIC_CUST_OTARIE C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM')
    AND B.FILE_TYPE = 'OTARIE_TRAFFIC_CUST' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
)