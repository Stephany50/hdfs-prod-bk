INSERT INTO CDR.IT_TRAFFIC_CUST_CELL_OTARIE PARTITION(TRANSACTION_DATE)
SELECT
    IDCUST
    ,IDGGSN
    ,IDRAT
    ,IDTAC
    ,IDCELL
    ,STATE
    ,NBYTESDN
    ,NBYTESUP
    ,ORIGINAL_FILE_NAME
    ,SUBSTRING(ORIGINAL_FILE_NAME,23,10) ORIGINAL_FILE_DATE
    ,ORIGINAL_FILE_SIZE
    ,ORIGINAL_FILE_LINE_COUNT
    ,CURRENT_TIMESTAMP
    ,TRANSACTION_DATE
FROM CDR.TT_TRAFFIC_CUST_CELL_OTARIE C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_TRAFFIC_CUST_CELL_OTARIE WHERE TRANSACTION_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;