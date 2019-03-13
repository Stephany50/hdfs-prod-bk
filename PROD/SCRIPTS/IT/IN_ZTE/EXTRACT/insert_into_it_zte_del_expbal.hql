INSERT INTO CDR.IT_ZTE_DEL_EXPBAL PARTITION (ORIGINAL_FILE_DATE)
SELECT
    ACC_NBR ,
    SUBS_ID ,
    ACCT_ID ,
    BAL_ID ,
    ACCT_RES_ID ,
    ACCT_RES_NAME ,
    ACCT_BOOK_TYPE_NAME ,
    CREATION_DATE ,
    PARTY_TYPE  ,
    PARTY_TYPE_NAME , 
    BALANCE_BEFORE ,
    ADJUST_AMOUNT ,
    BALANCE_AFTER ,
    ADJUST_DAYS ,
    OLD_EXPIRY_DATE ,
    ORIGINAL_FILE_NAME ,
    ORIGINAL_FILE_SIZE ,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZTE_DEL_EXPBAL C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM') 
    AND B.FILE_TYPE = 'ZTE_DEL_EXPBAL' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
);

--***************************
--- Log Filed received
-------------------------

INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT 
  ORIGINAL_FILE_NAME,
  'ZTE_DEL_EXPBAL' FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_ZTE_DEL_EXPBAL C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM') 
                   AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.FILE_TYPE = 'ZTE_DEL_EXPBAL' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
GROUP BY ORIGINAL_FILE_NAME;
