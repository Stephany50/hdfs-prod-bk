INSERT INTO CDR.SPARK_IT_ZTE_BALANCE_RESET PARTITION (BAL_RESET_DATE,FILE_DATE)
SELECT
  ACC_NBR ,
  ACCT_CODE ,
  BAL_RESET_TIME ,
  PRE_BALANCE ,
  PROVIDER_ID ,
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  CURRENT_TIMESTAMP() INSERT_DATE,
  TO_DATE(BAL_RESET_TIME) BAL_RESET_DATE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) FILE_DATE
  FROM CDR.TT_ZTE_BALANCE_RESET C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_BALANCE_RESET WHERE BAL_RESET_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
