INSERT INTO CDR.IT_ZTE_ADJUSTMENT PARTITION (CREATE_DATE)
SELECT
  ACCT_CODE,
  ACC_NBR,
  ACCT_BOOK_ID,
  ACCT_RES_CODE,
  PRE_REAL_BALANCE,
  CHARGE,
  PRE_EXP_DATE,
  DAYS,
  CHANNEL_ID,
  CREATE_DATE NQ_CREATE_DATE,
  TRANSACTIONSN,
  PROVIDER_ID,
  PREPAY_FLAG ,
  LOAN_AMOUNT,
  COMMISSION_AMOUNT,
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  CURRENT_TIMESTAMP() INSERT_DATE,
  TO_DATE(CREATE_DATE) CREATE_DATE
FROM CDR.TT_ZTE_ADJUSTMENT C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_ZTE_ADJUSTMENT WHERE CREATE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL

