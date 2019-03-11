
INSERT INTO CDR.IT_ZTE_RECHARGE PARTITION (PAY_DATE)
SELECT
    PAYMENT_ID,
    ACCT_CODE,
    ACC_NBR,
    PAY_TIME,
    ACCT_RES_CODE,
    BILL_AMOUNT,
    RESULT_BALANCE,
    CHANNEL_ID,
    PAYMENT_METHOD,
    DAYS,
    OLD_EXP_DATE,
    NEW_EXP_DATE,
    BENEFIT_NAME,
    BENEFIT_BAL_LIST,
    BENEFIT_PRICEPLAN,
    TRANSACTIONSN,
    PROVIDER_ID,
    PREPAY_FLAG,
    LOAN_AMOUNT,
    COMMISSION_AMOUNT,
    ORIGINAL_FILE_NAME,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd') ) ) ORIGINAL_FILE_DATE,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(PAY_TIME) PAY_DATE
FROM CDR.TT_ZTE_RECHARGE C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM')
AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
;

INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT
  ORIGINAL_FILE_NAME,
  'ZTE_RECHARGE_CDR' FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_ZTE_RECHARGE C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM')
                   AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.FILE_TYPE = 'ZTE_RECHARGE_CDR' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
GROUP BY ORIGINAL_FILE_NAME
;


