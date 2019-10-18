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
--LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_ZTE_RECHARGE WHERE PAY_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
--WHERE T.FILE_NAME IS NULL
