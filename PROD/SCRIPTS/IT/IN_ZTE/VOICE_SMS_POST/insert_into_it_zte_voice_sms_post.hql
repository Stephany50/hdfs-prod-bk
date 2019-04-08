INSERT INTO CDR.IT_ZTE_VOICE_SMS_POST PARTITION (START_DATE)
SELECT
    EVENT_INST_ID,
    RE_ID,
    BILLING_NBR,
    BILLING_IMSI,
    CALLING_NBR,
    CALLED_NBR,
    THIRD_PART_NBR,
    START_TIME,
    DURATION,
    LAC_A,
    CELL_A,
    LAC_B,
    CELL_B,
    CALLING_IMEI,
    CALLED_IMEI,
    PRICE_ID1,
    PRICE_ID2,
    PRICE_ID3,
    PRICE_ID4,
    PRICE_PLAN_ID1,
    PRICE_PLAN_ID2,
    PRICE_PLAN_ID3,
    PRICE_PLAN_ID4,
    ACCT_RES_ID1,
    ACCT_RES_ID2,
    ACCT_RES_ID3,
    ACCT_RES_ID4,
    CHARGE1,
    CHARGE2,
    CHARGE3,
    CHARGE4,
    BAL_ID1,
    BAL_ID2,
    BAL_ID3,
    BAL_ID4,
    ACCT_ITEM_TYPE_ID1,
    ACCT_ITEM_TYPE_ID2,
    ACCT_ITEM_TYPE_ID3,
    ACCT_ITEM_TYPE_ID4,
    PREPAY_FLAG,
    PRE_BALANCE1,
    BALANCE1,
    PRE_BALANCE2,
    BALANCE2,
    PRE_BALANCE3,
    BALANCE3,
    PRE_BALANCE4,
    BALANCE4,
    INTERNATIONAL_ROAMING_FLAG,
    CALL_TYPE,
    BYTE_UP,
    BYTE_DOWN,
    BYTES,
    PRICE_PLAN_CODE,
    SESSION_ID,
    RESULT_CODE,
    PROD_SPEC_STD_CODE,
    YZDISCOUNT,
    BYZCHARGE1,
    BYZCHARGE2,
    BYZCHARGE3,
    BYZCHARGE4,
    ONNET_OFFNET,
    PROVIDER_ID,
    PROD_SPEC_ID,
    TERMINATION_CAUSE,
    B_PROD_SPEC_ID,
    B_PRICE_PLAN_CODE,
    CALLSPETYPE,
    CHARGINGRATIO,
    ORIGINAL_FILE_NAME,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -25, 14),'yyyyMMddHHmmss'))) ORIGINAL_FILE_DATE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(START_TIME) START_DATE
FROM CDR.TT_ZTE_VOICE_SMS_POST  C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM')
    AND B.FILE_TYPE = '${hivevar:cdr_type}' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
);

