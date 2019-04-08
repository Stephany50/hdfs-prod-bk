INSERT INTO CDR.IT_ZTE_SUBSCRIPTION PARTITION (CREATEDDATE)
SELECT
  ACC_NBR ,
  CHANNEL_ID,
  SUBS_EVENT_ID	,
  CREATEDDATE NQ_CREATEDDATE,
  OLD_SUBS_STATE,
  NEW_SUBS_STATE,
  EVENT_COST,
  BENEFIT_NAME,
  BENEFIT_BAL_LIST,
  OLD_PROD_SPEC_CODE,
  PROD_SPEC_CODE,
  OLD_PRICE_PLAN_CODE,
  PRICE_PLAN_CODE,
  OLD_RELATED_PROD_CODE,
  RELATED_PROD_CODE,
  ACTIVE_DATE,
  EXPIRED_DATE,
  PROVIDER_ID,
  PREPAY_FLAG,
  PAYMENT_NUMBER,
  SUBSCRIPTION_COST,
  TRANSACTIONSN,
  ORIGINAL_FILE_NAME,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -19, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  CURRENT_TIMESTAMP() INSERT_DATE,
  TO_DATE(CREATEDDATE) CREATEDDATE
FROM CDR.TT_ZTE_SUBSCRIPTION C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM') 
                   AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME);

