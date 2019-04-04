INSERT INTO CDR.IT_OM_COMMISSIONS PARTITION (TRANSACTION_DATE)
SELECT
	TRANSACTION_ID,
	FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSACTION_DATE,'dd/MM/yyyy HH:mm:ss')) NQ_TRANSACTION_DATE,
	COMMISSION_ID,
	TRANSACTION_AMOUNT,
	PAYER_USER_ID,
	PAYER_CATEGORY_CODE,
	PAYEE_USER_ID,
	PAYEE_CATEGORY_CODE,
	COMMISSION_AMOUNT,
	SERVICE_TYPE,
	TRANSFER_STATUS,
	TRANSFER_SUBTYPE,
	PAYER_DOMAIN_CODE,
	PAYER_GRADE_NAME,
	PAYER_MOBILE_GROUP_ROLE,
	PAYER_GROUP_ROLE,
	PAYER_MSISDN_ACC,
	PARENT_PAYER_USER_ID,
	PARENT_PAYER_USER_MSISDN,
	OWNER_PAYER_USER_ID,
	OWNER_PAYER_USER_MSISDN,
	PAYER_WALLET_NUMBER,
	PAYEE_DOMAIN_CODE,
	PAYEE_GRADE_NAME,
	PAYEE_MOBILE_GROUP_ROLE,
	PAYEE_GROUP_ROLE,
	PAYEE_MSISDN_ACC,
	PARENT_PAYEE_USER_ID,
	PARENT_PAYEE_USER_MSISDN,
	OWNER_PAYEE_USER_ID,
	OWNER_PAYEE_USER_MSISDN,
	PAYEE_WALLET_NUMBER,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSACTION_DATE,'dd/MM/yyyy HH:mm:ss'))) TRANSACTION_DATE
FROM CDR.TT_OM_COMMISSIONS C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE,${hivevar:date_offset}), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM') 
    AND B.FILE_TYPE = 'OM_COMMISSIONS' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
);

INSERT INTO RECEIVED_FILES PARTITION(ORIGINAL_FILE_MONTH)
SELECT 
  ORIGINAL_FILE_NAME,
  'OM_COMMISSIONS' FILE_TYPE,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
  MAX(ORIGINAL_FILE_SIZE),
  MAX(ORIGINAL_FILE_LINE_COUNT),
  CURRENT_TIMESTAMP,
  DATE_FORMAT(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))), 'yyyy-MM') ORIGINAL_FILE_MONTH
FROM CDR.TT_OM_COMMISSIONS C
WHERE NOT EXISTS (SELECT 1 FROM RECEIVED_FILES B WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(current_date,${hivevar:date_offset}), 'yyyy-MM') 
                   AND DATE_FORMAT(current_date, 'yyyy-MM') AND B.FILE_TYPE = 'OM_COMMISSIONS' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME)
GROUP BY ORIGINAL_FILE_NAME;
