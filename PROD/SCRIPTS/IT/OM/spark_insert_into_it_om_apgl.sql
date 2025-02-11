it INSERT INTO CDR.IT_OM_APGL PARTITION (TRANSACTION_DATE)
SELECT
	SENDER_MSISDN,
	RECEIVER_MSISDN,
	SENDER_USER_ID,
	RECEIVER_USER_ID,
	SERVICE_TYPE,
	TRANSFER_SUBTYPE,
	SENDER_COUNTRY_CODE,
	RECEIVER_COUNTRY_CODE,
	TRANSACTION_STATUS,
	SENDER_PRE_BALANCE,
	SENDER_POST_BALANCE,
	RECEIVER_PRE_BALANCE,
	RECEIVER_POST_BALANCE,
	REFERENCE_NUMBER,
	REMARKS,
	TRANSACTION_ID,
	FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSACTION_DATE_TIME,'dd/MM/yyyy HH:mm:ss')) TRANSACTION_DATE_TIME,
	SENDER_CATEGORY_CODE,
	RECEIVER_CATEGORY_CODE,
	SENDER_DOMAIN_CODE,
	RECEIVER_DOMAIN_CODE,
	SENDER_DESIGNATION,
	RECEIVER_DESIGNATION,
	SENDER_STATE,
	RECEIVER_STATE,
	TRANSACTION_AMOUNT,
	COMMISSION_GROSSISTE,
	COMMISSION_AGENT,
	COMMISSION_OCA,
	COMMISSION_AUTRE,
	SERVICE_CHARGE_AMOUNT,
	TRANSACTION_TAG,
	IS_FINANCIAL,
	ZEBRA,
	ROLLBACKED,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSACTION_DATE_TIME,'dd/MM/yyyy HH:mm:ss'))) TRANSACTION_DATE
FROM CDR.TT_OM_APGL C
WHERE NOT EXISTS (
    SELECT 1 FROM RECEIVED_FILES B
    WHERE ORIGINAL_FILE_MONTH  BETWEEN DATE_FORMAT(DATE_SUB(CURRENT_DATE, 90), 'yyyy-MM') AND DATE_FORMAT(CURRENT_DATE , 'yyyy-MM')
    AND B.FILE_TYPE = 'OM_APGL' AND B.ORIGINAL_FILE_NAME = C.ORIGINAL_FILE_NAME
)
