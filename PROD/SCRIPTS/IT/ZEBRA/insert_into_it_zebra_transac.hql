INSERT INTO CDR.IT_ZEBRA_TRANSAC PARTITION (TRANSFER_DATE)
SELECT
	TRANSFER_ID	,
	REQUEST_SOURCE,
	FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATE_TIME,'dd/MM/yyyy hh:mm:ss a')) TRANSFER_DATE_TIME,
	NETWORK_CODE,
	TRANSACTION_TYPE,
	TRANSACTION_SUB_TYPE,
	TRANSACTION_CATEGORY,
	CHANNEL_TYPE,
	FROM_USER_ID,
	TO_USER_ID	,
	SENDER_MSISDN	,
	RECEIVER_MSISDN	,
	SENDER_CATEGORY	,
	RECEIVER_CATEGORY,
	SENDER_DEBIT_AMOUNT,
	RECEIVER_CREDIT_AMOUNT,
	TRANSFER_AMOUNT,
	MRP,
	PAYABLE_AMOUNT,
	NET_PAYABLE_AMOUNT,
	RECEIVER_PROCESSING_FEE	,
	RECEIVER_TAX1_AMOUNT,
	RECEIVER_TAX2_AMOUNT,
	RECEIVER_TAX3_AMOUNT,
	COMMISION,
	DIFFERENTIAL_APPLICABLE,
	DIFFERENTIAL_GIVEN,
	EXTERNAL_INVOICE_NUMBER,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(EXTERNAL_INVOICE_DATE,'dd/MM/yyyy'))) EXTERNAL_INVOICE_DATE,
	EXTERNAL_USER_ID,
	PRODUCT	,
	CREDIT_BACK_STATUS,
	TRANSFER_STATUS,
	RECEIVER_BONUS,
	RECEIVER_VALIDITY,
	RECEIVER_BONUS_VALIDITY,
	SERVICE_CLASS_CODE,
	INTERFACE_ID,
	CARD_GROUP,
	ERROR_REASON,
	SERIAL_NUMBER,
	IN_TXN,
	SEND_PRE_BAL,
	SEND_POST_BAL,
	RCVR_PRE_BAL,
	RCVR_POST_BAL,
	TXN_WALLET,
	ACTIVE_USER_ID,
	BONUS_ACCOUNT_DETAILS,
	TRANSFER_INITIATED_BY ,
	FIRST_APPROVED_BY ,
	SECOND_APPROVED_BY ,
	OTHER_COMMISION ,
	THIRD_APPROVED_BY ,
	-- BONUS_MAIN,
	-- BONUS_PROMO,
	-- BONUS_SMS,
	-- BONUS_VOICE,
	ORIGINAL_FILE_NAME,
	ORIGINAL_FILE_SIZE,
	ORIGINAL_FILE_LINE_COUNT,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
	CURRENT_TIMESTAMP() INSERT_DATE,
	TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATE,'dd/MM/yyyy')))  TRANSFER_DATE
FROM CDR.TT_ZEBRA_TRANSAC C 
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.IT_ZEBRA_TRANSAC WHERE TRANSFER_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL;
