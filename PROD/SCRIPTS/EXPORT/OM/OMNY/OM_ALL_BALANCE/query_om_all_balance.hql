SELECT
	ACCOUNT_ID,
	ACCOUNT_TYPE,
	ACCOUNT_NAME,
	BALANCE,
	USER_NAME,
	LAST_NAME,
	USER_DOMAIN,
	USER_CATEGORY,
	WALLET_NUMBER,
	FROZEN_AMOUNT,
	PAYMENT_TYPE_ID,
	date_format(registered_on, 'yyyy-MM-dd HH:mm:ss') OM_ACTIVATION_DATE,
	ORIGINAL_FILE_DATE
FROM 
	(
		SELECT
		DISTINCT ACCOUNT_ID,
		ACCOUNT_TYPE,
		ACCOUNT_NAME,
		BALANCE,
		USER_NAME,
		LAST_NAME,
		USER_DOMAIN,
		USER_CATEGORY,
		WALLET_NUMBER,
		FROZEN_AMOUNT,
		PAYMENT_TYPE_ID,
		ORIGINAL_FILE_DATE
		FROM CDR.SPARK_IT_OM_ALL_BALANCE where ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
	) A
	LEFT JOIN
	(
		SELECT msisdn, registered_on 
		FROM MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT_NEW where event_date = '###SLICE_VALUE###'
	) B
	ON fn_format_msisdn_to_9digits(A.ACCOUNT_ID) = fn_format_msisdn_to_9digits(B.msisdn)
