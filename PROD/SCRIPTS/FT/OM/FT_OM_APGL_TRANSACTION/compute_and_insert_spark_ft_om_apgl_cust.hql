insert into MON.SPARK_FT_OM_APGL_CUST PARTITION(TRANSACTION_DATE, FILE_DATE)
select 
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
	TRANSACTION_DATE_TIME,
	SENDER_CATEGORY_CODE,
	RECEIVER_CATEGORY_CODE,
	SENDER_DOMAIN_CODE,
	RECEIVER_DOMAIN_CODE,
	SENDER_DESIGNATION,
	RECEIVER_DESIGNATION,
	SENDER_STATE,
	FIN RECEIVER_STATE,
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
    FOREIGN_CURRENCY_CODE,
	COMMERCIAL_EXCHANGE_RATE,
	REFERENCE_EXCHANGE_RATE,
	TRANSACTION_DATE,
	FILE_DATE
from 
(select 
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
	TRANSACTION_DATE_TIME,
	SENDER_CATEGORY_CODE,
	RECEIVER_CATEGORY_CODE,
	SENDER_DOMAIN_CODE,
	RECEIVER_DOMAIN_CODE,
	SENDER_DESIGNATION,
	RECEIVER_DESIGNATION,
	FIN SENDER_STATE,
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
    FOREIGN_CURRENCY_CODE,
	COMMERCIAL_EXCHANGE_RATE,
	REFERENCE_EXCHANGE_RATE,
	TRANSACTION_DATE,
	FILE_DATE
from 
	(select * from CDR.SPARK_IT_OM_APGL where file_date='###SLICE_VALUE###') A
	left join 
	(select msisdn, FIN from dim.ref_apgl) B
	on fn_format_msisdn_to_9digits(trim(A.SENDER_MSISDN)) = trim(B.msisdn)
) T
left join 
(select msisdn, FIN from dim.ref_apgl) C
on fn_format_msisdn_to_9digits(trim(T.RECEIVER_MSISDN)) = trim(C.msisdn)