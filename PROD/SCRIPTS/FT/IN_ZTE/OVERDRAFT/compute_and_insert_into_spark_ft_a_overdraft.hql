INSERT INTO AGG.SPARK_FT_A_OVERDRAFT
 SELECT
     SERVICE_CODE
	, OPERATION_TYPE
	, REIMBURSMENT_CHANNEL
	, OFFER_PROFILE_CODE
	, COUNT(DISTINCT SERVED_PARTY_MSISDN) MSISDN_COUNT
	, COUNT(DISTINCT TRANSACTION_ID) OPERATION_COUNT
	, SUM(NVL(LOAN_AMOUNT,0)) LOAN_AMOUNT
	, SUM(NVL(FEE,0)) FEE
	, (CASE WHEN UPPER(OPERATION_TYPE) <> 'LOAN' AND REFILL_AMOUNT >= 250 AND FEE = 0 THEN '0-3'
			WHEN UPPER(OPERATION_TYPE) <> 'LOAN' AND REFILL_AMOUNT >= 250 AND FEE = 25 THEN '4-7'
			WHEN UPPER(OPERATION_TYPE) <> 'LOAN' AND REFILL_AMOUNT >= 250 AND FEE = 50 THEN '8-'
		END) PAYMENT_DELAY
	, OPERATOR_CODE
	, CURRENT_TIMESTAMP AS INSERT_DATE
	,TO_DATE(TRANSACTION_DATE) DATECODE
	FROM MON.SPARK_FT_OVERDRAFT
	WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
	GROUP BY TO_DATE(TRANSACTION_DATE)
	, SERVICE_CODE
	, OPERATION_TYPE
	, REIMBURSMENT_CHANNEL
	, OFFER_PROFILE_CODE
	, (CASE WHEN UPPER(OPERATION_TYPE) <> 'LOAN' AND REFILL_AMOUNT >= 250 AND FEE = 0 THEN '0-3'
			WHEN UPPER(OPERATION_TYPE) <> 'LOAN' AND REFILL_AMOUNT >= 250 AND FEE = 25 THEN '4-7'
			WHEN UPPER(OPERATION_TYPE) <> 'LOAN' AND REFILL_AMOUNT >= 250 AND FEE = 50 THEN '8-'
		END)
	, OPERATOR_CODE
