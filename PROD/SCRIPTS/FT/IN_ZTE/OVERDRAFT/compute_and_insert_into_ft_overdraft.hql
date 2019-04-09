INSERT INTO MON.FT_OVERDRAFT PARTITION(TRANSACTION_DATE)
SELECT DISTINCT
CAST(A.PAYMENT_ID AS STRING) TRANSACTION_ID
, A.PAY_TIME TRANSACTION_TIME
, 'AIRTIME' SERVICE_CODE
, 'LOAN' OPERATION_TYPE
, NULL REIMBURSMENT_CHANNEL
, IF(REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")) SERVED_PARTY_MSISDN
, CASE
WHEN A.PREPAY_FLAG = '1' THEN 'PURE PREPAID'
WHEN A.PREPAY_FLAG = '2' THEN 'POSTPAID'
WHEN A.PREPAY_FLAG = '3' THEN 'HYBRID'
ELSE 'UNKNOW'
END CONTRACT_TYPE
, NVL(B.PROFILE, B.OSP_CUSTOMER_FORMULE) OFFER_PROFILE_CODE
, 0 REFILL_AMOUNT
, -(A.BILL_AMOUNT)/100 LOAN_AMOUNT
, 0 FEE
, 'NO' FEE_FLAG
, 'IT_ZTE_RECHARGE' SOURCE_TABLE
, FN_GET_OPERATOR_CODE(A.ACC_NBR) OPERATOR_CODE
, CURRENT_TIMESTAMP() INSERT_DATE
,TO_DATE (A.PAY_TIME) TRANSACTION_DATE
FROM CDR.IT_ZTE_RECHARGE A
LEFT JOIN (SELECT * FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED') B
WHERE A.PAY_DATE = '###SLICE_VALUE###'
AND A.ACCT_RES_CODE = 20
AND IF(REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")) = B.ACCESS_KEY
UNION
SELECT DISTINCT
CAST(A.PAYMENT_ID AS STRING) TRANSACTION_ID
, A.PAY_TIME TRANSACTION_TIME
, 'AIRTIME' SERVICE_CODE
, 'REIMBURSMENT' OPERATION_TYPE
, (CASE WHEN A.PAYMENT_METHOD = 4 THEN 'SCRATCH'
WHEN A.CHANNEL_ID = 11 THEN 'C2S'
WHEN A.CHANNEL_ID = 17 THEN 'BO DEPOSIT'
ELSE CAST(A.CHANNEL_ID AS STRING)
END) REIMBURSMENT_CHANNEL
, IF(REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")) SERVED_PARTY_MSISDN
, CASE
WHEN A.PREPAY_FLAG = '1' THEN 'PURE PREPAID'
WHEN A.PREPAY_FLAG = '2' THEN 'POSTPAID'
WHEN A.PREPAY_FLAG = '3' THEN 'HYBRID'
ELSE 'UNKNOW'
END CONTRACT_TYPE
, NVL(B.PROFILE, B.OSP_CUSTOMER_FORMULE) OFFER_PROFILE_CODE
, -(A.BILL_AMOUNT)/100 REFILL_AMOUNT
, (A.LOAN_AMOUNT)/100 LOAN_AMOUNT
, (A.COMMISSION_AMOUNT)/100 FEE
,CASE
WHEN NVL(A.COMMISSION_AMOUNT, 0) = 0 THEN 'NO'
ELSE 'YES'
END FEE_FLAG
, 'IT_ZTE_RECHARGE' SOURCE_TABLE
, FN_GET_OPERATOR_CODE(A.ACC_NBR) OPERATOR_CODE
, CURRENT_TIMESTAMP() INSERT_DATE
,TO_DATE (A.PAY_TIME) TRANSACTION_DATE
FROM CDR.IT_ZTE_RECHARGE A
LEFT JOIN (SELECT * FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED') B
WHERE A.PAY_DATE = '###SLICE_VALUE###'
AND A.ACCT_RES_CODE = 1
AND (A.LOAN_AMOUNT > 0 OR A.COMMISSION_AMOUNT >0)
AND IF(REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")) = B.ACCESS_KEY
UNION
SELECT DISTINCT
CAST(A.PAYMENT_ID_IN AS STRING) TRANSACTION_ID
, A.PAY_TIME TRANSACTION_TIME
, 'AIRTIME' SERVICE_CODE
, 'REIMBURSMENT' OPERATION_TYPE
, 'P2P' REIMBURSMENT_CHANNEL
, IF(REGEXP_REPLACE(A.ACC_NBR_IN, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR_IN, "^237+(?!$)","")) SERVED_PARTY_MSISDN
, CASE
WHEN A.PREPAY_FLAG = '1' THEN 'PURE PREPAID'
WHEN A.PREPAY_FLAG = '2' THEN 'POSTPAID'
WHEN A.PREPAY_FLAG = '3' THEN 'HYBRID'
ELSE 'UNKNOW'
END CONTRACT_TYPE
, NVL(B.PROFILE, B.OSP_CUSTOMER_FORMULE) OFFER_PROFILE_CODE
, (A.CHARGE)/100 REFILL_AMOUNT
, (A.LOAN_AMOUNT)/100 LOAN_AMOUNT
, (A.COMMISSION_AMOUNT)/100 FEE
,CASE
WHEN NVL(A.COMMISSION_AMOUNT, 0) = 0 THEN 'NO'
ELSE 'YES'
END FEE_FLAG
, 'IT_ZTE_TRANSFER' SOURCE_TABLE
, FN_GET_OPERATOR_CODE(A.ACC_NBR_IN) OPERATOR_CODE
, CURRENT_TIMESTAMP() INSERT_DATE
,TO_DATE (A.PAY_TIME) TRANSACTION_DATE
FROM CDR.IT_ZTE_TRANSFER A
LEFT JOIN (SELECT * FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED') B
WHERE A.PAY_DATE = '###SLICE_VALUE###'
AND A.ACCT_RES_CODE = 1
AND (A.LOAN_AMOUNT > 0 OR A.COMMISSION_AMOUNT >0)
AND IF(REGEXP_REPLACE(A.ACC_NBR_IN, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR_IN, "^237+(?!$)","")) = B.ACCESS_KEY
UNION
SELECT DISTINCT
CAST(A.TRANSACTIONSN AS STRING) TRANSACTION_ID
, A.NQ_CREATE_DATE TRANSFER_DATE_TIME
, 'AIRTIME' SERVICE_CODE
, 'REIMBURSMENT' OPERATION_TYPE
, 'ADJUSTMENT' REIMBURSMENT_CHANNEL
, IF(REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")) SERVED_PARTY_MSISDN
, (CASE
WHEN A.PREPAY_FLAG = '1' THEN 'PURE PREPAID'
WHEN A.PREPAY_FLAG = '2' THEN 'POSTPAID'
WHEN A.PREPAY_FLAG = '3' THEN 'HYBRID'
ELSE 'UNKNOW'
END) CONTRACT_TYPE
, NVL(B.PROFILE, B.OSP_CUSTOMER_FORMULE) OFFER_PROFILE_CODE
, (A.CHARGE)/100 REFILL_AMOUNT
, (A.LOAN_AMOUNT)/100 LOAN_AMOUNT
, (A.COMMISSION_AMOUNT)/100 FEE
, CASE
WHEN NVL(A.COMMISSION_AMOUNT, 0) = 0 THEN 'NO'
ELSE 'YES'
END FEE_FLAG
, 'IT_ZTE_ADJUSTMENT' SOURCE_TABLE
,FN_GET_OPERATOR_CODE(A.ACC_NBR) OPERATOR_CODE
, CURRENT_TIMESTAMP() INSERT_DATE
, TO_DATE (A.CREATE_DATE) TRANSACTION_DATE
FROM CDR.IT_ZTE_ADJUSTMENT A
LEFT JOIN (SELECT * FROM MON.FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED') B
WHERE CREATE_DATE = '###SLICE_VALUE###'
AND A.ACCT_RES_CODE = 1
AND (A.LOAN_AMOUNT > 0 OR A.COMMISSION_AMOUNT >0)
AND IF(REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(A.ACC_NBR, "^237+(?!$)","")) = B.ACCESS_KEY
;