INSERT INTO MON.FT_DATA_TRANSFER PARTITION(TRANSACTION_DATE)
SELECT 
    A.TRANSACTION_TIME,
    A.TRANSACTION_ID,
    A.SENDER_MSISDN,
    A.RECEIVER_MSISDN,
    A.AMOUNT_CHARGED,
    A.BYTES_DEBITED,
    A.BYTES_CREDITED,
    A.DEBIT_ACT_RES_CODE,
    A.CREDIT_ACT_RES_CODE,
    A.SENDER_OPERATOR_CODE,
    A.RECEIVER_OPERATOR_CODE,
    NVL(B.PROFILE, B.OSP_CUSTOMER_FORMULE) SENDER_OFFER_PROFILE_CODE,
    NVL(C.PROFILE, C.OSP_CUSTOMER_FORMULE) RECEIVER_OFFER_PROFILE_CODE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    A.TRANSACTION_DATE
FROM
(
    SELECT
        MAX(CREATE_DATE) TRANSACTION_DATE,
        MAX(DATE_FORMAT(NQ_CREATE_DATE, 'HH:mm:ss')) TRANSACTION_TIME,
        SUBSTR(TRANSACTIONSN, 0, 19) TRANSACTION_ID,
        GET_NNP_MSISDN(MAX(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) IN ('SD', 'BD') THEN ACC_NBR ELSE NULL END )) SENDER_MSISDN,
        GET_NNP_MSISDN(MAX(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) = 'R' THEN ACC_NBR ELSE NULL END )) RECEIVER_MSISDN,
        SUM(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) IN ('SF', 'BF') AND ACCT_RES_CODE = 1 THEN A.CHARGE/100 ELSE 0 END) AMOUNT_CHARGED,
        SUM(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) IN ('SD', 'BD') THEN A.CHARGE ELSE 0 END ) BYTES_DEBITED,
        SUM(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) = 'R' THEN A.CHARGE ELSE 0 END ) BYTES_CREDITED,
        MAX(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) IN ('SD', 'BD') THEN ACCT_RES_CODE ELSE NULL END) DEBIT_ACT_RES_CODE,
        MAX(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) = 'R' THEN ACCT_RES_CODE ELSE NULL END) CREDIT_ACT_RES_CODE,
        GET_OPERATOR_CODE(MAX(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) IN ('SD', 'BD') THEN ACC_NBR ELSE NULL END )) SENDER_OPERATOR_CODE,
        GET_OPERATOR_CODE(MAX(CASE WHEN TRIM(SUBSTR(TRANSACTIONSN, 20)) = 'R' THEN ACC_NBR ELSE NULL END )) RECEIVER_OPERATOR_CODE
    FROM CDR.SPARK_IT_ZTE_ADJUSTMENT A
    WHERE CREATE_DATE = '###SLICE_VALUE###'
    AND CHANNEL_ID=11
    GROUP BY SUBSTR(TRANSACTIONSN, 0, 19)
) A
LEFT JOIN (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED') B ON REGEXP_REPLACE(A.SENDER_MSISDN, "^237+(?!$)", "") = B.ACCESS_KEY
LEFT JOIN (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###' AND OSP_STATUS <> 'TERMINATED') C ON REGEXP_REPLACE(A.RECEIVER_MSISDN, "^237+(?!$)", "") = C.ACCESS_KEY;

