SELECT
    TRANSACTION_ID TRANSACTIONID,
    TRANSACTION_DATE TRANSACTION_DATE,
    COMMISSION_ID COMMISSION_ID,
    TRANSACTION_AMOUNT TRANSACTION_AMOUNT,
    PAYER_USER_ID PAYER_USER_ID,
    PAYER_CATEGORY_CODE PAYER_CATEGORY_CODE,
    PAYEE_USER_ID PAYEE_USER_ID,
    PAYEE_CATEGORY_CODE PAYEE_CATEGORY_CODE,
    COMMISSION_AMOUNT COMMISSION_AMOUNT 
FROM CDR.SPARK_IT_OMNY_COMMISSION_DETAILS 
WHERE TRANSACTION_DATE = "###SLICE_VALUE###"