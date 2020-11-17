SELECT 
    regexp_replace(from_unixtime(unix_timestamp(TRANSACTION_DATE ,'yyyy-MM-dd'), 'dd-MM-yyyy'),'-','/')  AS `date`,
    SERVED_PARTY_MSISDN AS msisdn,
    OPERATION_TYPE AS operation_type,
    REIMBURSMENT_CHANNEL AS reimbursment_channel,
    SUM(LOAN_AMOUNT) AS loan_amount,
    SUM(REFILL_AMOUNT) AS refill_amount,
    SUM(FEE) AS fee,
    COUNT(*) AS transaction_count
FROM MON.SPARK_FT_OVERDRAFT
WHERE TRANSACTION_DATE='###SLICE_VALUE###'
GROUP BY regexp_replace(from_unixtime(unix_timestamp(TRANSACTION_DATE ,'yyyy-MM-dd'), 'dd-MM-yyyy'),'-','/')
    ,OPERATION_TYPE
    ,REIMBURSMENT_CHANNEL
    ,SERVED_PARTY_MSISDN