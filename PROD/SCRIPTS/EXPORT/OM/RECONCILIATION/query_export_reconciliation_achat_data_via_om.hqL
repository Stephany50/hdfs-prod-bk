SELECT
    A.*,
    price_plan_name,
    price_plan_code,
    B.*,
    D.*
 FROM
(
    SELECT
        MSISDN_PAYER,
        MSISDN_PAYEE,
        IDTXN,
        IPP_CODE,
        BUNDLE_NAME,
        bundle_price,
        TIMESTAMP_TXN
    FROM CDR.SPARK_IT_RECONCILIATION_ACHAT_DATA_VIA_OM
    WHERE TRANSACTION_DATE='###SLICE_VALUE###'
) A 
LEFT JOIN
(
    SELECT 
        SERVED_PARTY_MSISDN MSISDN,
        SUBSCRIPTION_SERVICE_DETAILS,
        TRANSACTION_DATE,
        TRANSACTION_TIME,
        SUBSCRIPTION_CHANNEL,
        rated_amount,
        benefit_balance_list
    FROM MON.SPARK_FT_SUBSCRIPTION
    WHERE TRANSACTION_DATE='###SLICE_VALUE###'
        AND SUBSCRIPTION_CHANNEL IN (111, 32) 
) B
ON A.MSISDN_PAYEE = B.MSISDN AND 
ABS(cast(cast(from_unixtime(unix_timestamp(CONCAT_WS(' ', B.TRANSACTION_DATE, B.TRANSACTION_TIME),'yyyy-MM-dd HHmmss'),'yyyy-MM-dd HH:mm:ss') as timestamp) as double) - cast(cast(timestamp_txn as timestamp) as double)) <= 90 
LEFT JOIN
(
    SELECT 
        price_plan_name,
        price_plan_code,
        original_file_date
    FROM
    CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT
    WHERE original_file_date='###SLICE_VALUE###'
    GROUP BY price_plan_name,
             price_plan_code,
             original_file_date
)  C
ON A.IPP_CODE=C.price_plan_code 
AND UPPER(B.SUBSCRIPTION_SERVICE_DETAILS)=UPPER(C.price_plan_name) 
AND B.TRANSACTION_DATE=C.original_file_date
LEFT JOIN (
    SELECT 
        transfer_id,
        transfer_status,
        transfer_datetime
    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
    WHERE transfer_datetime='###SLICE_VALUE###'
    GROUP BY transfer_id,
        transfer_status,
        transfer_datetime 
) D
ON A.IDTXN = D.transfer_id AND to_date(A.timestamp_txn)=D.transfer_datetime