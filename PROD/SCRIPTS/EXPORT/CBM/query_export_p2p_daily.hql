SELECT 
    DATE_FORMAT(REFILL_DATE,'dd/MM/yyyy') period
    ,sender_msisdn sender
    ,receiver_msisdn receiver
    ,SUM(TRANSFER_AMT) ref_amt
    ,SUM(1) ref_nb
    ,SUM(TRANSFER_FEES) fees
FROM MON.SPARK_FT_CREDIT_TRANSFER
WHERE REFILL_DATE = '###SLICE_VALUE###'
    AND TERMINATION_IND='000'
GROUP BY REFILL_DATE
    ,sender_msisdn
    ,receiver_msisdn
