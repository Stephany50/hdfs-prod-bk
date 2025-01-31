SELECT
    REFILL_DATE AS period,
    RECEIVER_MSISDN AS msisdn,
    SUM(REFILL_AMOUNT) AS ref_amt,
    SUM(1) ref_nb,
    REFILL_MEAN AS type,
    sender_msisdn,
    sender_category,
    refill_type 
FROM MON.SPARK_FT_REFILL
WHERE REFILL_DATE = '###SLICE_VALUE###'
    AND TERMINATION_IND='200'
    AND REFILL_MEAN = 'C2S'  
GROUP BY REFILL_DATE
    ,RECEIVER_MSISDN
    ,REFILL_MEAN
    , sender_msisdn
    , sender_category
    , refill_type 