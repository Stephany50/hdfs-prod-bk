SELECT
    REFILL_DATE AS  PERIOD,
    RECEIVER_MSISDN AS MSISDN,
    SUM(REFILL_AMOUNT) AS REF_AMT,
    SUM(1) REF_NB,
    REFILL_MEAN AS TYPE
FROM MON.SPARK_FT_REFILL
WHERE REFILL_DATE = '###SLICE_VALUE###'
    AND TERMINATION_IND='200'
    AND REFILL_MEAN = 'C2S'  
GROUP BY REFILL_DATE
    ,RECEIVER_MSISDN
    ,REFILL_MEAN