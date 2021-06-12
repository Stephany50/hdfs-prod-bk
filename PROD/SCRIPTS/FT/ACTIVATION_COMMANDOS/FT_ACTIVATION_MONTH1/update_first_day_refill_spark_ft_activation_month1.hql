INSERT OVERWRITE TABLE MON.SPARK_FT_ACTIVATION_MONTH1
select 
      A.MSISDN MSISDN,
      A.FIRST_DAY_REFILL,
      A.REFILL_COUNT,
      A.REFILL_AMOUNT,
      (
            CASE
                  WHEN A.ACTIVATION_DATE = date_sub(concat(substr('###SLICE_VALUE###', 1, 7), '-01'), 1) THEN B.REFILL_AMOUNT
                  ELSE A.NEXT_MONTH_FIRST_DAY_REFIL_AMT
            END
      ) NEXT_MONTH_FIRST_DAY_REFIL_AMT,
      CURRENT_TIMESTAMP REFRESH_DATE,
      A.FIRST_DAY_REFILL_AMOUNT,
      CURRENT_TIMESTAMP INSERT_DATE,
      substr(add_months('###SLICE_VALUE###', -1), 1, 7) EVENT_MONTH,
      A.ACTIVATION_DATE
from  
(
      SELECT 
            MSISDN, 
            FIRST_DAY_REFILL, 
            REFILL_COUNT, 
            REFILL_AMOUNT, 
            NEXT_MONTH_FIRST_DAY_REFIL_AMT, 
            REFRESH_DATE, 
            FIRST_DAY_REFILL_AMOUNT, 
            INSERT_DATE, 
            EVENT_MONTH, 
            ACTIVATION_DATE
      FROM MON.SPARK_TMP_ACTIVATION_MONTH
      WHERE EVENT_MONTH = substr(add_months('###SLICE_VALUE###', -1), 1, 7)
)  A 
left join
(
      SELECT 
            RECEIVER_MSISDN AS MSISDN, 
            COUNT(*) AS REFILL_COUNT, 
            SUM(REFILL_AMOUNT) AS REFILL_AMOUNT
      FROM MON.SPARK_FT_REFILL
      WHERE REFILL_DATE = '###SLICE_VALUE###'
            AND REFILL_MEAN IN ('SCRATCH', 'C2S')
            AND TERMINATION_IND = '200'
      GROUP BY RECEIVER_MSISDN
) B
ON A.MSISDN = B.MSISDN

