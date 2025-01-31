INSERT OVERWRITE TABLE MON.SPARK_FT_ACTIVATION_MONTH1
select 
      A.MSISDN MSISDN,
      (
            CASE 
                  WHEN A.FIRST_DAY_REFILL IS NOT NULL THEN LEAST(A.FIRST_DAY_REFILL, to_date('###SLICE_VALUE###'))
                  ELSE '###SLICE_VALUE###'
            END 
      )  FIRST_DAY_REFILL,
      NVL((nvl(A.REFILL_COUNT, 0) + nvl(B.REFILL_COUNT, 0)), 0) REFILL_COUNT,
      NVL((nvl(A.REFILL_AMOUNT, 0) + nvl(B.REFILL_AMOUNT, 0)), 0) REFILL_AMOUNT,
      A.NEXT_MONTH_FIRST_DAY_REFIL_AMT,
      CURRENT_TIMESTAMP REFRESH_DATE,
      (
            CASE 
                  WHEN (A.FIRST_DAY_REFILL IS NULL) OR (A.FIRST_DAY_REFILL IS NOT NULL AND A.FIRST_DAY_REFILL > '###SLICE_VALUE###') THEN B.REFILL_AMOUNT
                  ELSE A.FIRST_DAY_REFILL_AMOUNT
            END
      ) FIRST_DAY_REFILL_AMOUNT,
      CURRENT_TIMESTAMP INSERT_DATE,
      SUBSTR('###SLICE_VALUE###',1,7) EVENT_MONTH,
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
      WHERE EVENT_MONTH = substr('###SLICE_VALUE###', 1, 7)
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



