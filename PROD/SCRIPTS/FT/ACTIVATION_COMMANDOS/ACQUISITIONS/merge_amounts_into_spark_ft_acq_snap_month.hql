INSERT OVERWRITE TABLE MON.SPARK_FT_ACQ_SNAP_MONTH
select 
      A.MSISDN MSISDN,
      (
            CASE 
                  WHEN A.FIRST_DAY_REFILL IS NOT NULL THEN LEAST(A.FIRST_DAY_REFILL, to_date('###SLICE_VALUE###'))
                  ELSE '###SLICE_VALUE###'
            END 
      ) FIRST_DAY_REFILL,
      (
            CASE 
                  WHEN (A.FIRST_DAY_REFILL IS NULL) OR (A.FIRST_DAY_REFILL IS NOT NULL AND A.FIRST_DAY_REFILL > '###SLICE_VALUE###') THEN B.REFILL_AMOUNT
                  ELSE A.FIRST_DATE_REFILL_AMOUNT
            END
      ) FIRST_DATE_REFILL_AMOUNT,
      NVL((nvl(A.REFILL_AMOUNT, 0) + nvl(B.REFILL_AMOUNT, 0)), 0) REFILL_AMOUNT,
      NVL((nvl(A.conso_amount, 0) + nvl(C.conso_amount, 0)), 0) conso_amount,
      NVL((nvl(A.depot_amount, 0) + nvl(D.depot_amount, 0)), 0) depot_amount,
      NVL((nvl(A.souscriptiondata_amount, 0) + nvl(E.souscriptiondata_amount, 0)), 0) souscriptiondata_amount,
      NVL((nvl(A.transaction_AMOUNT, 0) + nvl(F.transaction_AMOUNT, 0)), 0) transaction_AMOUNT,
      (
            CASE
                  WHEN last_day(A.ACTIVATION_DATE) = '###SLICE_VALUE###' THEN NVL((nvl(A.REFILL_AMOUNT, 0) + nvl(B.REFILL_AMOUNT, 0)), 0)
                  ELSE A.FIRST_MONTH_REFILL
            END
      ) FIRST_MONTH_REFILL,
      (
            CASE
                  WHEN last_day(A.ACTIVATION_DATE) = '###SLICE_VALUE###' THEN NVL((nvl(A.conso_amount, 0) + nvl(C.conso_amount, 0)), 0)
                  ELSE A.FIRST_MONTH_CONSO
            END
      ) FIRST_MONTH_CONSO,
      (
            CASE
                  WHEN last_day(A.ACTIVATION_DATE) = '###SLICE_VALUE###' THEN NVL((nvl(A.depot_amount, 0) + nvl(D.depot_amount, 0)), 0)
                  ELSE A.FIRST_MONTH_DEPOT
            END
      ) FIRST_MONTH_DEPOT,
      (
            CASE
                  WHEN last_day(A.ACTIVATION_DATE) = '###SLICE_VALUE###' THEN NVL((nvl(A.souscriptiondata_amount, 0) + nvl(E.souscriptiondata_amount, 0)), 0)
                  ELSE A.FIRST_MONTH_SOUSCRIPTIONDATA
            END
      ) FIRST_MONTH_SOUSCRIPTIONDATA,
      (
            CASE
                  WHEN last_day(A.ACTIVATION_DATE) = '###SLICE_VALUE###' THEN NVL((nvl(A.transaction_AMOUNT, 0) + nvl(F.transaction_AMOUNT, 0)), 0)
                  ELSE A.FIRST_MONTH_TRANSACTION
            END
      ) FIRST_MONTH_TRANSACTION,
      (
            CASE
                  WHEN last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.REFILL_AMOUNT, 0) + nvl(B.REFILL_AMOUNT, 0)), 0)
                  ELSE A.SECOND_MONTH_REFILL
            END
      ) SECOND_MONTH_REFILL,
      (
            CASE
                  WHEN last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.conso_amount, 0) + nvl(C.conso_amount, 0)), 0)
                  ELSE A.SECOND_MONTH_CONSO
            END
      ) SECOND_MONTH_CONSO,
      (
            CASE
                  WHEN last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.depot_amount, 0) + nvl(D.depot_amount, 0)), 0)
                  ELSE A.SECOND_MONTH_DEPOT
            END
      ) SECOND_MONTH_DEPOT,
      (
            CASE
                  WHEN last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.souscriptiondata_amount, 0) + nvl(E.souscriptiondata_amount, 0)), 0)
                  ELSE A.SECOND_MONTH_SOUSCRIPTIONDATA
            END
      ) SECOND_MONTH_SOUSCRIPTIONDATA,
      (
            CASE
                  WHEN last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.transaction_AMOUNT, 0) + nvl(F.transaction_AMOUNT, 0)), 0)
                  ELSE A.SECOND_MONTH_TRANSACTION
            END
      ) SECOND_MONTH_TRANSACTION,
      (
            CASE
                  WHEN last_day(concat(substr(last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')), 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.REFILL_AMOUNT, 0) + nvl(B.REFILL_AMOUNT, 0)), 0)
                  ELSE A.THIRD_MONTH_REFILL
            END
      ) THIRD_MONTH_REFILL,
      (
            CASE
                  WHEN last_day(concat(substr(last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')), 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.conso_amount, 0) + nvl(C.conso_amount, 0)), 0)
                  ELSE A.THIRD_MONTH_CONSO
            END
      ) THIRD_MONTH_CONSO,
      (
            CASE
                  WHEN last_day(concat(substr(last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')), 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.depot_amount, 0) + nvl(D.depot_amount, 0)), 0)
                  ELSE A.THIRD_MONTH_DEPOT
            END
      ) THIRD_MONTH_DEPOT,
      (
            CASE
                  WHEN last_day(concat(substr(last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')), 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.souscriptiondata_amount, 0) + nvl(E.souscriptiondata_amount, 0)), 0)
                  ELSE A.THIRD_MONTH_SOUSCRIPTIONDATA
            END
      ) THIRD_MONTH_SOUSCRIPTIONDATA,
      (
            CASE
                  WHEN last_day(concat(substr(last_day(concat(substr(A.ACTIVATION_DATE, 1, 7), '-01')), 1, 7), '-01')) = '###SLICE_VALUE###' THEN NVL((nvl(A.transaction_AMOUNT, 0) + nvl(F.transaction_AMOUNT, 0)), 0)
                  ELSE A.THIRD_MONTH_TRANSACTION
            END
      ) THIRD_MONTH_TRANSACTION,
      CURRENT_TIMESTAMP INSERT_DATE,
      SUBSTR('###SLICE_VALUE###',1,7) EVENT_MONTH,
      A.ACTIVATION_DATE
from  
(
      SELECT 
            MSISDN,
            FIRST_DAY_REFILL,
            FIRST_DATE_REFILL_AMOUNT,
            REFILL_AMOUNT,
            conso_amount,
            depot_amount,
            souscriptiondata_amount,
            transaction_AMOUNT,
            FIRST_MONTH_CONSO,
            FIRST_MONTH_DEPOT,
            FIRST_MONTH_REFILL,
            FIRST_MONTH_SOUSCRIPTIONDATA,
            FIRST_MONTH_TRANSACTION,
            SECOND_MONTH_CONSO,
            SECOND_MONTH_DEPOT,
            SECOND_MONTH_REFILL,
            SECOND_MONTH_SOUSCRIPTIONDATA,
            SECOND_MONTH_TRANSACTION,
            THIRD_MONTH_CONSO,
            THIRD_MONTH_DEPOT,
            THIRD_MONTH_REFILL,
            THIRD_MONTH_SOUSCRIPTIONDATA,
            THIRD_MONTH_TRANSACTION,
            ACTIVATION_DATE
      FROM MON.SPARK_FT_TMP_ACQ_SNAP_MONTH
      WHERE EVENT_MONTH = substr('###SLICE_VALUE###', 1, 7)
)  A 
left join
(
      SELECT 
            RECEIVER_MSISDN MSISDN, 
            SUM(REFILL_AMOUNT) REFILL_AMOUNT
      FROM MON.SPARK_FT_REFILL
      WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '200'
      GROUP BY RECEIVER_MSISDN
) B
ON GET_NNP_MSISDN_9DIGITS(A.MSISDN) = GET_NNP_MSISDN_9DIGITS(B.MSISDN)
left join
(
      select 
            msisdn,
            sum(CONSO) conso_amount
      from MON.SPARK_FT_CONSO_MSISDN_DAY 
      where event_date = '###SLICE_VALUE###' and nvl(CONSO, 0) > 0 
      group by msisdn
) C
on GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(C.msisdn)
left join
(
      select 
            RECEIVER_MSISDN msisdn,
            sum(TRANSACTION_AMOUNT) DEPOT_AMOUNT
      from CDR.SPARK_IT_OMNY_TRANSACTIONS 
      where TRANSFER_DATETIME = '###SLICE_VALUE###' and TRANSFER_STATUS='TS'
      group by RECEIVER_MSISDN
) D
on GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(D.msisdn)
left join
(
      select 
            served_party_msisdn msisdn,
            sum(amount_data) souscriptiondata_amount
      from mon.spark_ft_subscription 
      where transaction_date = '###SLICE_VALUE###' and nvl(amount_data, 0) > 0
      group by served_party_msisdn
) E
on GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(E.msisdn)
left join
(
      select 
            sender_MSISDN msisdn,
            sum(TRANSACTION_AMOUNT) transaction_AMOUNT
      from CDR.SPARK_IT_OMNY_TRANSACTIONS 
      where TRANSFER_DATETIME = '###SLICE_VALUE###' and TRANSFER_STATUS='TS'
      group by sender_MSISDN
) F
on GET_NNP_MSISDN_9DIGITS(A.msisdn) = GET_NNP_MSISDN_9DIGITS(F.msisdn)
