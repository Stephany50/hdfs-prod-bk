insert into AGG.spark_FT_A_CREDIT_TRANSFER_REVENUE
select
      substr(REFILL_TIME,1,2) refill_hour,
      TERMINATION_IND ,
      COMMERCIAL_OFFER,
      count(*) transfer_count,
      sum(TRANSFER_AMT) transfer_amount,
      sum(TRANSFER_FEES) TRANSFER_FEES,
      CURRENT_TIMESTAMP last_update,
      SENDER_OPERATOR_CODE OPERATOR_CODE,
      (CASE WHEN UPPER(ORIGINAL_FILE_NAME) LIKE '%VIP%' THEN 1 ELSE 0 END) IS_VIP,
      REFILL_DATE
from mon.spark_ft_credit_transfer
where refill_date = '###SLICE_VALUE###'
group by
    REFILL_DATE,
    substr(REFILL_TIME,1,2),
    TERMINATION_IND ,
    COMMERCIAL_OFFER,
    SENDER_OPERATOR_CODE,
    (CASE WHEN UPPER(ORIGINAL_FILE_NAME) LIKE '%VIP%' THEN 1 ELSE 0 END)
