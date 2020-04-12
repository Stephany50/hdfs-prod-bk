INSERT INTO AGG.SPARK_FT_A_CREDIT_TRANSFER_SENDER

SELECT

sender_msisdn,
commercial_offer,
count(*) transfer_count,
sum(TRANSFER_AMT) transfer_amount,
sum(TRANSFER_FEES) TRANSFER_FEES,
current_timestamp last_update,
SENDER_OPERATOR_CODE operator_code,
(CASE WHEN UPPER(ORIGINAL_FILE_NAME) LIKE '%VIP%' THEN 1 ELSE 0 END) IS_VIP,
REFILL_DATE

from MON.SPARK_FT_CREDIT_TRANSFER
WHERE REFILL_DATE ='###SLICE_VALUE###' and termination_ind='000'
group by refill_date, sender_msisdn, TERMINATION_IND, commercial_offer, SENDER_OPERATOR_CODE
,(CASE WHEN UPPER(ORIGINAL_FILE_NAME) LIKE '%VIP%' THEN 1 ELSE 0 END)
order by refill_date




