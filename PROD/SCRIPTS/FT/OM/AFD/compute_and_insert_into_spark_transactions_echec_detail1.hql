insert into TMP.TT_RECON_AFD1
select 
transfer_id,
sender_msisdn,
receiver_msisdn,
transaction_amount,
transfer_status,
txnmode,
transfer_datetime
from cdr.spark_it_omny_transactions
where transfer_datetime='###SLICE_VALUE###'
and transfer_id in
(select trim(transfer_id) from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(trim(transfer_status))='TS' and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)
minus
(
select trim(transactionsn) from CDR.SPARK_IT_ZTE_SUBSCRIPTION  where CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1) 
)
)