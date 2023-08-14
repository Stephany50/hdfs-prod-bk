insert into mon.spark_ft_report_afd PARTITION(event_date)
select trans_succes_om, trans_succes_in, trans_roll_back, '###SLICE_VALUE###' event_date 
from 
(
    select count(trim(transfer_id)) trans_succes_om from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(trim(transfer_status))='TS' and fn_format_msisdn_to_9digits(receiver_msisdn) in (select distinct trim(msisdn) from dim.ref_compte_afd)
) A,
(
    select count(distinct trim(transactionsn)) trans_succes_in from CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE between '###SLICE_VALUE###' and date_add('###SLICE_VALUE###', 1)
    and trim(transactionsn) in 
    (
        select distinct trim(transfer_id) from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(trim(transfer_status))='TS' and fn_format_msisdn_to_9digits(receiver_msisdn) in (select distinct trim(msisdn) from dim.ref_compte_afd)
    )
) B,
(
    select count(*) trans_roll_back from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(txnmode) like '%ROLLBACKHELPER%'
) C