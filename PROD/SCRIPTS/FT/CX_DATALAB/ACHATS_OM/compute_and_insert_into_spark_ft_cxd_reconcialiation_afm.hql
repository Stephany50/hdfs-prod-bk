select
numero,
transfer_id,
date_debit,
IF(P5.date_depot is not null, P5.date_depot,P2.date_depot)date_depot
from
(select 
sender_msisdn numero,
transfer_id,
transfer_datetime_nq date_debit
from cdr.spark_it_omny_transactions
where transfer_datetime="###SLICE_VALUE###"
and transfer_id in
(select trim(upper(transfer_id)) from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' 
and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)
minus
(select trim(upper(transactionsn)) from CDR.SPARK_IT_ZTE_SUBSCRIPTION where WHERE CREATEDDATE='###SLICE_VALUE###')
)
)P1
left join
(select trim(transactionsn)transactionsn,nq_createddate date_depot from CDR.SPARK_IT_ZTE_SUBSCRIPTION where WHERE CREATEDDATE = "###SLICE_VALUE###")P2
ON P1.transfer_id=P2.transactionsn
left join
(select trim(upper(transfer_id))transfer_id,receiver_msisdn sender_msisdn, transfer_datetime date_depot from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(txnmode) like '%ROLLBACKHELPER%')P5
ON P1.transfer_id=P5.transfer_id

union

select
P3.numero,
P3.transfer_id,
P3.date_debit,
IF(P4.nq_createddate is not null,P4.nq_createddate,P3.date_depot) date_depot
from 
(SELECT * FROM MON.SPARK_FT_CXD_RECONCIALIATION_AFM WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1) AND DATE_DEPOT is null)P3
left join
(select trim(transactionsn)transactionsn,nq_createddate from CDR.SPARK_IT_ZTE_SUBSCRIPTION where WHERE CREATEDDATE='###SLICE_VALUE###')P4
ON P3.transfer_id=trim(upper(P4.transactionsn))
left join
(select trim(upper(transfer_id))transfer_id,receiver_msisdn sender_msisdn, transfer_datetime date_depot from cdr.spark_it_omny_transactions where transfer_datetime='###SLICE_VALUE###' and upper(txnmode) like '%ROLLBACKHELPER%')P6
ON P3.transfer_id=P6.transfer_id


