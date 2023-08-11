insert into MON.SPARK_FT_RECONCILIATION_AFD PARTITION(transfer_datetime)
select  
B.transfer_id,
A.sender_msisdn,
A.receiver_msisdn,
A.transaction_amount,
B.transfer_status,
B.txnmode,
B.transfer_datetime
from
TMP.TT_RECON_AFD2 A
left join 
TMP.TT_RECON_AFD1 B
on A.sender_msisdn = B.sender_msisdn and A.receiver_msisdn = B.receiver_msisdn and A.transaction_amount = B.transaction_amount