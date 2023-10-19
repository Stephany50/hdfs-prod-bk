INSERT INTO  MON.SPARK_FT_OMA_TRANSACTION
select 
    sender_msisdn,
    receiver_msisdn,
    transfer_id,
    transaction_amount,
    FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME_NQ,'dd/MM/yyyy HH:mm:ss')+3600) TRANSFER_DATETIME,
    txnmode,
    transfer_status,
    service_type,
    transfer_datetime transaction_date 
from  CDR.SPARK_IT_OMNY_TRANSACTIONS 
where transfer_datetime='###SLICE_VALUE###'