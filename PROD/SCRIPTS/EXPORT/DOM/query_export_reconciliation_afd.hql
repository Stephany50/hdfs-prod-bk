select trans_succes_om, trans_echec_in, trans_roll_back
from 
(select count(trim(transfer_id)) trans_succes_om from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(trim(transfer_status))='TS' and fn_format_msisdn_to_9digits(receiver_msisdn) in (select trim(msisdn) from dim.ref_compte_afd)) A,
(select count(*) trans_echec_in from MON.SPARK_FT_RECONCILIATION_AFD where transfer_datetime = "###SLICE_VALUE###") B,
(select count(*) trans_roll_back from cdr.spark_it_omny_transactions where transfer_datetime="###SLICE_VALUE###" and upper(txnmode) like '%ROLLBACKHELPER%') C