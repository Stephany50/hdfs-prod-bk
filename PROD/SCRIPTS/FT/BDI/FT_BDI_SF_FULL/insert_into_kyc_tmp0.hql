---extraction du differentiel des données contenues dans le crm.
insert into TMP.TT_KYC_BDI0
select A.* from CDR.SPARK_IT_BDI_FULL A
join (select *
from cdr.spark_it_bdi_crm_b2c
where original_file_date=date_add('###SLICE_VALUE###',1)) B on FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn))
where original_file_date =  date_add(to_date('###SLICE_VALUE###'), 1) and 
trim(A.msisdn) rlike '^\\d+$' and length(FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn))) = 9