---extraction du differentiel des données contenues dans le crm.
insert into TMP.TT_KYC_BDI0
select A.* from (select * from CDR.SPARK_IT_BDI_FULL where original_file_date=date_add('###SLICE_VALUE###',1)) A
join (select * from cdr.spark_it_bdi_crm_b2c where original_file_date=date_add('###SLICE_VALUE###',1)) B 
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn))