insert into TMP.TT_IT_BDI_1B
select a.*
from
(select *
from TMP.TT_IT_BDI_1A) a
join (select distinct FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn
from cdr.spark_it_bdi_hlr
where original_file_date='###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = trim(b.msisdn)