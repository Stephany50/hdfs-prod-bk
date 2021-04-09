insert into TMP.TT_BDI_LIGNE_FLOTTE2_1C
select a.*
from
(select *
from TMP.TT_BDI_LIGNE_FLOTTE2_1B) a
join (select distinct FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn
from cdr.spark_it_bdi_hlr
where original_file_date='###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = trim(b.msisdn)