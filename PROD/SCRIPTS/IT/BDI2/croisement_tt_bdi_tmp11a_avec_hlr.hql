insert into TMP.TT_BDIHLR
select distinct a.msisdn as msisdn
from
(select distinct  FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn
from  TMP.TT_BDI_TMP1_1A
where not(msisdn is null or trim(msisdn) = '')) a
join (select distinct FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn
from cdr.spark_it_bdi_hlr
where original_file_date='###SLICE_VALUE###') b
on trim(a.msisdn) = trim(b.msisdn)