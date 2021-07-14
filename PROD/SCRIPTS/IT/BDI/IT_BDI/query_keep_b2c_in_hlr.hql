insert into TMP.TT_KYC_PERS_PHY_B2C_ACTIVE
select a.*
from
(select *
from TMP.TT_KYC_PERS_PHY_B2C_FINAL) a
join (select distinct FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn
from cdr.spark_it_bdi_hlr
where original_file_date='###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = trim(b.msisdn)