insert into TMP.TT_ZSMART_GAP2
select a.*,b.* from
(select *
from TMP.tt_gap_bdi_hlr
) a
left join
(select *
from cdr.spark_it_bdi_zsmart
where original_file_date='###SLICE_VALUE###'
) b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))