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
on substr(upper(trim(a.msisdn)),-9,9) = substr(upper(trim(b.msisdn)),-9,9)