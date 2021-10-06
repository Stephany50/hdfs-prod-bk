select
if(A.nb_zm_conf = 0 and B.nb_zm >= 10,'OK','NOK')
FROM (select count(*) as nb_zm_conf from MON.SPARK_FT_ZSMART_CONF where event_date='###SLICE_VALUE###') A
,(select count(*) as nb_zm from cdr.spark_it_bdi_zsmart where original_file_date=date_add('###SLICE_VALUE###',1)) B
