select
if(A.nb_ft = 0 and B.nb_it >= 10 and C.nb_ft_prev >= 10,'OK','NOK')
FROM (select count(*) as nb_ft from MON.SPARK_FT_BDI_CRM_B2C where event_date = '###SLICE_VALUE###') A
,(select count(*) as nb_it from cdr.spark_it_bdi_crm_b2c where original_file_date = date_add('###SLICE_VALUE###',1)) B
,  (select count(*) as nb_ft_prev from MON.SPARK_FT_BDI_CRM_B2C where event_date = date_sub('###SLICE_VALUE###',1)) C