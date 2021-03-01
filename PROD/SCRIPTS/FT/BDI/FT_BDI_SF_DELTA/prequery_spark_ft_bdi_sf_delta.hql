select
if(A.nb_ft_sf_b2c = 0 and B.nb_ft_sf_b2b = 0 and C.nb_ft_b2b_dlk >=0 and
D.nb_ft_b2c_dlk >= 0 and E.nb_crm_b2c >= 0 and F.nb_photo_in >= 0,'OK','NOK')
FROM (select count(*) as nb_ft_sf_b2c from MON.SPARK_FT_BDI_SF_DELTA where event_date='###SLICE_VALUE###') A
,(select count(*) as nb_ft_sf_b2b from MON.SPARK_FT_BDI_B2B_SF_DELTA where event_date='###SLICE_VALUE###') B
,(select count(*) as nb_ft_b2b_dlk from Mon.spark_ft_bdi_B2B where event_date='###SLICE_VALUE###') C
,(select count(*) as nb_ft_b2c_dlk from Mon.spark_ft_bdi_1A where event_date='###SLICE_VALUE###') D
,(select count(*) as nb_crm_b2c from cdr.spark_it_bdi_crm_b2c where original_file_date=date_add('###SLICE_VALUE###',1)) E
,(select count(*) as nb_photo_in from Mon.spark_ft_contract_snapshot where event_date=date_add('###SLICE_VALUE###',1)) F




