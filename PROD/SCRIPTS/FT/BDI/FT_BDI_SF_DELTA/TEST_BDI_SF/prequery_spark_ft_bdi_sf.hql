select 
if(A.nb_ft_sf_b2c = 0 and B.nb_ft_b2c_dlk >=0 and
D.nb_photo_in >= 0,'OK','NOK')
FROM (select count(*) as nb_ft_sf_b2c from MON.SPARK_FT_BDI_SF where event_date='###SLICE_VALUE###') A 
,(select count(*) as nb_ft_b2c_dlk from Mon.spark_ft_bdi where event_date='###SLICE_VALUE###') B
,(select count(*) as nb_photo_in from Mon.spark_ft_contract_snapshot where event_date=date_add('###SLICE_VALUE###',1)) D