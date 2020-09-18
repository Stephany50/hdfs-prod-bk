select
if(ft_a_bdi.nb_ft_a_bdi = 0 and ft_bdi.nb_ft_bdi >= 10
AND ft_a_bdi_prev.nb_ft_a_bdi_p > 0,'OK','NOK')
FROM (select count(*) as nb_ft_a_bdi from MON.SPARK_FT_A_BDI_AMELIORE where event_date_plus1='###SLICE_VALUE###') ft_a_bdi
, (select count(*) as nb_ft_a_bdi_p from MON.SPARK_FT_A_BDI_AMELIORE where event_date_plus1=date_sub('###SLICE_VALUE###',1)) ft_a_bdi_prev
,(select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI_AMELIORE where event_date=date_sub('###SLICE_VALUE###',1)) ft_bdi