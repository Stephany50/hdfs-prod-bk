select
if(ft_a_bdi.nb_ft_a_bdi = 0 and ft_bdi.nb_ft_bdi >= 10,'OK','NOK')
FROM (select count(*) as nb_ft_a_bdi from AGG.SPARK_FT_A_BDI_AMELIORE where event_date='###SLICE_VALUE###') ft_a_bdi
,(select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI_AMELIORE where event_date='###SLICE_VALUE###') ft_bdi