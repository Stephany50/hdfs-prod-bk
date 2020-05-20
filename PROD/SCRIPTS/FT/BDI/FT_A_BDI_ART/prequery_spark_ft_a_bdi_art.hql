select
if(ft_a_bdi.nb_ft_a_bdi = 0 and ft_bdi.nb_ft_bdi >= 10,'OK','NOK')
FROM (select count(*) as nb_ft_a_bdi from MON.SPARK_FT_A_BDI_ART where event_date_plus1=date_add(to_date('###SLICE_VALUE###'),1)) ft_a_bdi
,(select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI_ART where event_date=to_date('###SLICE_VALUE###')) ft_bdi