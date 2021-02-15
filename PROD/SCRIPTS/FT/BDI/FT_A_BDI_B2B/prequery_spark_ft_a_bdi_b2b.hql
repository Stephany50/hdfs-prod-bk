select
if(A.nb_fta_b2b = 0 and B.nb_ft_b2b >= 10,'OK','NOK')
FROM (select count(*) as nb_fta_b2b from AGG.SPARK_FT_A_BDI_B2B where event_date='###SLICE_VALUE###') A
,(select count(*) as nb_ft_b2b from MON.SPARK_FT_BDI_B2B where event_date='###SLICE_VALUE###') B