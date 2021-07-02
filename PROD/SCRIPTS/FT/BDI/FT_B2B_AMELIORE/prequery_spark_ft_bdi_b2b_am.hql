select
if(A.nb_ft_b2b_am = 0 and B.nb_ft_b2b >= 10,'OK','NOK')
FROM (select count(*) as nb_ft_b2b_am from MON.SPARK_FT_BDI_B2B_AM where event_date='###SLICE_VALUE###') A
,(select count(*) as nb_ft_b2b from MON.SPARK_FT_BDI_B2B where event_date='###SLICE_VALUE###') B
