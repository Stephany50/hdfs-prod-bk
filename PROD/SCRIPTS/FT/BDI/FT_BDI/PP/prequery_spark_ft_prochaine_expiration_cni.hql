select
if(ft_expiration.nb_ft_expiration = 0 and ft_bdi.nb_ft_bdi >= 10 ,'OK','NOK')
FROM (select count(*) as nb_ft_expiration from MON.SPARK_FT_PROCHAINE_EXPIRATION_CNI where event_date='###SLICE_VALUE###') ft_expiration
,(select count(*) as nb_ft_bdi from MON.SPARK_FT_KYC_BDI_PP where event_date='###SLICE_VALUE###') ft_bdi
