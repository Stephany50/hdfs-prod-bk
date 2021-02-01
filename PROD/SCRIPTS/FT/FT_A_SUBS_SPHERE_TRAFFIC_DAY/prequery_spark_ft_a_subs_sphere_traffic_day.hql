select
if(A.nb_ft_subs = 0 and B.nb_ft_msc_tr >= 10,'OK','NOK')
FROM (select count(*) as nb_ft_subs from AGG.SPARK_FT_A_SUBS_SPHERE_TRAFFIC_DAY where event_date ='###SLICE_VALUE###') A
,(select count(*) as nb_ft_msc_tr from MON.SPARK_FT_MSC_TRANSACTION where TRANSACTION_DATE='###SLICE_VALUE###') B