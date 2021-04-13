select
if(A.nb_ft_bdi >= 10 and B.nb_ft_clsd >= 10 and C.nb_ft_oasnt >= 10 and D.nb_ft_atate = 0
            ,'OK','NOK')
FROM (select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI where event_date='###SLICE_VALUE###') A
,(select count(*) as nb_ft_clsd from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date='###SLICE_VALUE###') B
,(select count(*) as nb_ft_oasnt from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT where event_date='###SLICE_VALUE###') C
,(select count(*) as nb_ft_atate from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO where event_date='###SLICE_VALUE###') D