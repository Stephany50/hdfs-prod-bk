select
if(A.nb_ft_bdi >= 10 and B.nb_ft_clsd >= 10 and C.nb_ft_oasnt >= 10 and D.nb_ft_atate = 0
and E.nb_ft_bdi1 >=10 and F.nb_ft_dim >=10 and G.nb_it_30J >= 10
            ,'OK','NOK')
FROM (select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI_CRM_B2C where event_date='###SLICE_VALUE###') A
,(select count(*) as nb_ft_clsd from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date='###SLICE_VALUE###') B
,(select count(*) as nb_ft_oasnt from MON.SPARK_FT_OMNY_ACCOUNT_SNAPSHOT where event_date='###SLICE_VALUE###') C
,(select count(*) as nb_ft_atate from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO where event_date='###SLICE_VALUE###') D
,(select count(*) as nb_ft_bdi1 from MON.SPARK_FT_BDI where event_date='###SLICE_VALUE###') E
,(select count(*) as nb_ft_dim from MON.BACKUP_SPARK_DT_BASE_IDENTIFICATION where event_date='###SLICE_VALUE###') F
,(select count(*) as nb_it_30J from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY_30J where event_date='###SLICE_VALUE###') G
