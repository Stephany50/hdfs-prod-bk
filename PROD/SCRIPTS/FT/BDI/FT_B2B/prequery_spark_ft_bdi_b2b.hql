select
if(B.nb_hlr2 >= 10 and C.nb_ft_b2b = 0 and D.nb_flotte >= 10 and E.nb_pm >= 10
and F.nb_zm_conf >= 10 ,'OK','NOK')
FROM (select count(*) as nb_hlr2 from MON.SPARK_FT_ABONNE_HLR where event_date='###SLICE_VALUE###') B
, (select count(*) as nb_zm_conf from MON.SPARK_FT_ZSMART_CONF where event_date='###SLICE_VALUE###') F
,(select count(*) as nb_ft_b2b from MON.SPARK_FT_BDI_B2B where event_date='###SLICE_VALUE###') C
,(select count(*) as nb_flotte from cdr.Spark_it_bdi_ligne_flotte_1A where original_file_date=date_add('###SLICE_VALUE###',1)) D
,(select count(*) as nb_pm from cdr.spark_it_bdi_pers_morale_1A where original_file_date=date_add('###SLICE_VALUE###',1)) E
