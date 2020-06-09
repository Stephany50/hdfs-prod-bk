select
if(it_bdi_art.nb_it_bdi_art = 0
    and it_bdi.nb_it_bdi >= 10
    and ft_bdi.nb_ft_bdi >= 10
    and ft_csnap.nb_ft_csnap >= 10
    and ft_clsd.nb_ft_clsd >= 10
    --and it_zsm.nb_zsm >= 10
    --and ph_om.nb_ph_om >= 10
  ,'OK'
  ,'NOK'
)
FROM (select count(*) as nb_it_bdi_art from CDR.SPARK_IT_BDI_ART where ORIGINAL_FILE_DATE=to_date('###SLICE_VALUE###')) it_bdi_art
,(select count(*) as nb_it_bdi from CDR.SPARK_IT_BDI where ORIGINAL_FILE_DATE=to_date('###SLICE_VALUE###')) it_bdi
,(select count(*) as nb_ft_bdi from Mon.spark_ft_bdi where event_date=date_sub(to_date('###SLICE_VALUE###'),1)) ft_bdi
,(SELECT count(*) as nb_ft_csnap FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = date_sub(to_date('###SLICE_VALUE###'),1)) ft_csnap
,(select count(*) as nb_ft_clsd from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date=date_sub(to_date('###SLICE_VALUE###'),1)) ft_clsd
--,(select count(*) as nb_zsm from CDR.SPARK_IT_BDI_ZSMART where ORIGINAL_FILE_DATE=to_date('###SLICE_VALUE###')) it_zsm
--,(select count(*) as nb_ph_om from MON.spark_ft_omny_account_snapshot where event_date = date_sub(to_date('###SLICE_VALUE###'),1)) ph_om