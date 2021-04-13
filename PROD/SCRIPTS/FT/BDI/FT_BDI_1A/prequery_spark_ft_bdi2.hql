select
if(ft_bdi.nb_ft_bdi = 0 and it_bdi.nb_it_bdi >= 10 and ft_clsd.nb_ft_clsd >= 10 and ft_mdm.nb_ft_mdm >= 10
     and ft_dmomm.nb_ft_dmomm >= 10 and ft_bdi_prev.nb_ft_bdi >= 10
            and ft_accat.nb_ft_accat >= 10
            and ft_csnap.nb_ft_csnap >= 10
            ,'OK','NOK')
FROM (select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI where event_date='###SLICE_VALUE###') ft_bdi
,(select count(*) as nb_ft_bdi from MON.SPARK_FT_BDI where event_date=DATE_SUB('###SLICE_VALUE###',1)) ft_bdi_prev
,(SELECT count(*) as nb_it_bdi FROM CDR.SPARK_IT_BDI where original_file_date=date_add('###SLICE_VALUE###',1)) it_bdi
,(select count(*) as nb_ft_clsd from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date='###SLICE_VALUE###') ft_clsd
,(select count(*) as nb_ft_mdm from MON.SPARK_FT_MARKETING_DATAMART_MONTH where EVENT_MONTH=substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)) ft_mdm
,(select count(*) as nb_ft_dmomm from  MON.SPARK_FT_DATAMART_OM_MONTH where mois = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)) ft_dmomm
,(select count(*) as nb_ft_accat FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE =date_add('###SLICE_VALUE###',1)) ft_accat
,(SELECT count(*) as nb_ft_csnap FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = date_add('###SLICE_VALUE###',1)) ft_csnap