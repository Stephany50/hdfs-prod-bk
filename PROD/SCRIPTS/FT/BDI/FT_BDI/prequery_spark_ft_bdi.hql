select
case when ft_bdi = 0  and it_bdi >= 10 and ft_clsd >= 10 and ft_mdm >= 10 and ft_dcmm >= 10 and ft_dmomm >= 10
            and ft_accat >= 10 and ft_csnap >= 10 and it_zebm >= 10  then 'OK' else 'NOK'
end
FROM (select count(*) from MON.SPARK_FT_BDI where event_date=to_date('###SLICE_VALUE###')) ft_bdi
,(SELECT count(*) FROM CDR.SPARK_IT_BDI where original_file_date=date_add(to_date('###SLICE_VALUE###'),1)) it_bdi
,(select count(*) from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date=to_date('###SLICE_VALUE###')) ft_clsd
,(select count(*) from MON.SPARK_FT_MARKETING_DATAMART_MONTH where EVENT_MONTH=substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)) ft_mdm
,(select count(*) from MON.SPARK_FT_DATA_CONSO_MSISDN_MONTH where EVENT_MONTH = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)) ft_dcmm
,(select count(*) from  MON.SPARK_FT_DATAMART_OM_MONTH where EVENT_MONTH = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)) ft_dmomm
,(select count(*) FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE =to_date('###SLICE_VALUE###')) ft_accat
,(SELECT count(*) FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = to_date('###SLICE_VALUE###')) ft_csnap
,(select count(*) FROM CDR.SPARK_IT_ZEBRA_MASTER WHERE transaction_date = to_date('###SLICE_VALUE###')) it_zebm