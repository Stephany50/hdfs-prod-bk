select if(A.nb_ft = 0 and B.nb_ft_imonl >=10 and C.nb_ft_cllsd >= 10 and D.nb_phin >= 10,'OK','NOK')
FROM (select count(*) as nb_ft from MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI where event_date='###SLICE_VALUE###') A,
(select count(*) as nb_ft_imonl from MON.SPARK_FT_IMEI_ONLINE where sdate='###SLICE_VALUE###') B,
(select count(*) as nb_ft_cllsd from MON.SPARK_FT_CLIENT_LAST_SITE_DAY where event_date='###SLICE_VALUE###') C,
(select count(*) as nb_phin from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date=date_add('###SLICE_VALUE###',1)) D
