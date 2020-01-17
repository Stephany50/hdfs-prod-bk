SELECT IF(A.FT_EXIST = 0 and B.msc_transaction_exist>0 ,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM mon.ft_client_site_traffic_day WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) msc_transaction_exist FROM mon.spark_ft_msc_transaction WHERE TRANSACTION_DATE='###SLICE_VALUE###' limit 1)B


