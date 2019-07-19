SELECT IF(A.FT_EXIST = 0 and B.msc_transaction_exist>0 ,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM mon.ft_client_site_traffic_day WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) msc_transaction_exist FROM mon.ft_msc_transaction WHERE EVENT_DATE='###SLICE_VALUE###')B


