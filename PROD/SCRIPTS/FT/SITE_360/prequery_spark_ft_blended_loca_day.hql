SELECT IF(T_1.FT_MSISDN_DAY_COUNT = 0 and T_2.FT_MSISDN_DAY_COUNT > 0 and T_3.FT_MSISDN_DAY_COUNT > 0,"OK","NOK") COUNT_DAY_EXIST
FROM
    (SELECT COUNT(*) FT_MSISDN_DAY_COUNT FROM mon.spark_ft_blended_loca_day WHERE event_date='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_MSISDN_DAY_COUNT FROM mon.spark_ft_client_last_site_day WHERE event_date='###SLICE_VALUE###') T_2,
    (SELECT COUNT(*) FT_MSISDN_DAY_COUNT FROM mon.spark_ft_client_site_traffic_day WHERE event_date='###SLICE_VALUE###') T_3