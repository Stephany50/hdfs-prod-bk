SELECT IF(T_1.FT_AMN_LOCAL_TRAFFIC_DAY = 0 and T_2.FT_COUNT > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_AMN_LOCAL_TRAFFIC_DAY FROM MON.FT_AMN_LOCAL_TRAFFIC_DAY2 WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.FT_MSC_TRANSACTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2
