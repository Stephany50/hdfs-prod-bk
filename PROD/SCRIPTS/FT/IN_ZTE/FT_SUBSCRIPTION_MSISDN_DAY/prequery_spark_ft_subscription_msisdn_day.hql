SELECT IF(T_1.FT_MSISDN_DAY_COUNT = 0 and T_2.FT_COUNT > 1000,"OK","NOK") MSISDN_DAY_EXIST
FROM
    (SELECT COUNT(*) FT_MSISDN_DAY_COUNT FROM MON.SPARK_FT_SUBSCRIPTION_MSISDN_DAY WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2