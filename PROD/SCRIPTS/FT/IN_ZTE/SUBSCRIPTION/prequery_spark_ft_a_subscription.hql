SELECT IF(T_1.FT_A_COUNT = 0 and T_2.FT_COUNT > 1000,"OK","NOK") EXTRACT_EXIST
FROM
(SELECT COUNT(*) FT_A_COUNT FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2