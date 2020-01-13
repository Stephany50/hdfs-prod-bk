SELECT IF(T_1.FT_A_COUNT = 0 and T_2.FT_COUNT > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_A_COUNT FROM AGG.SPARK_FT_A_GPRS_LOCATION WHERE SESSION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE='###SLICE_VALUE###') T_2
