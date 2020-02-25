SELECT IF(T_1.FT_A_OVD_COUNT = 0 and T_2.FT_OVD_COUNT > 1,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) FT_A_OVD_COUNT FROM MON.SPARK_FT_A_OVERDRAFT WHERE DATECODE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_OVD_COUNT FROM MON.SPARK_FT_OVERDRAFT WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2
