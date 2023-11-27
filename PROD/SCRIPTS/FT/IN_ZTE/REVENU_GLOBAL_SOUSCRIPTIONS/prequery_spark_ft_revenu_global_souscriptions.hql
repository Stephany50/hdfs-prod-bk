SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_EXIST > 0 and T_3.FT_EXIST > 0 , "OK","NOK") EXIST
FROM
    (SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_REVENU_GLOBAL_SOUSCRIPTIONS WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_EXIST FROM AGG.SPARK_FT_A_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2,
    (SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_SUBS_RETAIL_ZEBRA WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_3