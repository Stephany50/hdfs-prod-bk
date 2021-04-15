SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_EXIST>0 and T_3.FT_EXIST>0 , "OK", "NOK")
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_SUBSCRIPTION_ACCESS WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_EXIST FROM CDR.SPARK_IT_BDI_LIGNE_FLOTTE WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###') T_3