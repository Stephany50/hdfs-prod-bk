SELECT IF(T_1.FT_A_EXIST = 0 and T_2.FT_EXIST > 0 ,"OK","NOK") FT_ROAM_EXIST
FROM
(SELECT COUNT(*) FT_A_EXIST FROM MON.SPARK_FT_ROAMING_CEMAC_IN_OUT WHERE CALL_DATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_MSC_TRANSACTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2

