SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_RECHARGE_EXIST>0 and T_3.PROFILE_EXIST>0,"OK","NOK") EXTRACT_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_REFILL WHERE REFILL_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_RECHARGE_EXIST FROM MON.FT_RECHARGE WHERE TRANSACTION_DATE='###SLICE_VALUE###')T_2,
(SELECT COUNT(*) PROFILE_EXIST FROM CDR.SPARK_IT_ZTE_PROFILE WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_3
