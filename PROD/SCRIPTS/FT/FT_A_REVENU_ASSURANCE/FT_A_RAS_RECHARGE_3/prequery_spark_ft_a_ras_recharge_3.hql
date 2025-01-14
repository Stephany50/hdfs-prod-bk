SELECT IF(T_1.FT_A_RAS_RECHARGE_3 = 0 and T_2.FT_CREDIT_TRANSFER > 1,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) FT_A_RAS_RECHARGE_3 FROM AGG.SPARK_FT_A_RAS_RECHARGE_3 WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_CREDIT_TRANSFER FROM MON.SPARK_FT_CREDIT_TRANSFER WHERE REFILL_DATE='###SLICE_VALUE###') T_2