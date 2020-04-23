SELECT IF(T_1.FT_A_RAS_IRSF = 0 and T_2.FT_BILLED_TRANSACTION_PREPAID > 1,"OK","NOK") FT_EXIST
FROM
    (SELECT COUNT(*) FT_A_RAS_IRSF FROM AGG.SPARK_FT_A_RAS_IRSF WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
    (SELECT COUNT(*) FT_BILLED_TRANSACTION_PREPAID FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_2