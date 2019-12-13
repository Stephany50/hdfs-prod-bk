SELECT IF(T_1.FT_A_COUNT = 0 and T_2.FT_COUNT > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_A_COUNT FROM AGG.SPARK_FT_QOS_SMSC_SPECIAL_NUMBER WHERE STATE_DATE='###SLICE_VALUE###' LIMIT 100) T_1,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_SMSC_TRANSACTION_A2P WHERE TRANSACTION_BILLING_DATE='###SLICE_VALUE###' LIMIT 100) T_2
