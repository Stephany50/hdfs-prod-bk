SELECT IF(T.FT_EXIST = 0 and VOICE_EXIST>0 and DATA_EXIST>0 and ADJUSTMENT_EXIST>0 and RECHARGE_EXIST>0 and SUBSCRIPTION_EXIST>0
    and TRANSFER_EXIST>0 and ED_EXIST>0 and EC_EXIST>0 and BAL_RESET_EXIST>0 and DEL_EXPBAL_EXIST >0  ,"OK","NOK") TABLES_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_EDR_PRPD_EQT WHERE EVENT_DATE='###SLICE_VALUE###') T,
(SELECT COUNT(*) VOICE_EXIST FROM CDR.SPARK_IT_ZTE_VOICE_SMS WHERE START_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) DATA_EXIST FROM CDR.SPARK_IT_ZTE_DATA WHERE START_DATE='###SLICE_VALUE###') B,
(SELECT COUNT(*) ADJUSTMENT_EXIST FROM CDR.SPARK_IT_ZTE_ADJUSTMENT WHERE CREATE_DATE='###SLICE_VALUE###') C,
(SELECT COUNT(*) RECHARGE_EXIST FROM CDR.SPARK_IT_ZTE_RECHARGE WHERE PAY_DATE='###SLICE_VALUE###') D,
(SELECT COUNT(*) SUBSCRIPTION_EXIST FROM CDR.SPARK_IT_ZTE_SUBSCRIPTION WHERE CREATEDDATE='###SLICE_VALUE###') E,
(SELECT COUNT(*) TRANSFER_EXIST FROM CDR.SPARK_IT_ZTE_TRANSFER WHERE PAY_DATE='###SLICE_VALUE###') F,
(SELECT COUNT(*) ED_EXIST FROM CDR.SPARK_IT_ZTE_EMERGENCY_DATA WHERE TRANSACTION_DATE='###SLICE_VALUE###') G,
(SELECT COUNT(*) EC_EXIST FROM CDR.SPARK_IT_ZTE_EMERGENCY_CREDIT WHERE TRANSACTION_DATE='###SLICE_VALUE###') H,
(SELECT COUNT(*) BAL_RESET_EXIST FROM CDR.SPARK_IT_ZTE_BALANCE_RESET WHERE BAL_RESET_DATE='###SLICE_VALUE###') I,
(SELECT COUNT(*) DEL_EXPBAL_EXIST FROM CDR.SPARK_IT_ZTE_DEL_EXPBAL WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') J;