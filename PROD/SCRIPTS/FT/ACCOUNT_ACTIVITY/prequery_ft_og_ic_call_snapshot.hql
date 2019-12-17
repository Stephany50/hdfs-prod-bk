SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_MSC_TRANSACTION_BEFORE>0 and T_3.FT_SUBSCRIPTION_BEFORE>0 and T_4.FT_CRA_GPRS_BEFORE>0 and T_5.FT_OG_IC_CALL_BEFORE>0 ,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_OG_IC_CALL_SNAPSHOT WHERE EVENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_MSC_TRANSACTION_BEFORE FROM MON.FT_MSC_TRANSACTION WHERE TRANSACTION_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_2,
(SELECT COUNT(*) FT_SUBSCRIPTION_BEFORE FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_3,
(SELECT COUNT(*) FT_CRA_GPRS_BEFORE FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_4,
(SELECT COUNT(*) FT_OG_IC_CALL_BEFORE FROM MON.FT_OG_IC_CALL_SNAPSHOT WHERE EVENT_DATE=DATE_SUB('###SLICE_VALUE###',1)) T_5
