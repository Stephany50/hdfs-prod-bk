SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_CONTRACT_SNAPSHOT>0 and T_3.FT_ACCOUNT_ACTIVITY_BEFORE>0 and T_4.FT_OG_IC_CALL_SNAPSHOT>0 ,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_CONTRACT_SNAPSHOT FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_ACCOUNT_ACTIVITY_BEFORE FROM MON.SPARK_FT_ACCOUNT_ACTIVITY WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1)) T_3,
(SELECT COUNT(*) FT_OG_IC_CALL_SNAPSHOT FROM MON.SPARK_FT_OG_IC_CALL_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###') T_4