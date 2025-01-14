SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_CONTRACT_SNAPSHOT>0 and T_3.FT_BILLED_TRANSACTION>0 and
T_4.FT_BILLED_TRANSACTION_POST > 0 and T_5.FT_CRA_GPRS > 0 ,"OK","NOK") FT_ROAM_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_ROAMING_RETAIL_OUT WHERE EVENT_DATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_CONTRACT_SNAPSHOT FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(*) FT_BILLED_TRANSACTION FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_3,
(SELECT COUNT(*) FT_BILLED_TRANSACTION_POST FROM MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_4,
(SELECT COUNT(*) FT_CRA_GPRS FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE = '###SLICE_VALUE###') T_5
