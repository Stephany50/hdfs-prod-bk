SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_BILLED_PREPAID>0 ,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_VOICE_SMS_DA_USAGE_DAILY WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_BILLED_PREPAID FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2

