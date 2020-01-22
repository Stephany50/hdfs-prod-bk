SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_CRA_GPRS>0 ,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_DATA_DA_USAGE_DAILY WHERE SESSION_DATE = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_CRA_GPRS FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE = '###SLICE_VALUE###') T_2
