SELECT IF(T_1.FT_EXIST = 0 and T_2.FT_SUBSCRIPTION>0 ,"OK","NOK") IT_CTI_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY WHERE PERIOD = '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_SUBSCRIPTION FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2

