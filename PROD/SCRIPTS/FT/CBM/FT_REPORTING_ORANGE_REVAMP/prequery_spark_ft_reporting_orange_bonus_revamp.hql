SELECT IF(A.FT_EXIST_Revamp = 0 and B.FT_EXIST_Subsc>0 ,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST_Revamp FROM MON.SPARK_FT_REPORTING_ORANGE_BONUS_REVAMP WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_EXIST_Subsc FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') B