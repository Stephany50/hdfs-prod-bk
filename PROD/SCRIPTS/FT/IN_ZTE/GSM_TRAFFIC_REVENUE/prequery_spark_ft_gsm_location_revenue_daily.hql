SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT>0,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_GSM_LOCATION_REVENUE_DAILY WHERE TRANSACTION_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID WHERE TRANSACTION_DATE='###SLICE_VALUE###') B