SELECT IF(A.AGG_EXIST = 0 and B.FT_EXIST>0,"OK","NOK") FROM
(SELECT COUNT(*) AGG_EXIST FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_POST WHERE TRANSACTION_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_BILLED_TRANSACTION_POSTPAID WHERE TRANSACTION_DATE='###SLICE_VALUE###') B
