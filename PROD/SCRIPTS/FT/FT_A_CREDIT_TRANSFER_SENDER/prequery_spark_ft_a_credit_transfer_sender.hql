SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT>0 , "OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM AGG.SPARK_FT_A_CREDIT_TRANSFER_SENDER WHERE REFILL_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_CREDIT_TRANSFER WHERE REFILL_DATE='###SLICE_VALUE###') B



