SELECT IF(A.FT_EXIST = 0 and B.FT_MSC_TRANSACTION_EXIST>0 and C.ft_refill_EXIST>0, "OK", "NOK") FROM
(SELECT count(*) FT_EXIST FROM mon.spark_ft_refill_location WHERE refill_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_MSC_TRANSACTION_EXIST FROM MON.SPARK_FT_MSC_TRANSACTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') B,
(SELECT COUNT(*) ft_refill_EXIST FROM MON.Spark_ft_refill WHERE refill_date='###SLICE_VALUE###') C
