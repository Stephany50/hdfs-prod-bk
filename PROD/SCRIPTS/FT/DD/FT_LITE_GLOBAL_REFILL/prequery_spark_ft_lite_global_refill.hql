SELECT IF(A.FT_EXIST = 0 and B.FT_SUBSCRIPTION_EXIST>0, "OK", "NOK") FROM
(SELECT count(*) FT_EXIST FROM MON.SPARK_FT_LITE_GLOBAL_REFILL WHERE refill_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_SUBSCRIPTION_EXIST FROM MON.SPARK_ft_refill WHERE refill_date='###SLICE_VALUE###') B