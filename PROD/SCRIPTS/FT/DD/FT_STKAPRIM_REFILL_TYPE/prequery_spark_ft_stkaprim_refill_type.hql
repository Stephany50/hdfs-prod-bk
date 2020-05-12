SELECT IF(A.FT_EXIST = 0 and B.ft_refill_EXIST>0 AND C.IT_ZEBRA_MASTER_BALANCE_EXIST, "OK", "NOK") FROM
(SELECT count(*) FT_EXIST FROM MON.SPARK_FT_STKAPRIM_REFILL_TYPE WHERE refill_date='###SLICE_VALUE###') A,
(SELECT COUNT(*) ft_refill_EXIST FROM MON.SPARK_ft_refill WHERE REFILL_DATE='###SLICE_VALUE###') B,
(SELECT COUNT(*) IT_ZEBRA_MASTER_BALANCE_EXIST FROM CDR.SPARK_IT_ZEBRA_MASTER_BALANCE WHERE EVENT_DATE='###SLICE_VALUE###') C