SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT > 0 and C.FT_COUNT > 0, "OK", "NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_RECHARGE_BY_RETAILER WHERE REFILL_DATE = '###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE FILE_DATE ='###SLICE_VALUE###') B,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_REFILL WHERE FILE_DATE ='###SLICE_VALUE###') C