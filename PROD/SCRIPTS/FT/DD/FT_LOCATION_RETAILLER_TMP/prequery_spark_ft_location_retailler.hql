SELECT IF(T_1.FT_COUNT = 0 AND T_2.FT_REFILL > 1 AND T_3.FT_REFILL > 1 AND T_4.FT_CLIENT_LAST_SITE_DAY > 1 AND T_5.FT_SUBSCRIPTION > 1,"OK","NOK") FT_EXIST
FROM
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_LOCATION_RETAILLER_TMP WHERE REFILL_DATE= '###SLICE_VALUE###') T_1,
(SELECT COUNT(*) FT_REFILL FROM MON.SPARK_FT_REFILL WHERE REFILL_DATE ='###SLICE_VALUE###' AND REFILL_MEAN = 'C2S')T_2,
(SELECT COUNT(*) FT_REFILL FROM MON.SPARK_FT_REFILL WHERE REFILL_DATE ='###SLICE_VALUE###' AND REFILL_MEAN = 'SCRATCH')T_3,
(SELECT COUNT(*) FT_CLIENT_LAST_SITE_DAY FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE ='###SLICE_VALUE###')T_4,
(SELECT COUNT(*) FT_SUBSCRIPTION FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE ='###SLICE_VALUE###')T_5
