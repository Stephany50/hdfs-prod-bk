SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT > 0 and C.FT_COUNT > 0 , "OK", "NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_STKAPRIM_GLOBAL_REPORT WHERE REFILL_DATE = '###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_RECHARGE_BY_RETAILER WHERE REFILL_DATE ='###SLICE_VALUE###') B,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_RECHARGE_BY_RETAILER_MONTH WHERE EVENT_MONTH = SUBSTR(ADD_MONTHS('###SLICE_VALUE###',-1),1,7)) C