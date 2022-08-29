SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT>0 and C.FT_COUNT>0 and D.FT_COUNT>0 and E.FT_COUNT>0 , "OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CBM_DATAMART_FORFAIT WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_CBM_CUST_INSIGTH_DAILY WHERE PERIOD='###SLICE_VALUE###') B,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_VAS_RETAILLER_IRIS WHERE SDATE='###SLICE_VALUE###') C,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_EMERGENCY_DATA WHERE transaction_date='###SLICE_VALUE###') D,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_SUBSCRIPTION WHERE transaction_date='###SLICE_VALUE###') E




