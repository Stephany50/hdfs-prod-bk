SELECT IF(A.FT_EXIST = 0 and B.FT_COUNT>0 , "OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_MARKETING_DATAMART WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_COUNT FROM MON.SPARK_FT_CLIENT_360 WHERE EVENT_DATE='###SLICE_VALUE###') B



