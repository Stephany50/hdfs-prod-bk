SELECT IF(A.FT_EXIST = 0 AND B.FT_EXIST_1 >0 AND C.FT_EXIST_2 >0 ,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CXD_RECONCIALIATION_AFM WHERE EVENT_DATE = '###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_EXIST_1 FROM cdr.spark_it_omny_transactions where transfer_datetime ='###SLICE_VALUE###') B,
(SELECT COUNT(*) FT_EXIST_2 FROM MON.SPARK_FT_CXD_RECONCIALIATION_AFM WHERE EVENT_DATE = DATE_SUB('###SLICE_VALUE###',1) )C