SELECT IF(T_1.FT_EXIST = 0 and T_2.Hold_FT_OTARIE_DATA_TRAFFIC_DAY>0 ,"OK","NOK") FT_IMEI_ONLINE_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_IMEI_ONLINE WHERE SDATE = '###SLICE_VALUE###' AND SRC_TABLE = 'MSC|' ) T_1,
(SELECT COUNT(*) Hold_FT_OTARIE_DATA_TRAFFIC_DAY FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY WHERE TRANSACTION_DATE = DATE_SUB('###SLICE_VALUE###',1)) T_2
