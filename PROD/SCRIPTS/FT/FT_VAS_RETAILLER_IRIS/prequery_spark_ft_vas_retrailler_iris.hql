SELECT IF(T_1.FT_EXIST = 0 and T_2.IT_TABLE>0 AND T_3.IT_FILE_NAME=2 ,"OK","NOK") FT_IMEI_ONLINE_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_VAS_RETAILLER_IRIS WHERE EVENT_DATE = '###SLICE_VALUE###'  ) T_1,
(SELECT COUNT(*) IT_TABLE FROM CDR.SPARK_IT_VAS_RETAILLER_IRIS  WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_2,
(SELECT COUNT(distinct original_file_name) IT_FILE_NAME FROM CDR.SPARK_IT_VAS_RETAILLER_IRIS  WHERE TRANSACTION_DATE = '###SLICE_VALUE###') T_3

