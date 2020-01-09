SELECT IF(T_1.FT_EXIST = 0 and T_2.Hold_IT_OM_APGL>0 ,"OK","NOK") FT_OM_APGL_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_OM_APGL_TRANSACTION WHERE DOCUMENT_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) Hold_IT_OM_APGL FROM CDR.SPARK_IT_OM_APGL WHERE original_file_date = '###SLICE_VALUE###') T_2

