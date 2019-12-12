SELECT IF(T_1.FT_EXIST = 0 and T_2.PROD_SPEC_EXTRACT_EXIST>0 and T_3.PRICE_PLAN_EXTRACT_EXIST>0,"OK","NOK") EXTRACT_EXIST
FROM
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_SUBSCRIPTION WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) PROD_SPEC_EXTRACT_EXIST FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_2,
(SELECT COUNT(*) PRICE_PLAN_EXTRACT_EXIST FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') T_3
