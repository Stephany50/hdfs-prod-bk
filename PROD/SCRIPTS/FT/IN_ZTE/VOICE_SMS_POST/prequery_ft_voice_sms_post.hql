SELECT IF(A.FT_EXIST = 0 and B.PRICE_PLAN_EXTRACT_EXIST>0,"OK","NOK") FROM
(SELECT COUNT(*) FT_EXIST FROM MON.FT_BILLED_TRANSACTION_POSTPAID WHERE TRANSACTION_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) PRICE_PLAN_EXTRACT_EXIST FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###')B

