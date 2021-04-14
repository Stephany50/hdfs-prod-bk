SELECT IF(A.FT_EXIST = 0 and B.PRICE_PLAN_EXTRACT_EXIST>0 AND C.PROFILE_EXIST>0,"OK","NOK") FROM
(SELECT count(*) FT_EXIST FROM MON.SPARK_FT_CRA_GPRS WHERE SESSION_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) PRICE_PLAN_EXTRACT_EXIST FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###')B,
(SELECT COUNT(*) PROFILE_EXIST FROM CDR.SPARK_IT_ZTE_PROFILE WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###')C
