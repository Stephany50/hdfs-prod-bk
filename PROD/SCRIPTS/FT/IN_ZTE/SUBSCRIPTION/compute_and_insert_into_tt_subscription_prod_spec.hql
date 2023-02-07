insert into tt.subscription_prod_spec
SELECT 
    STD_CODE, 
    MIN(PROD_SPEC_NAME) PROD_SPEC_NAME
FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT
WHERE ORIGINAL_FILE_DATE = '###SLICE_VALUE###'
GROUP BY STD_CODE