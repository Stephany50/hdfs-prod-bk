
INSERT INTO  MON.MISSING_FILES
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_SUBS_EXTRACT' FLUX_NAME,CONCAT('Data_etract_SUBS','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_SUBS_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_PROD_SPEC_EXTRACT' FLUX_NAME,CONCAT('Data_etract_PROD_SPEC','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_PROD_SPEC_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_PROD_EXTRACT' FLUX_NAME,CONCAT('Data_etract_PROD','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_PROD_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_PRICE_PLAN_EXTRACT' FLUX_NAME,CONCAT('Data_etract_PRICE_PLAN','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_PRICE_PLAN_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_CUST_EXTRACT' FLUX_NAME,CONCAT('Data_etract_CUST','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_CUST_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_BAL_EXTRACT' FLUX_NAME,CONCAT('Data_etract_BAL','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_BAL_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_ACCT_EXTRACT' FLUX_NAME,CONCAT('Data_etract_ACCT','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_ACCT_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_ACC_NBR_EXTRACT' FLUX_NAME,CONCAT('Data_etract_ACC_NBR','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_ACC_NBR_EXTRACT B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_DEL_EXPBAL' FLUX_NAME,CONCAT('Del_ExpBal','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_DEL_EXPBAL B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_BAL_SNAP' FLUX_NAME,CONCAT('bal_pr-ocs21','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','dat') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_BAL_SNAP B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
UNION
SELECT C.* FROM (SELECT 'IN' TABLE_SOURCE,'EXTRACT' FLUX_TYPE,'ZTE_PROFILE_CDR' FLUX_NAME,CONCAT('Profile','_',DATE_FORMAT('###SLICE_VALUE###','YYYYMMdd'),'.','csv') FILE_NAME, CURRENT_TIMESTAMP INSERT_DATE,'###SLICE_VALUE###' ORIGINAL_FILE_DATE )C
LEFT JOIN (SELECT DISTINCT ORIGINAL_FILE_NAME FROM CDR.SPARK_IT_ZTE_PROFILE B WHERE ORIGINAL_FILE_DATE='###SLICE_VALUE###') B ON SUBSTRING(B.ORIGINAL_FILE_NAME,-12,8) = SUBSTRING(C.FILE_NAME,-12,8)
WHERE B.ORIGINAL_FILE_NAME IS NULL
