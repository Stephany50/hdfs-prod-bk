select 
MSISDN, 
NUMERO_PIECE, 
ORIGINAL_FILE_DATE 
FROM CDR.SPARK_IT_KYC_BDI_FULL 
WHERE ORIGINAL_FILE_DATE = DATE_SUB(CURRENT_DATE, 3)