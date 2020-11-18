INSERT INTO CDR.SPARK_IT_CRM_ABONNEMENT_HIERARCH
SELECT
MSISDN,
ID_CLIENT_B2B_CRM,
ID_PARENT_B2B_CRM,
ID_RACINE_B2B_CRM,
NIVEAU,
ORIGINAL_FILE_NAME,
CURRENT_TIMESTAMP INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -17, 6),'ddMMyy'))) ORIGINAL_FILE_DATE
FROM CDR.TT_CRM_ABONNEMENT_HIERARCH C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_BDI_CRM_B2B where  ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,2) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL