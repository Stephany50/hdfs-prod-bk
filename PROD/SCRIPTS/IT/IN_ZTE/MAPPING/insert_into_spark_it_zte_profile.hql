INSERT INTO CDR.SPARK_IT_ZTE_PROFILE PARTITION(original_file_date)
SELECT
    profile_id
    ,profile_name 
    ,price_plan_code 
    ,rtrim(std_code)
    ,CURRENT_TIMESTAMP insert_date
    ,original_file_name 
    ,TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) original_file_date
FROM CDR.TT_ZTE_PROFILE  C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_ZTE_PROFILE WHERE insert_date BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL