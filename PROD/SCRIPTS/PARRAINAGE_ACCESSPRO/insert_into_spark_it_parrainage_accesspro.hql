INSERT INTO CDR.SPARK_IT_PARRAINAGE_ACCESSPRO 
SELECT
numero_comando,
numero_migre,
date_migration,
offre_migree,
original_file_name,
original_file_size,
original_file_line_count,
CURRENT_TIMESTAMP() INSERT_DATE,
To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_PARRAINAGE_ACCESSPRO  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_PARRAINAGE_ACCESSPRO ) T 
ON T.file_name = C.original_file_name 
WHERE T.file_name IS NULL