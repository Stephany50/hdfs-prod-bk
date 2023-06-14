CREATE TABLE CDR.SPARK_IT_PARRAINAGE_ACCESSPRO (
numero_comando VARCHAR(15),
numero_migre VARCHAR(15),
date_migration TIMESTAMP,
offre_migree VARCHAR(100),
--Colonnes à ajouter
ORIGINAL_FILE_NAME VARCHAR(50), 
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP) 
--Fin
PARTITIONED BY (original_file_date DATE) 
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 