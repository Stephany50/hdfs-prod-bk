INSERT INTO CDR.SPARK_IT_BACKUPS
SELECT
MSISDN,
BACKUP_MSISDN,
FROM_UNIXTIME(UNIX_TIMESTAMP(CREATED_AT,'yyyy-MM-dd HH:mm:ss')) CREATED_AT,
FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_UPDATE_AT,'yyyy-MM-dd HH:mm:ss')) LAST_UPDATE_AT,
LAST_UPDATE_CANAL,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
CURRENT_TIMESTAMP  INSERTED_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(LAST_UPDATE_AT,'yyyy-MM-dd HH:mm:ss'))) DATE_LAST_UPDATE_AT,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd'))) FILE_DATE
--BACKUPS_20230704000000.csv
FROM TT.BACKUPS C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_BACKUPS 
WHERE FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE
-- A UTILISER QUAND LE PROCESS SERA STABLE CAR IL Y A DES FICHIERS DE JUIN AVEC LE NOM 20220610 QUI PEUVENT ENCORE ETRE COLLECTE 2 FOIS
) T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL