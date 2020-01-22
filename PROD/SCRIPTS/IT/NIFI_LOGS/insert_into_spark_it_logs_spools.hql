INSERT INTO CDR.SPARK_IT_LOGS_SPOOLS PARTITION (LOG_DATE)
SELECT
    FILENAME,
    FILESIZE,
    FILECOUNT,
    FILETYPE,
    MERGED_FILENAME,
    FLUXTYPE,
    PROVENANCE,
    STATUS,
    FROM_UNIXTIME(CAST(LOG_DATETIME/1000 AS BIGINT)) LOG_DATETIME,
    REGEXP_REPLACE(REGEXP_REPLACE(FLOWFILE_ATTR, "\\.", "_"), "%7C", "|") FLOWFILE_ATTR,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(CAST(LOG_DATETIME/1000 AS BIGINT))) LOG_DATE
FROM CDR.TT_LOGS_SPOOLS