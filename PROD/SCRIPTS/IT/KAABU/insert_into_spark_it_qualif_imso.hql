INSERT INTO cdr.spark_it_qualif_imso
SELECT
       agent_terrain,
       loginBackoffice,
       date_emission,
       msisdn,
       etat,
       commentaire,
       original_file_name,
       CURRENT_TIMESTAMP() INSERT_DATE,
       original_file_size,
       original_file_line_count,
       To_date(date_emission) event_date,
	FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -18, 8),'yyyyMMdd')) ORIGINAL_FILE_DATE
FROM   cdr.tt_qualif_imso C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_QUALIF_IMSO WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL