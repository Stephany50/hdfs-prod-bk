INSERT INTO CDR.SPARK_DT_RATING_EVENT
SELECT
    rating_event_id
    ,rating_event_name
    ,rating_event_zone
    ,rating_event_operator
    ,rating_event_service
    ,rating_event_specific_tarif
    ,rtrim(rating_event_spec_tarif_desc)
    ,CURRENT_TIMESTAMP insert_date
    ,original_file_name
    ,TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) original_file_date
FROM CDR.TT_RATING_EVENT  C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_DT_RATING_EVENT WHERE insert_date BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL