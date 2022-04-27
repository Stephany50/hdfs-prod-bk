INSERT INTO cdr.spark_it_graph 
SELECT
    C.original_file_name,
    msisdn,
    name,
    CURRENT_TIMESTAMP() INSERT_DATE,
    To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, 7, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_GRAPH C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM cdr.spark_it_graph) T ON T.file_name = C.original_file_name
WHERE T.file_name IS NULL

