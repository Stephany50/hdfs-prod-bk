INSERT INTO cdr.spark_it_btl_report 
SELECT
    msisdn,
    transaction_date,
    type_forfait,
    msisdn_vendeur, 
    original_file_name,
    original_file_size,
    original_file_line_count,
    CURRENT_TIMESTAMP() INSERT_DATE,
    To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM cdr.tt_btl_report  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM cdr.tt_btl_report) T ON T.file_name = C.original_file_name
WHERE T.file_name IS NULL
