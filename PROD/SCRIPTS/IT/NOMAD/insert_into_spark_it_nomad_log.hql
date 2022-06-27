INSERT INTO cdr.spark_it_nomad_log
SELECT
    version,
    action_type,
    comment,
    is_creation,
    remote_address,
    update_date,
    updater_id,
    class,
    user_id,
    contract_id,
    success,
    original_file_name,
    original_file_size,
    original_file_line_count,
    CURRENT_TIMESTAMP() INSERT_DATE,
    To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM cdr.tt_nomad_log  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM cdr.spark_it_nomad_log ) T ON T.file_name = C.original_file_name
WHERE T.file_name IS NULL
