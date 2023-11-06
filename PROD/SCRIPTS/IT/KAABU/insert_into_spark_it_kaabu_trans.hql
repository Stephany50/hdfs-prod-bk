INSERT INTO cdr.spark_it_kaabu_transactions
SELECT
    TRANSFER_ID,
    CANAL,
    EXTRA_DATA,
    LIBELLE_A_AFFICHER,
    AGENT_NUMBER,
    NUMERO_CLIENT,
    TRANSACTION_AMOUNT,
    TRANSFER_STATUS,
    TYPE_OP,
    SOURCE,
    LOGIN,
    Id,
    TRANSFER_DATETIME,
    original_file_name,
    original_file_size,
    original_file_line_count,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (original_file_name, 13, 8),'yyyyMMdd'))) original_file_date,
    to_date(TRANSFER_DATETIME) TRANSFER_DATE
FROM   cdr.spark_tt_kaabu_transactions C
       LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   cdr.spark_it_kaabu_transactions) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL
