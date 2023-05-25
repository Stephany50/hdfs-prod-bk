INSERT INTO CDR.SPARK_IT_PARRAINAGE_OMA 
SELECT
id,
status,
date_action,
remark,
msisdn_parrain,
msisdn_filleul,
type_parrainage,
canal_parrainage,
parrain_is_commando,
date_parrainage,
original_file_name,
original_file_size,
original_file_line_count,
CURRENT_TIMESTAMP() INSERT_DATE,
To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM cdr.tt_PARRAINAGE_OMA  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_PARRAINAGE_OMA ) T ON T.file_name = C.original_file_name
WHERE T.file_name IS NULL