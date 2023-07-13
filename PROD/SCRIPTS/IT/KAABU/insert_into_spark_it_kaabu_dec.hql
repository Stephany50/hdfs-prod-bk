INSERT INTO CDR.SPARK_IT_KAABU_CLIENT_DEC
SELECT
    NUMERO_DOSSIER , 
    STATUS_ENTREE  , 
    STATUS_SORTIE , 
    PLATEAU   , 
    REMONTEE_DOSSIER   , 
    DATE_PREMIER_TRAITEMENT , 
    TELEPHONE , 
    DATE_DERNIER_TRAITEMENT , 
    original_file_name ,
    original_file_size ,
    original_file_line_count ,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (original_file_name, 11, 8),'yyyyMMdd'))) original_file_date,
    to_date(DATE_PREMIER_TRAITEMENT) date_controle
FROM   CDR.tt_kaabu_client_dec  C
    LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM  CDR.SPARK_IT_KAABU_CLIENT_DEC) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL