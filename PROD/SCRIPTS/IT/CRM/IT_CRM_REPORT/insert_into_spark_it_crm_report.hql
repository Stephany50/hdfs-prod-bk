INSERT INTO cdr.spark_it_crm_reporting 
SELECT
    TicketNumber,
    onc_Numeroappelant,
    fileAttente,
    Date_Interaction,
    categorie,
    typarticle,
    article,
    motif,
    Agent,
    CUID_AGENT,
    comment_interact,
    description_ticket,
    original_file_name,
    original_file_size,
    original_file_line_count,
    CURRENT_TIMESTAMP() INSERT_DATE,
    To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.tt_crm_report C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM cdr.spark_it_crm_reporting) T ON T.file_name = C.original_file_name
WHERE T.file_name IS NULL
