INSERT INTO CDR.SPARK_IT_RIGHTQ_TRANSACTIONS PARTITION(original_file_date)
SELECT

    ORIGINAL_FILE_NAME,
    ORIGINAL_FILE_SIZE,
    ORIGINAL_FILE_LINE_COUNT,
    Agence,
    Ticket,
    Service,
    Client,
    Mobile,
    Arrivee,
    Attente,
    Service,
    Note,
    Commentaire,
    Status,
    Utilisateur,
    Updated_tickets),
    email,
    CURRENT_TIMESTAMP() INSERT_DATE,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE


FROM CDR.TT_RIGHTQ_TRANSACTIONS A
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_RIGHTQ_TRANSACTIONS WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,${hivevar:date_offset}) AND CURRENT_DATE )B
ON  A.ORIGINAL_FILE_NAME = B.FILE_NAME
WHERE B.FILE_NAME IS NULL