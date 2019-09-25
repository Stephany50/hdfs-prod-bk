 INSERT INTO cdr.it_client_snapid_directory PARTITION(original_file_date)
SELECT
       msisdn,
       pwdclient,
       From_unixtime(Unix_timestamp(lastmod,'yyyy-MM-dd HH:mm:ss')) lastmod,
       nom,
       prenom,
       datenaissance,
       lieunaissance,
       genre,
       civilite,
       typepieceidentification,
       idpieceidentification,
       datedelivrance,
       lieudelivrance,
       dateexpiration,
       ville,
       quartier,
       source,
       copie,
       datecreation,
       datedernieremodif,
       seller_msisdn,
       profession,
       date_capture,
       original_file_name,
       original_file_size,
       original_file_line_count,
       CURRENT_TIMESTAMP() INSERT_DATE,
       To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd')))   ORIGINAL_FILE_DATE
FROM   cdr.tt_client_snapid_directory C
       LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   cdr.it_client_snapid_directory) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;  