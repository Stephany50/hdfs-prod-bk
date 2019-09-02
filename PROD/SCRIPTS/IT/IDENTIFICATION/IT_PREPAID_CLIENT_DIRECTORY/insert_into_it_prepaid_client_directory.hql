 INSERT INTO cdr.it_prepaid_client_directory PARTITION(original_file_date)
SELECT
       ident_id,
       numero_tel,
       nom,
       prenom,
       nee_le,
       nee_a,
       profession,
       quartier_residence,
       ville_village,
       cni,
       cni_valide_de,
       cni_valide_a,
       etablie_a,
       numero_autre_1,
       numero_autre_2,
       indate,
       outdate,
       identification_status,
       utilisateur,
       type_document,
       numero_document,
       fichier_chargement,
       source_platform,
       activation_status,
       status_icc,
       status_hlr_entrant,
       status_hlr_sortant,
       image_file,
       original_file_name,
       original_file_size,
       original_file_line_count,
       CURRENT_TIMESTAMP() INSERT_DATE,
       To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd')))  ORIGINAL_FILE_DATE

FROM   cdr.tt_prepaid_client_directory C
       LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   cdr.it_prepaid_client_directory) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;
