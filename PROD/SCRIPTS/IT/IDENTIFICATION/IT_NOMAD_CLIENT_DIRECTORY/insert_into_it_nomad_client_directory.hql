 INSERT INTO cdr.it_nomad_client_directory PARTITION(original_file_date)
SELECT
       numero,
       typedecontrat,
       source,
       telephone,
       gender,
       titre,
       prenomduclient,
       nomduclient,
       datedenaissance,
       lieudenaissance,
       piece,
       numeropiece,
       delivrance,
       expiration,
       nationalite,
       quartier,
       ville,
       loginvendeur,
       prenomvendeur,
       nomvendeur,
       numeroduvendeur,
       logindistributeur,
       prenomdistributeur,
       nomdistributeur,
       emisle,
       majle,
       loginmaj,
       prenommaj,
       nommaj,
       etat,
       etatdexportglobal,
       loginvalidateur,
       prenomvalidateur,
       nomvalidateur,
       causeechec,
       commentaire,
       pwdclient,
       last_update_date,
       delivrance1,
       lieudedelivrance,
       copie,
       update_on,
       original_file_name,
       original_file_size,
       original_file_line_count,
       CURRENT_TIMESTAMP() INSERT_DATE,
       To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd')))    ORIGINAL_FILE_DATE
FROM   cdr.tt_nomad_client_directory C
       LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   cdr.it_nomad_client_directory) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL;