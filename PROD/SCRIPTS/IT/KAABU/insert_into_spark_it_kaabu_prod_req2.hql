INSERT INTO cdr.spark_it_kaabu_client_directory_req2 
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
       last_update_date ,
       delivrance1,
       lieudedelivrance,
       copie,
       update_on,
       id,
       picture_loaded_date,
       source_donnees,
       typeoperation,
       original_file_name,
       original_file_size,
       original_file_line_count,
       CURRENT_TIMESTAMP() INSERT_DATE,
       TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (original_file_name, 17, 8),'yyyyMMdd'))) original_file_date,
       to_date(date_controle) date_controle
FROM   cdr.tt_kaabu_client_directory_req2 C
       LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM   cdr.spark_it_kaabu_client_directory_req2) T ON T.file_name = C.original_file_name
WHERE  T.file_name IS NULL