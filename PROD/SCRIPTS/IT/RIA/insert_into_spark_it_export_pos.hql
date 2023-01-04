INSERT INTO cdr.spark_it_export_pos 
SELECT
  id ,Name ,Status ,Latitude ,
  Longitude ,CreatedAt ,UpdatedAt ,
  Secteur ,Zone_PMO ,Region_administrative ,
  Region_commerciale ,Type_PDV ,Sous_type  ,
  Nom_propre ,Contact_Vendeur ,Numeros_eRecharge ,
  Numeros_Nomad ,Numeros_Orange_Money ,Dual_Wallet ,
  Localisation_precise ,Geolocalisation ,Produits_Orange ,
  Produits_MTN ,Produits_Nexttel ,Produits_CAMTEL ,
  Produit_YOOMEE ,Produits_SGC ,Autres_concurrents ,
  Visibilite_Orange ,Visibilite_MTN ,Visibilite_Nexttel ,
  UserId ,CreatedBy ,UpdatedBy,
  CURRENT_TIMESTAMP() INSERT_DATE,
  ORIGINAL_FILE_NAME,
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  TO_DATE(SUBSTRING (ORIGINAL_FILE_NAME, -14, 10)) ORIGINAL_FILE_DATE
FROM cdr.tt_export_pos C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM cdr.spark_it_export_pos WHERE ORIGINAL_FILE_DATE BETWEEN DATE_SUB(CURRENT_DATE,3) AND CURRENT_DATE )T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL
