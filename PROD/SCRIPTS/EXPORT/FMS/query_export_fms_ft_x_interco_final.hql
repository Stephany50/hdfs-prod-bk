SELECT
NVL(src,'') src,
NVL(cra_src,'') cra_src,
NVL(date_format(sdate,'dd/MM/yyyy'),'') sdate,
NVL(heure,'') heure,
CASE
WHEN UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('Camtel National')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('CAMTEL')) THEN 'Camtel National'
WHEN UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('Orange France')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('FTLD')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('IC')) THEN 'Orange France'
WHEN UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('BELG')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('BICS')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('Belgacom International Carrier Services')) THEN 'BELG'
WHEN UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('Orange CI')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('OCI')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('Orange Côte d\'Ivoire')) THEN 'Orange CI'
WHEN UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('SYBASE')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('SINCH')) THEN 'SYBASE'
WHEN UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('NEXTTEL')) or UPPER(TRIM(NVL(faisceau,''))) = UPPER(TRIM('VIETTEL')) THEN 'VIETTEL'
END faisceau,
NVL(usage_appel,'') usage_appel,
NVL(indication_appel,'') indication_appel,
NVL(type_appel,'') type_appel,
NVL(type_abonne,'') type_abonne,
NVL(destination_appel,'') destination_appel,
NVL(type_heure,'') type_heure,
NVL(nbre_appel,'') nbre_appel,
NVL(duree_appel,'')  duree_appel,
NVL(date_format(inserted_date,'dd/MM/yyyy HH:mm:ss') ,'') inserted_date,
NVL(operator_code,'')  operator_code,
NVL(msc_location,'')  msc_location
FROM AGG.SPARK_FT_X_INTERCO_FINAL
WHERE SDATE ='###SLICE_VALUE###'