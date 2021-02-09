SELECT
NVL(src,'') src,
NVL(cra_src,'') cra_src,
NVL(heure,'') heure,
NVL(faisceau,'') faisceau,
NVL(usage_appel,'') usage_appel,
NVL(indication_appel,'') indication_appel,
NVL(type_appel,'') type_appel,
NVL(type_abonne,'') type_abonne,
NVL(destination_appel,'') destination_appel,
NVL(type_heure,'') type_heure,
NVL(nbre_appel,'') nbre_appel,
NVL(duree_appel,'')  duree_appel,
NVL(inserted_date ,'') inserted_date ,
NVL(operator_code,'')  operator_code,
NVL(msc_location,'')  msc_location,
NVL(sdate,'')    sdate
FROM AGG.SPARK_FT_X_INTERCO_FINAL
WHERE SDATE ='###SLICE_VALUE###'