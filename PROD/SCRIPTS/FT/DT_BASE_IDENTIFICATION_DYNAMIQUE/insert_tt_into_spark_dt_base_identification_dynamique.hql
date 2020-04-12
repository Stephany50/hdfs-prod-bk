INSERT INTO DIM.SPARK_DT_BASE_IDENTIFICATION_DYNAMIQUE

SELECT
    MSISDN MSISDN,
    NOM NOM,
    PRENOM PRENOM,
    NEE_LE NEE_LE,
    NEE_A NEE_A,
    PROFESSION PROFESSION,
    QUARTIER_RESIDENCE QUARTIER_RESIDENCE,
    VILLE_VILLAGE VILLE_VILLAGE,
    CNI CNI,
    DATE_IDENTIFICATION DATE_IDENTIFICATION,
    TYPE_DOCUMENT TYPE_DOCUMENT,
    FICHIER_CHARGEMENT FICHIER_CHARGEMENT,
    DATE_INSERTION DATE_INSERTION,
    EST_SNAPPE EST_SNAPPE,
    IDENTIFICATEUR IDENTIFICATEUR,
    DATE_MISE_A_JOUR DATE_MISE_A_JOUR,
    DATE_TABLE_MIS_A_JOUR DATE_TABLE_MIS_A_JOUR,
    GENRE GENRE,
    CIVILITE CIVILITE,
    TYPE_PIECE_IDENTIFICATION TYPE_PIECE_IDENTIFICATION,
    PROFESSION_IDENTIFICATEUR PROFESSION_IDENTIFICATEUR,
    MOTIF_REJET MOTIF_REJET,
    DATE_DEBUT_VALIDITE DATE_DEBUT_VALIDITE,
    IF(LEAD(DATE_DEBUT_VALIDITE, 1, NULL) OVER (PARTITION BY MSISDN ORDER BY DATE_DEBUT_VALIDITE ASC) IS NULL, NULL, DATE_SUB(LEAD(DATE_DEBUT_VALIDITE, 1, NULL) OVER (PARTITION BY MSISDN ORDER BY DATE_DEBUT_VALIDITE ASC), 1)) DATE_FIN_VALIDITE,
    IF(LEAD(DATE_DEBUT_VALIDITE, 1, NULL) OVER (PARTITION BY MSISDN ORDER BY DATE_DEBUT_VALIDITE ASC) IS NULL, "TRUE", "FALSE") EST_ACTIF
FROM TMP.SPARK_TT_DT_BASE_IDENTIFICATION_DYNAMIQUE
GROUP BY
    MSISDN,
    NOM,
    PRENOM,
    NEE_LE,
    NEE_A,
    PROFESSION,
    QUARTIER_RESIDENCE,
    VILLE_VILLAGE,
    CNI,
    DATE_IDENTIFICATION,
    TYPE_DOCUMENT,
    FICHIER_CHARGEMENT,
    DATE_INSERTION,
    EST_SNAPPE,
    IDENTIFICATEUR,
    DATE_MISE_A_JOUR,
    DATE_TABLE_MIS_A_JOUR,
    GENRE,
    CIVILITE,
    TYPE_PIECE_IDENTIFICATION,
    PROFESSION_IDENTIFICATEUR,
    MOTIF_REJET,
    DATE_DEBUT_VALIDITE;
;