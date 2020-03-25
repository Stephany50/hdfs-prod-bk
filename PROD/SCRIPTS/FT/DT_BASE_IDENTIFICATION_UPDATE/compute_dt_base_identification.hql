-- 2. Mise à jour des identifications extantes et ajout des nouvelles pour les MSISDN Correctes issues de SNAPID

INSERT INTO TT.SPARK_DT_BASE_IDENTIFICATION_2
SELECT

    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.NOM, b.NOM), b.NOM) NOM,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.PRENOM, b.PRENOM), b.PRENOM) PRENOM,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.NEE_LE, b.NEE_LE), b.NEE_LE) NEE_LE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.NEE_A, b.NEE_A), b.NEE_A) NEE_A,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.PROFESSION, b.PROFESSION), b.PROFESSION) PROFESSION,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.QUARTIER_RESIDENCE, b.QUARTIER_RESIDENCE), b.QUARTIER_RESIDENCE) QUARTIER_RESIDENCE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.VILLE_VILLAGE, b.VILLE_VILLAGE), b.VILLE_VILLAGE) VILLE_VILLAGE,
    nvl(a.CNI, b.CNI) CNI,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.DATE_IDENTIFICATION, b.DATE_IDENTIFICATION), b.DATE_IDENTIFICATION) DATE_IDENTIFICATION,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.TYPE_DOCUMENT, b.TYPE_DOCUMENT), b.TYPE_DOCUMENT) TYPE_DOCUMENT,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.FICHIER_CHARGEMENT, b.FICHIER_CHARGEMENT), b.FICHIER_CHARGEMENT) FICHIER_CHARGEMENT,
    CURRENT_TIMESTAMP,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.EST_SNAPPE, b.EST_SNAPPE), b.EST_SNAPPE) EST_SNAPPE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.IDENTIFICATEUR, b.IDENTIFICATEUR), b.IDENTIFICATEUR) IDENTIFICATEUR,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, NULL, CURRENT_TIMESTAMP) DATE_MISE_A_JOUR,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, NULL, CURRENT_TIMESTAMP) DATE_TABLE_MIS_A_JOUR,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.GENRE, b.GENRE), b.GENRE) GENRE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.CIVILITE, b.CIVILITE), b.CIVILITE) CIVILITE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.TYPE_PIECE_IDENTIFICATION, b.TYPE_PIECE_IDENTIFICATION), b.TYPE_PIECE_IDENTIFICATION) TYPE_PIECE_IDENTIFICATION,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL, nvl(a.PROFESSION_IDENTIFICATEUR, b.PROFESSION_IDENTIFICATEUR), b.PROFESSION_IDENTIFICATEUR) PROFESSION_IDENTIFICATEUR,
    nvl(a.MOTIF_REJET, b.MOTIF_REJET) MOTIF_REJET

FROM
TT.SPARK_DT_BASE_IDENTIFICATION A
FULL OUTER JOIN
(
    SELECT MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE,
        VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, EST_SNAPPE, IDENTIFICATEUR
        , GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
    FROM TT.SPARK_TT_IDENTIFICATION_MSISDN_2
    WHERE FN_GET_OPERATOR_CODE(MSISDN)  IN ('OCM', 'SET') AND CASE WHEN CAST(MSISDN AS INT) IS NULL THEN 'N' ELSE 'Y' END  = 'Y'
) B
ON a.MSISDN = b.MSISDN