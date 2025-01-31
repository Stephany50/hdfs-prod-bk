-- 1. Extraction des donnees d'identification les plus recentes pour chaque MSISDN
-- Donnees base NSTOOL
INSERT  INTO TT.SPARK_TT_IDENTIFICATION_MSISDN
SELECT MSISDN,
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
    CURRENT_TIMESTAMP AS DATE_INSERTION,
    'NON' AS EST_SNAPPE,
    IDENTIFICATEUR,
    NULL AS GENRE,
    NULL AS CIVILITE,
    NULL AS TYPE_PIECE_IDENTIFICATION,
    NULL AS PROFESSION_IDENTIFICATEUR
FROM
(
    SELECT
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
        IDENTIFICATEUR,
        TYPE_DOCUMENT,
        FICHIER_CHARGEMENT,
        ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY DATE_IDENTIFICATION DESC) AS RANG
    FROM
    (
        SELECT
            MSISDN,
            NOM,
            PRENOM,
            NVL(FROM_UNIXTIME(UNIX_TIMESTAMP(NEE_LE,'yyyyMMdd HH:mm:ss')),
                FROM_UNIXTIME(UNIX_TIMESTAMP(NEE_LE,'dd/MM/yy')))  NEE_LE,
            NEE_A,
            PROFESSION,
            QUARTIER_RESIDENCE,
            UPPER(VILLE_VILLAGE) AS VILLE_VILLAGE,
            CNI,
            INDATE AS DATE_IDENTIFICATION,
            FN_FORMAT_MSISDN_TO_9DIGITS(IF(REGEXP_REPLACE(UTILISATEUR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(UTILISATEUR, "^237+(?!$)","")))  IDENTIFICATEUR,
            TYPE_DOCUMENT,
            FICHIER_CHARGEMENT
        FROM (SELECT * FROM CDR.SPARK_IT_PREPAID_CLIENT_DIRECTORY WHERE  ORIGINAL_FILE_DATE in (select original_file_date AS T  from CDR.SPARK_IT_PREPAID_CLIENT_DIRECTORY GROUP BY original_file_date ORDER BY original_file_date DESC limit 1)) TY
        LATERAL VIEW POSEXPLODE(SPLIT(NVL(NUMERO_TEL,''), ' ')) tmp1 AS index, MSISDN
        
    )T1
    WHERE MSISDN IS NOT NULL
)T
WHERE RANG = 1