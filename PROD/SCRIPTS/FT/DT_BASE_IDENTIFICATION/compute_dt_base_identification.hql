set hive.tez.container.size=8192;
set hive.tez.java.opts=-Xmx6553m;
set tez.runtime.io.sort.mb=3276;
set tez.runtime.io.sort.mb=819;

add jar hdfs:///PROD/UDF/hive-udf-1.0.jar;
create temporary function FN_FORMAT_MSISDN_TO_9DIGITS as 'cm.orange.bigdata.udf.FormatMsisdnTo9Digits';
create temporary function FN_GET_OPERATOR_CODE as 'cm.orange.bigdata.udf.GetOperatorCode';
        


TRUNCATE TABLE DATALAB.TT_IDENTIFICATION_MSISDN;
-- 1. Extraction des donnees d'identification les plus recentes pour chaque MSISDN
-- Donnees base NSTOOL
INSERT  INTO DATALAB.TT_IDENTIFICATION_MSISDN
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
                FROM_UNIXTIME(UNIX_TIMESTAMP(NEE_LE,'dd/mm/yy')))  NEE_LE,
            NEE_A,
            PROFESSION,
            QUARTIER_RESIDENCE,
            UPPER(VILLE_VILLAGE) AS VILLE_VILLAGE,
            CNI,
            INDATE AS DATE_IDENTIFICATION,
            FN_FORMAT_MSISDN_TO_9DIGITS(IF(REGEXP_REPLACE(UTILISATEUR, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(UTILISATEUR, "^237+(?!$)","")))  IDENTIFICATEUR,
            TYPE_DOCUMENT,
            FICHIER_CHARGEMENT
        FROM DATALAB.IT_PREPAID_CLIENT_DIRECTORY
        LATERAL VIEW POSEXPLODE(SPLIT(NVL(NUMERO_TEL,''), ' ')) tmp1 AS index, MSISDN
        WHERE  ORIGINAL_FILE_DATE =DATE_SUB('2019-08-02',-1)
    )T1
    WHERE MSISDN IS NOT NULL
)T
WHERE RANG = 1;


-- Donnees base SNAP ID
MERGE INTO DATALAB.TT_IDENTIFICATION_MSISDN a
USING
(
    SELECT MSISDN, NOM, PRENOM, NEE_LE,NEE_A, PROFESSION, QUARTIER_RESIDENCE, VILLE_VILLAGE,
         CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, DATE_INSERTION, EST_SNAPPE,
         IDENTIFICATEUR, GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
    FROM
    (
        SELECT
            MSISDN,
            UPPER(NOM) AS NOM,
            UPPER(PRENOM) AS PRENOM,
            NVL(FROM_UNIXTIME(UNIX_TIMESTAMP(DATENAISSANCE,'yyyyMMdd HH:mm:ss')),
                FROM_UNIXTIME(UNIX_TIMESTAMP(DATENAISSANCE,'dd/mm/yy'))) NEE_LE,
            UPPER(LIEUNAISSANCE) AS NEE_A,
            NULL AS PROFESSION,
            UPPER(QUARTIER) AS QUARTIER_RESIDENCE,
            UPPER(VILLE) AS VILLE_VILLAGE,
            IDPIECEIDENTIFICATION AS CNI,
            LASTMOD AS DATE_IDENTIFICATION,
            NULL AS TYPE_DOCUMENT,
            UPPER(SOURCE) AS FICHIER_CHARGEMENT,
            CURRENT_TIMESTAMP AS DATE_INSERTION,
            'NON' AS EST_SNAPPE,
            IF(REGEXP_REPLACE(SELLER_MSISDN, "^237+(?!$)","")='237',NULL,REGEXP_REPLACE(SELLER_MSISDN, "^237+(?!$)","")) AS IDENTIFICATEUR,
            UPPER(GENRE) AS GENRE,
            UPPER(CIVILITE) AS CIVILITE,
            TYPEPIECEIDENTIFICATION AS TYPE_PIECE_IDENTIFICATION,
            UPPER(PROFESSION) AS PROFESSION_IDENTIFICATEUR,
            ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY NVL(FROM_UNIXTIME(UNIX_TIMESTAMP(DATEDERNIEREMODIF,'yyyyMMdd HH:mm:ss')),
                FROM_UNIXTIME(UNIX_TIMESTAMP(DATEDERNIEREMODIF,'dd/mm/yy'))) DESC) AS RG
        FROM DATALAB.IT_CLIENT_SNAPID_DIRECTORY
        WHERE ORIGINAL_FILE_DATE >= DATE_SUB('2019-08-02',30) -- pour récuperer les plus d'identifications récentes
    )T
    WHERE RG = 1 -- dedoublonnage    
) b
ON (a.MSISDN = b.MSISDN)
WHEN MATCHED THEN
    UPDATE SET
        NOM = b.NOM,
        PRENOM = b.PRENOM,
        NEE_LE = b.NEE_LE,
        NEE_A = b.NEE_A,
        QUARTIER_RESIDENCE = b.QUARTIER_RESIDENCE,
        VILLE_VILLAGE = b.VILLE_VILLAGE,
        CNI = b.CNI,
        DATE_IDENTIFICATION = b.DATE_IDENTIFICATION,
        FICHIER_CHARGEMENT = b.FICHIER_CHARGEMENT,
        IDENTIFICATEUR = b.IDENTIFICATEUR,
        GENRE = b.GENRE,
        CIVILITE = b.CIVILITE,
        TYPE_PIECE_IDENTIFICATION = b.TYPE_PIECE_IDENTIFICATION,
        PROFESSION_IDENTIFICATEUR = b.PROFESSION_IDENTIFICATEUR
WHEN NOT MATCHED THEN
INSERT   VALUES (b.MSISDN, b.NOM, b.PRENOM, b.NEE_LE, b.NEE_A, b.PROFESSION, b.QUARTIER_RESIDENCE, b.VILLE_VILLAGE, b.CNI,
               b.DATE_IDENTIFICATION, b.TYPE_DOCUMENT, b.FICHIER_CHARGEMENT, CURRENT_TIMESTAMP , b.EST_SNAPPE,
               b.IDENTIFICATEUR, b.GENRE, b.CIVILITE, b.TYPE_PIECE_IDENTIFICATION, b.PROFESSION_IDENTIFICATEUR
              );


-- 2. Mise à jour des identifications extantes et ajout des nouvelles pour les MSISDN Correctes issues de SNAPID                 
MERGE INTO DATALAB.DT_BASE_IDENTIFICATION a
USING
(
    SELECT MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE, 
        VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, EST_SNAPPE, IDENTIFICATEUR
        , GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
    FROM DATALAB.TT_IDENTIFICATION_MSISDN
    WHERE FN_GET_OPERATOR_CODE(MSISDN)  IN ('OCM', 'SET') AND CASE WHEN CAST(MSISDN AS INT) IS NULL THEN 'N' ELSE 'Y' END  = 'Y'
) b
ON (a.MSISDN = b.MSISDN)
WHEN MATCHED AND a.DATE_IDENTIFICATION < b.DATE_IDENTIFICATION THEN
    UPDATE 
    SET CNI = b.CNI,
        NOM = b.NOM,
        PRENOM = b.PRENOM,
        NEE_LE = b.NEE_LE,
        NEE_A = b.NEE_A,
        PROFESSION = b.PROFESSION,
        QUARTIER_RESIDENCE = b.QUARTIER_RESIDENCE,
        VILLE_VILLAGE = b.VILLE_VILLAGE,
        DATE_IDENTIFICATION = b.DATE_IDENTIFICATION,
        TYPE_DOCUMENT = b.TYPE_DOCUMENT,
        FICHIER_CHARGEMENT = b.FICHIER_CHARGEMENT,
        EST_SNAPPE = b.EST_SNAPPE,
        IDENTIFICATEUR = b.IDENTIFICATEUR,
        DATE_MISE_A_JOUR = CURRENT_TIMESTAMP ,
        DATE_TABLE_MIS_A_JOUR = CURRENT_TIMESTAMP,
        GENRE = b.GENRE,
        CIVILITE = b.CIVILITE,
        TYPE_PIECE_IDENTIFICATION = b.TYPE_PIECE_IDENTIFICATION,
        PROFESSION_IDENTIFICATEUR = b.PROFESSION_IDENTIFICATEUR
WHEN NOT MATCHED THEN
    INSERT
    VALUES (b.MSISDN, b.NOM, b.PRENOM, b.NEE_LE, b.NEE_A, b.PROFESSION, b.QUARTIER_RESIDENCE, b.VILLE_VILLAGE, b.CNI, 
                 b.DATE_IDENTIFICATION, b.TYPE_DOCUMENT, b.FICHIER_CHARGEMENT, CURRENT_TIMESTAMP, b.EST_SNAPPE, b.IDENTIFICATEUR,
                 CURRENT_TIMESTAMP , CURRENT_TIMESTAMP, b.GENRE, b.CIVILITE, b.TYPE_PIECE_IDENTIFICATION, b.PROFESSION_IDENTIFICATEUR,NULL);


--Mis à jour de la table dim.dt_base_identification à partir des données issues de NOMAD
merge into DATALAB.dt_base_identification a
using(
 select
    TELEPHONE,NOMDUCLIENT,PRENOMDUCLIENT,DATEDENAISSANCE,LIEUDENAISSANCE,PROFESSION,QUARTIER,VILLE,NUMEROPIECE,
    DATE_IDENTIFICATION,TYPE_DOCUMENT,fichier_chargement,DATE_INSERTION,EST_SNAPPE,ETAT, ETATDEXPORTGLOBAL,IDENTIFICATEUR,
    date_mise_a_jour,date_table_mis_a_jour,genre,civilite,TYPE_PIECE_IDENTIFICATION,PROFESSION_IDENTIFICATEUR,MOTIF_REJET
from 
(
select
    TELEPHONE ,
    NOMDUCLIENT,
    PRENOMDUCLIENT,
    DATEDENAISSANCE,
    LIEUDENAISSANCE,
    null PROFESSION,
    QUARTIER,
    VILLE,
    NUMEROPIECE,
    substr(EMISLE,1,10)  DATE_IDENTIFICATION,
    null TYPE_DOCUMENT,
    'NOMAD' fichier_chargement,
   substr(MAJLE,1,10) DATE_INSERTION,
   -- (Case when ETAT='VALID' then '1' else null end) EST_SNAPPE,
   (CASE WHEN upper(ETAT)='VALID' and upper(ETATDEXPORTGLOBAL)='SUCCESS' then 'OUI'
          WHEN upper(ETAT)='INVALID' then 'NON'  else 'UNKNOWN' END )EST_SNAPPE,
    ETAT ,
    ETATDEXPORTGLOBAL,
    LOGINVENDEUR IDENTIFICATEUR,
    substr(MAJLE,1,10)  date_mise_a_jour,
    substr(MAJLE,1,10)  date_table_mis_a_jour,
    (Case when TITRE ='Madame(Mme)' then 'F' else 'M' END) genre,
    TITRE  civilite,
     piece TYPE_PIECE_IDENTIFICATION,
    null PROFESSION_IDENTIFICATEUR,
     null MOTIF_REJET ,
     ROW_NUMBER() OVER (PARTITION BY TELEPHONE ORDER BY substr(MAJLE,1,10) DESC) AS RG
  from DATALAB.TT_NOMAD_STATUT_DIRECTORY
  where
    original_file_date = DATE_ADD('2019-08-02',1)
    and TYPEDECONTRAT='Nouvel Abonnement'
    and  ETATDEXPORTGLOBAL ='SUCCESS'
    and LOGINVENDEUR not in ('testfo','NKOLBONG','testve')
  )T   where RG =1
) b on (a.MSISDN = b.TELEPHONE)
WHEN MATCHED THEN
UPDATE SET  
    NOM  =b.NOMDUCLIENT,
    PRENOM=b.PRENOMDUCLIENT,
    NEE_LE=b.DATEDENAISSANCE,
    NEE_A =b.LIEUDENAISSANCE,
    PROFESSION=b.PROFESSION,
    QUARTIER_RESIDENCE=b.QUARTIER,
    VILLE_VILLAGE =b.VILLE,
    CNI=b.NUMEROPIECE,
    DATE_IDENTIFICATION=b.DATE_IDENTIFICATION,
    TYPE_DOCUMENT=b.TYPE_DOCUMENT,
    FICHIER_CHARGEMENT=b.fichier_chargement,
    DATE_INSERTION =b.DATE_INSERTION,
    EST_SNAPPE =b. EST_SNAPPE,
    IDENTIFICATEUR =b. IDENTIFICATEUR,
    DATE_MISE_A_JOUR =CURRENT_TIMESTAMP,
    DATE_TABLE_MIS_A_JOUR =CURRENT_TIMESTAMP,
    GENRE=b.genre,
    CIVILITE =b.CIVILITE,
    TYPE_PIECE_IDENTIFICATION =b.TYPE_PIECE_IDENTIFICATION,
    PROFESSION_IDENTIFICATEUR =b.PROFESSION_IDENTIFICATEUR,
    MOTIF_REJET=b.MOTIF_REJET
 WHEN NOT MATCHED THEN
 INSERT
   VALUES (b.TELEPHONE,b.NOMDUCLIENT,b.PRENOMDUCLIENT,b.DATEDENAISSANCE,b.LIEUDENAISSANCE,b.PROFESSION,b.QUARTIER,b.VILLE,b.NUMEROPIECE,
           b.DATE_IDENTIFICATION,b.TYPE_DOCUMENT,b.fichier_chargement,b.DATE_INSERTION,b. EST_SNAPPE,b. IDENTIFICATEUR,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,b.genre,
           b.CIVILITE ,b.TYPE_PIECE_IDENTIFICATION,b.PROFESSION_IDENTIFICATEUR,b.MOTIF_REJET);

    
/*******  
-- Logging des donnees calculees pour cette date 
*******/
         
--INSERT INTO DT_DATES_SCRIPT_PROCESSED
--VALUES (TO_DATE(s_slice_value, 'yyyymmdd'), 'BASE_IDENTIFICATION', CURRENT_TIMESTAMP);


--UPDATE DATALAB.DT_BASE_IDENTIFICATION
--SET DATE_TABLE_MIS_A_JOUR = CURRENT_TIMESTAMP;


-- 3. Sauvegarde des identifications dont les MSISDN sont Incorrectes dans la table TT_BAD_IDENTIFICATION_MSISDN
MERGE INTO DATALAB.TT_BAD_IDENTIFICATION_MSISDN a
USING
(
    SELECT MSISDN, NOM, PRENOM, NEE_LE, NEE_A, PROFESSION, QUARTIER_RESIDENCE,
        VILLE_VILLAGE, CNI, DATE_IDENTIFICATION, TYPE_DOCUMENT, FICHIER_CHARGEMENT, EST_SNAPPE, IDENTIFICATEUR
        , GENRE, CIVILITE, TYPE_PIECE_IDENTIFICATION, PROFESSION_IDENTIFICATEUR
    FROM DATALAB.TT_IDENTIFICATION_MSISDN
    WHERE FN_GET_OPERATOR_CODE(MSISDN)  NOT IN ('OCM', 'SET') OR CASE WHEN CAST(MSISDN AS INT) IS NULL THEN 'N' ELSE 'Y' END= 'N'
) b
ON (a.MSISDN = b.MSISDN)
WHEN MATCHED AND a.DATE_IDENTIFICATION < b.DATE_IDENTIFICATION THEN
    UPDATE
    SET CNI = b.CNI,
        NOM = b.NOM,
        PRENOM = b.PRENOM,
        NEE_LE = b.NEE_LE,
        NEE_A = b.NEE_A,
        PROFESSION = b.PROFESSION,
        QUARTIER_RESIDENCE = b.QUARTIER_RESIDENCE,
        VILLE_VILLAGE = b.VILLE_VILLAGE,
        DATE_IDENTIFICATION = b.DATE_IDENTIFICATION,
        TYPE_DOCUMENT = b.TYPE_DOCUMENT,
        FICHIER_CHARGEMENT = b.FICHIER_CHARGEMENT,
        EST_SNAPPE = b.EST_SNAPPE,
        IDENTIFICATEUR = b.IDENTIFICATEUR,
        DATE_MISE_A_JOUR = CURRENT_TIMESTAMP,
        DATE_TABLE_MIS_A_JOUR = CURRENT_TIMESTAMP,
        GENRE = b.GENRE,
        CIVILITE = b.CIVILITE,
        TYPE_PIECE_IDENTIFICATION = b.TYPE_PIECE_IDENTIFICATION,
        PROFESSION_IDENTIFICATEUR = b.PROFESSION_IDENTIFICATEUR
WHEN NOT MATCHED THEN
    INSERT
    VALUES (b.MSISDN, b.NOM, b.PRENOM, b.NEE_LE, b.NEE_A, b.PROFESSION, b.QUARTIER_RESIDENCE, b.VILLE_VILLAGE, b.CNI,
                 b.DATE_IDENTIFICATION, b.TYPE_DOCUMENT, b.FICHIER_CHARGEMENT, CURRENT_TIMESTAMP, b.EST_SNAPPE, b.IDENTIFICATEUR, CURRENT_TIMESTAMP , CURRENT_TIMESTAMP
                 , b.GENRE, b.CIVILITE, b.TYPE_PIECE_IDENTIFICATION, b.PROFESSION_IDENTIFICATEUR);
                 
TRUNCATE TABLE DATALAB.TT_IDENTIFICATION_MSISDN;




