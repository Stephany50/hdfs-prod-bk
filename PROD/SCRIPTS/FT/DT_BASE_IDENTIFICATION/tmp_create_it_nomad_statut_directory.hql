CREATE TABLE TMP.IT_NOMAD_CLIENT_DIRECTORY1(
NUMERO STRING,
TYPEDECONTRAT STRING,
SOURCE STRING,
TELEPHONE STRING,
GENDER STRING,
TITRE STRING,
PRENOMDUCLIENT STRING,
NOMDUCLIENT STRING,
DATEDENAISSANCE STRING,
LIEUDENAISSANCE STRING,
PIECE STRING,
NUMEROPIECE STRING,
DELIVRANCE STRING,
EXPIRATION STRING,
NATIONALITE STRING,
QUARTIER STRING,
VILLE STRING,
LOGINVENDEUR STRING,
PRENOMVENDEUR STRING,
NOMVENDEUR STRING,
NUMERODUVENDEUR STRING,
LOGINDISTRIBUTEUR STRING,
PRENOMDISTRIBUTEUR STRING,
NOMDISTRIBUTEUR STRING,
EMISLE STRING,
MAJLE STRING,
LOGINMAJ STRING,
PRENOMMAJ STRING,
NOMMAJ STRING,
ETAT STRING,
ETATDEXPORTGLOBAL STRING,
LOGINVALIDATEUR STRING,
PRENOMVALIDATEUR STRING,
NOMVALIDATEUR STRING,
CAUSEECHEC STRING,
COMMENTAIRE STRING,
PWDCLIENT STRING,
LAST_UPDATE_DATE STRING,
DELIVRANCE1 STRING,
LIEUDEDELIVRANCE STRING,
COPIE STRING,
UPDATE_ON STRING,
INSERT_DATE STRING,
ORIGINAL_FILE_NAME STRING)
PARTITIONED BY (ORIGINAL_FILE_date DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");


insert into TMP.IT_NOMAD_CLIENT_DIRECTORY1 select
NUMERO,
TYPEDECONTRAT,
SOURCE,
TELEPHONE,
GENDER,
TITRE,
PRENOMDUCLIENT,
NOMDUCLIENT,
DATEDENAISSANCE,
LIEUDENAISSANCE,
PIECE,
NUMEROPIECE,
DELIVRANCE,
EXPIRATION,
NATIONALITE,
QUARTIER,
VILLE,
LOGINVENDEUR,
PRENOMVENDEUR,
NOMVENDEUR,
NUMERODUVENDEUR,
LOGINDISTRIBUTEUR,
PRENOMDISTRIBUTEUR,
NOMDISTRIBUTEUR,
EMISLE,
MAJLE,
LOGINMAJ,
PRENOMMAJ,
NOMMAJ,
ETAT,
ETATDEXPORTGLOBAL,
LOGINVALIDATEUR,
PRENOMVALIDATEUR,
NOMVALIDATEUR,
CAUSEECHEC,
COMMENTAIRE,
PWDCLIENT,
LAST_UPDATE_DATE,
DELIVRANCE1,
LIEUDEDELIVRANCE,
COPIE,
UPDATE_ON,
INSERT_DATE,
ORIGINAL_FILE_NAME,
to_date(ORIGINAL_FILE_date)
from backup_dwh.IT_NOMAD_CLIENT_DIRECTORY1