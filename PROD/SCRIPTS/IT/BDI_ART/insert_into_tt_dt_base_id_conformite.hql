insert into TMP.TT_DT_BASE_ID_CONFORMITE 
select
msisdn,
nom,
prenom ,
date_naissance ,
nee_a,
profession ,
quartier_residence ,
ville_village,
cni,
(CASE
WHEN trim(date_identification) IS NULL OR trim(date_identification) = '' THEN NULL
WHEN trim(date_identification) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_identification), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_identification) like '%-%' THEN  cast(SUBSTR(trim(date_identification), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_identification,
type_document ,
fichier_chargement ,
date_insertion ,
est_snappe,
identificateur ,
date_mise_a_jour ,
date_table_mis_a_jour,
genre ,
civilite ,
type_piece_identification ,
profession_identificateur,
motif_rejet ,
nvl(nom,'') || ' ' || nvl(prenom,'') as nom_prenom
from (
select a.*,
row_number() over(partition by msisdn order by date_mise_a_jour desc nulls last) as rang
from (
select 
trim(msisdn) AS msisdn,
trim(nom) AS nom,
trim(prenom ) AS prenom ,
nee_le AS DATE_NAISSANCE,
trim(nee_a) AS nee_a,
trim(profession ) AS profession ,
trim(quartier_residence ) AS quartier_residence ,
trim(ville_village) AS ville_village,
trim(cni) AS cni,
(CASE
WHEN trim(date_identification) IS NULL OR trim(date_identification) = '' THEN NULL
WHEN trim(date_identification) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_identification), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_identification) like '%-%' THEN  cast(SUBSTR(trim(date_identification), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_identification,
trim(type_document ) AS type_document ,
trim(fichier_chargement ) AS fichier_chargement ,
trim(date_insertion ) AS date_insertion ,
trim(est_snappe) AS est_snappe,
trim(identificateur ) AS identificateur ,
(CASE
WHEN trim(date_mise_a_jour) IS NULL OR trim(date_mise_a_jour) = '' THEN NULL
WHEN trim(date_mise_a_jour) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_mise_a_jour), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_mise_a_jour) like '%-%' THEN  cast(SUBSTR(trim(date_mise_a_jour), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_mise_a_jour,
trim(date_table_mis_a_jour) as date_table_mis_a_jour,
trim(genre ) AS genre ,
trim(civilite ) AS civilite ,
trim(type_piece_identification ) AS type_piece_identification ,
trim(profession_identificateur) AS profession_identificateur,
trim(motif_rejet ) AS motif_rejet
from dim.spark_dt_base_identification ) a) b where rang = 1
