 CREATE TABLE CDR.SPARK_it_bdi_crm_b2c(
msisdn varchar(100),
type_personne varchar(100),
nom_prenom varchar(100),
id_type_piece varchar(100),
type_piece varchar(100),
numero_piece varchar(100),
date_expiration varchar(100),
date_naissance varchar(100),
date_activation varchar(100),
adresse varchar(100),
quartier varchar(100),
ville varchar(100),
statut varchar(100),
statut_validation_bo varchar(100),
motif_rejet_bo varchar(100),
date_validation_bo varchar(100),
login_validateur_bo varchar(100),
canal_validateur_bo varchar(100),
type_abonnement varchar(100),
csmoddate varchar(100),
ccmoddate varchar(100),
compte_client_structure varchar(100),
nom_structure varchar(100),
numero_registre_commerce varchar(100),
numero_piece_representant_legal varchar(100),
imei varchar(100),
statut_derogation varchar(100),
region_administrative varchar(100),
region_commerciale varchar(100),
site_name varchar(100),
ville_site varchar(100),
offre_commerciale varchar(100),
type_contrat varchar(100),
segmentation varchar(100),
date_souscription varchar(100),
date_changement_statut varchar(100),
ville_structure varchar(100),
quartier_structure varchar(100),
raison_statut varchar(100),
prenom varchar(100),
nom varchar(100),
customer_id varchar(100),
contract_id varchar(100),
compte_client varchar(100),
plan_localisation varchar(100),
contrat_soucription varchar(100),
acceptation_cgv varchar(100),
disponibilite_scan varchar(100),
nom_tuteur varchar(100),
prenom_tuteur varchar(100),
date_naissance_tuteur varchar(100),
numero_piece_tuteur varchar(100),
date_expiration_tuteur varchar(100),
id_type_piece_tuteur varchar(100),
type_piece_tuteur varchar(100),
adresse_tuteur varchar(100),
identificateur varchar(100),
localisation_identificateur varchar(100),
profession varchar(100),
ORIGINAL_FILE_NAME    VARCHAR(100),
ORIGINAL_FILE_SIZE    VARCHAR(100),
ORIGINAL_FILE_LINE_COUNT INT,
insert_date timestamp
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')