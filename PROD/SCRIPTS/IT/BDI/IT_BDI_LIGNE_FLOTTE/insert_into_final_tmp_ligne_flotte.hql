insert into TMP.TT_KYC_FINAL_FLOTTE
select
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.msisdn,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS msisdn,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.customer_id,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS customer_id,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.contract_id,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS contract_id,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.compte_client,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS compte_client,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.type_personne,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_personne,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.type_piece,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_piece,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.numero_piece,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_piece,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.id_type_piece,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS id_type_piece,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.nom_prenom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom_prenom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.nom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.prenom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS prenom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_naissance,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_naissance,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_expiration,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_expiration,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.adresse,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS adresse,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.ville,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS ville,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.quartier,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS quartier,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_souscription,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_souscription,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_activation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_activation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.statut,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS statut,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.raison_statut,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS raison_statut,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_changement_statut,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_changement_statut,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.plan_localisation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS plan_localisation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.contrat_soucription,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS contrat_soucription,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.disponibilite_scan,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS disponibilite_scan,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.acceptation_cgv,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS acceptation_cgv,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.type_piece_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_piece_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.numero_piece_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_piece_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.nom_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.prenom_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS prenom_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_naissance_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_naissance_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.date_expiration_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_expiration_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.adresse_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS adresse_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.compte_client_structure,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS compte_client_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.nom_structure,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.numero_registre_commerce,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_registre_commerce,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.numero_piece_representant_legal,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_piece_representant_legal,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.imei,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS imei,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.statut_derogation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS statut_derogation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.region_administrative,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS region_administrative,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.region_commerciale,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS region_commerciale,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.site_name,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS site_name,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.ville_site,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS ville_site,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.offre_commerciale,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS offre_commerciale,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.type_contrat,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_contrat,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.segmentation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS segmentation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.odbincomingcalls,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS odbincomingcalls,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.odboutgoingcalls,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS odboutgoingcalls,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(a.derogation_identification,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS derogation_identification
from (
select a1.*,
row_number() over(partition by msisdn order by date_activation2 desc nulls last) as rang
from (
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) as msisdn,
customer_id,
contract_id,
compte_client,
type_personne,
type_piece,
numero_piece,
id_type_piece,
nom_prenom,
nom,
prenom,
date_naissance,
date_expiration,
adresse,
ville,
quartier,
date_souscription,
date_activation,
statut,
raison_statut,
date_changement_statut,
plan_localisation,
contrat_soucription,
disponibilite_scan,
acceptation_cgv,
type_piece_tuteur,
numero_piece_tuteur,
nom_tuteur,
prenom_tuteur,
date_naissance_tuteur,
date_expiration_tuteur,
adresse_tuteur,
compte_client_structure,
nom_structure,
numero_registre_commerce,
numero_piece_representant_legal,
imei,
statut_derogation,
region_administrative,
region_commerciale,
site_name,
ville_site,
offre_commerciale,
type_contrat,
segmentation,
odbincomingcalls,
odboutgoingcalls,
derogation_identification,
a.insert_date,
a.original_file_date,
nvl((CASE
WHEN trim(a.DATE_ACTIVATION) IS NULL OR trim(a.DATE_ACTIVATION) = '' THEN NULL
WHEN trim(a.DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(a.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END),
(CASE
WHEN trim(a.DATE_SOUSCRIPTION) IS NULL OR trim(a.DATE_SOUSCRIPTION) = '' THEN NULL
WHEN trim(a.DATE_SOUSCRIPTION) like '%/%'
THEN  cast(translate(SUBSTR(trim(a.DATE_SOUSCRIPTION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(a.DATE_SOUSCRIPTION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_SOUSCRIPTION), 1, 19) AS TIMESTAMP)
ELSE NULL
END)) as date_activation2
from TMP.TT_KYC_CLASSIFIED_FLOTTE a
) a1
) a 
join (select distinct FN_FORMAT_MSISDN_TO_9DIGITS(trim(msisdn)) as msisdn
from cdr.spark_it_bdi_hlr where original_file_date='###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = trim(b.msisdn)
where a.rang = 1
