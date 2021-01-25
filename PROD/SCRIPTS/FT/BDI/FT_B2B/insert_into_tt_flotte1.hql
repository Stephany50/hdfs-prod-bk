insert into TMP.tt_flotte1
select
(case when trim(B.raison_sociale) = '' or B.raison_sociale is null
    then trim(A.nom_structure)
    else trim(B.raison_sociale)
end) as nom_structure,
(case when trim(B.numero_registre_commerce) = '' or B.numero_registre_commerce is null
    then trim(A.numero_registre_commerce)
    else trim(B.numero_registre_commerce)
end ) as NUMERO_REGISTRE_COMMERCE,
(case when trim(B.cni_representant_local) = '' or B.cni_representant_local is null
    then trim(A.numero_piece_representant_legal)
    else trim(B.cni_representant_local) end ) as numero_piece_representant_legal,
nvl(A.date_activation,A.date_souscription) as date_souscription,
trim(B.adresse_structure) as adresse_structure,
trim(A.msisdn) as msisdn,
trim(A.nom_prenom) as nom_prenom,
trim(A.numero_piece) as numero_piece,
trim(A.imei) as imei,
trim(A.adresse) as adresse,
trim(A.statut) as statut,
nvl(A.disponibilite_scan,B.disponibilite_scan) as disponibilite_scan,
nvl(A.acceptation_cgv,B.acceptation_cgv) as acceptation_cgv,
customer_id, contract_id, compte_client, type_personne, type_piece, id_type_piece, nom, prenom, date_naissance,
date_expiration, ville, quartier, statut_old as statut_old, raison_statut, odbic, odboc, date_changement_statut,
plan_localisation, contrat_soucription, type_piece_tuteur, numero_piece_tuteur,
nom_tuteur, prenom_tuteur, date_naissance_tuteur, date_expiration_tuteur, adresse_tuteur, compte_client_structure,
statut_derogation, region_administrative, region_commerciale, site_name, ville_site, offre_commerciale, type_contrat,
segmentation, derogation_identification,
compte_client_parent, nom_representant_legal, prenom_representant_legal, contact_telephonique, ville_structure,
quartier_structure, sms_contact, doc_plan_localisation, doc_fiche_souscription, doc_attestation_cnps,
doc_rccm, type_client
from
(select
msisdn,
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
(CASE
WHEN trim(DATE_SOUSCRIPTION) IS NULL OR trim(DATE_SOUSCRIPTION) = '' THEN NULL
WHEN trim(DATE_SOUSCRIPTION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_SOUSCRIPTION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_SOUSCRIPTION) like '%-%' THEN  cast(SUBSTR(trim(DATE_SOUSCRIPTION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_SOUSCRIPTION,
(CASE
WHEN trim(DATE_ACTIVATION) IS NULL OR trim(DATE_ACTIVATION) = '' THEN NULL
WHEN trim(DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION,
statut_old,
raison_statut,
statut,
odbic,
odboc,
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
derogation_identification
from TMP.tt_flotte02
where upper(trim(compte_client_structure)) not like '1.%'
and not((nom_structure like '%FORIS%TELEC%' or nom_structure like '%SAVANA%ISLAM%')
        and upper(trim(compte_client_structure)) in ('4.8004','4.4335')
     )
) A
left join
(select
compte_client as compte_client_parent,
raison_sociale,
nom_representant_legal,
prenom_representant_legal,
cni_representant_local,
contact_telephonique,
ville_structure,
quartier_structure,
adresse_structure,
numero_registre_commerce,
sms_contact,
doc_plan_localisation,
doc_fiche_souscription,
acceptation_cgv,
doc_attestation_cnps,
doc_rccm,
disponibilite_scan,
type_client
from TMP.tt_pm01) B
on substr(upper(trim(A.compte_client_structure)),1,6) =  substr(upper(trim(B.compte_client_parent)),1,6)