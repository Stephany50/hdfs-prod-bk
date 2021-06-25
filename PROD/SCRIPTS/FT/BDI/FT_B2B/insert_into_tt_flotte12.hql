--etape 17 au moins une ligne des entreprises conformes
insert into TMP.tt_flotte12
select
(case when trim(a.nom_structure_an) = 'OUI'
then b.nom_structure
else a.nom_structure
end) nom_structure,
( case when trim(a.rccm_an)='OUI'
then b.numero_registre_commerce
else a.numero_registre_commerce
end ) numero_registre_commerce,
(case when trim(a.num_piece_rpstant_an) = 'OUI'
then b.num_piece_representant_legal
else a.num_piece_representant_legal
end) num_piece_representant_legal,
(case when trim(a.date_souscription_an) = 'OUI'
then b.date_souscription
else a.date_souscription
end) date_souscription,
(case when trim(a.adresse_structure_an) = 'OUI'
then (case when length(trim(nvl(b.adresse_structure,''))) = 0 then a.ville_structure||"  "||a.quartier_structure else b.adresse_structure end)                           
else a.adresse_structure
end) adresse_structure,
a.msisdn,
a.nom_prenom,
(case when trim(a.numero_piece_an) = 'OUI'
then b.numero_piece
else a.numero_piece
end) numero_piece,
b.imei,
a.adresse,
a.statut,
a.disponibilite_scan,
a.acceptation_cgv,
a.customer_id,
a.contract_id,
a.compte_client,
a.type_personne,
a.type_piece,
a.id_type_piece,
a.nom,
a.prenom,
a.date_naissance,
a.date_expiration,
a.ville,
a.quartier,
a.statut_old,
a.raison_statut,
a.odbic,
a.odboc,
a.date_changement_statut,
a.plan_localisation,
a.contrat_soucription,
a.type_piece_tuteur,
a.numero_piece_tuteur,
a.nom_tuteur,
a.prenom_tuteur,
a.date_naissance_tuteur,
a.date_expiration_tuteur,
a.adresse_tuteur,
a.compte_client_structure,
a.statut_derogation,
a.region_administrative,
a.region_commerciale,
a.site_name,
a.ville_site,
a.offre_commerciale,
a.type_contrat,
a.segmentation,
a.derogation_identification,
a.compte_client_parent,
a.nom_representant_legal,
a.prenom_representant_legal,
a.contact_telephonique,
a.ville_structure,
a.quartier_structure,
a.sms_contact,
a.doc_plan_localisation,
a.doc_fiche_souscription,
a.doc_attestation_cnps,
a.doc_rccm,
a.type_client,
a.rang2,
a.type_personne_morale,
a.nom_structure_an,
a.rccm_an,
a.num_piece_rpstant_an,
a.date_souscription_an,
a.adresse_structure_an,
a.num_tel_an,
a.nom_prenom_an,
a.numero_piece_an,
a.imei_an,
a.adresse_an,
a.statut_an,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from 
TMP.tt_flotte10 a
left join TMP.tt_flotte11 b on substr(upper(trim(a.compte_client_structure)),1,6) = substr(upper(trim(b.compte_client_structure)),1,6)