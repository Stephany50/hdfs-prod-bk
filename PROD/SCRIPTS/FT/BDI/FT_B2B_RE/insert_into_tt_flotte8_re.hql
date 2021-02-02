insert into TMP.tt_flotte8_re
select (case when trim(b.nom_structure_an) = 'OUI'
then a.nom_structure
else b.nom_structure
end) as nom_structure,
( case when trim(b.rccm_an)='OUI'
then a.numero_registre_commerce
else b.numero_registre_commerce
end ) as numero_registre_commerce,
(case when trim(b.num_piece_rpstant_an) = 'OUI'
then a.num_piece_representant_legal
else b.numero_piece_representant_legal
end) as num_piece_representant_legal,
(case when b.date_activation is null
then a.date_souscription
else b.date_activation
end) as date_souscription,
a.adresse_structure,
a.msisdn,
(case when trim(b.nom_prenom_an) = 'OUI'
then a.nom_prenom
else b.nom_prenom
end) as nom_prenom,
(case when trim(b.numero_piece_an) = 'OUI'
then a.numero_piece
else b.numero_piece
end ) as numero_piece,
a.imei,
(case when trim(b.adresse_an) = 'OUI'
then a.adresse
else b.adresse
end) as adresse,
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
a.rang,
a.type_personne_morale
from TMP.tt_flotte7_re a
left join (select *
from MON.SPARK_FT_ZSMART_CONF
where event_date='###SLICE_VALUE###') b
on upper(trim(a.msisdn)) = upper(trim(b.msisdn))