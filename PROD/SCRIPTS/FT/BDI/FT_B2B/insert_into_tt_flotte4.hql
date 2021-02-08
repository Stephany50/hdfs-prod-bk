insert into TMP.tt_flotte4
select
(case
when b.msisdn is null then a.nom_structure
else b.nom_structure
end) as nom_structure,
(case
when c.msisdn is null then a.numero_registre_commerce
else c.numero_registre_commerce
end) as numero_registre_commerce,
(case
when d.msisdn is null then a.num_piece_representant_legal
else d.num_piece_representant_legal
end) as num_piece_representant_legal,
a.date_souscription,
(case
when e.msisdn is null then a.adresse_structure
else e.adresse_structure
end) as adresse_structure,
a.msisdn,
a.nom_prenom,
a.numero_piece,
a.imei,
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
a.rang,
(case
when upper(trim(a.type_client)) like '%GC%ETATS%'  then a.type_client
else f.TYPE_PERSONNE_MORALE
end) as type_personne_morale
from TMP.tt_flotte3 a
left join TMP.tt_flotte4_ns b on trim(a.msisdn) = trim(b.msisdn)
left join TMP.tt_flotte4_RCCM c on trim(a.msisdn) = trim(c.msisdn)
left join TMP.tt_flotte4_PIECE_REP d on trim(a.msisdn) = trim(d.msisdn)
left join TMP.tt_flotte4_ADRES_STRUCT e on trim(a.msisdn) = trim(e.msisdn)
left join DIM.SPARK_DT_BDI_B2B_2019_CONFORM f on trim(a.msisdn) = trim(f.msisdn)