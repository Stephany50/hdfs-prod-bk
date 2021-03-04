insert into TMP.tt_flotte5
select
a.nom_structure,
(case when upper(trim(b.TYPE_PERSONNE_MORALE)) like '%GC%ETATS%' then 'NON ASSUJETTI'
when upper(trim(b.TYPE_PERSONNE_MORALE)) in ('ASSOCIATION','MINISTERE/ORGANISME GOUVERNEMENTAL','ONG','AMBASSADE') then 'NON ASSUJETTI'
else a.numero_registre_commerce end) as numero_registre_commerce,
a.num_piece_representant_legal,
a.date_souscription,
a.adresse_structure,
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
a.type_personne_morale
from TMP.tt_flotte4 a
left join (select *
from TMP.tt_flotte4
where  (upper(trim(type_personne_morale)) in ('ASSOCIATION','MINISTERE/ORGANISME GOUVERNEMENTAL','ONG','AMBASSADE')
or upper(trim(type_personne_morale)) like '%GC%ETATS%')
and (trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2)
) b
on trim(a.msisdn) = trim(b.msisdn)
