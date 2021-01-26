insert into TMP.tt_flotte01
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) as msisdn, A.customer_id, A.contract_id, A.compte_client, A.type_personne, A.type_piece, A.numero_piece, A.id_type_piece,
A.nom_prenom, A.nom, A.prenom, A.date_naissance, A.date_expiration, A.adresse, A.ville, A.quartier, A.date_souscription,
A.date_activation, A.statut as statut_old, A.raison_statut, A.date_changement_statut, A.plan_localisation, A.contrat_soucription,
A.disponibilite_scan, A.acceptation_cgv, A.type_piece_tuteur, A.numero_piece_tuteur, A.nom_tuteur, A.prenom_tuteur,
A.date_naissance_tuteur, A.date_expiration_tuteur, A.adresse_tuteur, A.compte_client_structure, A.nom_structure,
A.numero_registre_commerce, A.numero_piece_representant_legal, A.imei, A.statut_derogation, A.region_administrative,
A.region_commerciale, A.site_name, A.ville_site, A.offre_commerciale, A.type_contrat, A.segmentation, A.odbincomingcalls,
A.odboutgoingcalls, A.derogation_identification, B.statut as statut,B.odbic,odboc
from
(select *
from cdr.Spark_it_bdi_ligne_flotte_1A
where original_file_date=date_add('###SLICE_VALUE###',1)) A
left join (select *
from MON.SPARK_FT_ABONNE_HLR
where event_date='###SLICE_VALUE###') B
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.msisdn))