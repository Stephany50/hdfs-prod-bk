insert into CDR.SPARK_IT_BDI_LIGNE_FLOTTE
select
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.msisdn),'\n+','n'),'[|]+',' ')),'"+','') AS msisdn,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.customer_id),'\n+','n'),'[|]+',' ')),'"+','') AS customer_id,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.contract_id),'\n+','n'),'[|]+',' ')),'"+','') AS contract_id,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.compte_client),'\n+','n'),'[|]+',' ')),'"+','') AS compte_client,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.type_personne),'\n+','n'),'[|]+',' ')),'"+','') AS type_personne,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.type_piece),'\n+','n'),'[|]+',' ')),'"+','') AS type_piece,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.numero_piece),'\n+','n'),'[|]+',' ')),'"+','') AS numero_piece,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.id_type_piece),'\n+','n'),'[|]+',' ')),'"+','') AS id_type_piece,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.nom_prenom),'\n+','n'),'[|]+',' ')),'"+','') AS nom_prenom,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.nom),'\n+','n'),'[|]+',' ')),'"+','') AS nom,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.prenom),'\n+','n'),'[|]+',' ')),'"+','') AS prenom,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_naissance),'\n+','n'),'[|]+',' ')),'"+','') AS date_naissance,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_expiration),'\n+','n'),'[|]+',' ')),'"+','') AS date_expiration,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.adresse),'\n+','n'),'[|]+',' ')),'"+','') AS adresse,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.ville),'\n+','n'),'[|]+',' ')),'"+','') AS ville,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.quartier),'\n+','n'),'[|]+',' ')),'"+','') AS quartier,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_souscription),'\n+','n'),'[|]+',' ')),'"+','') AS date_souscription,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_activation),'\n+','n'),'[|]+',' ')),'"+','') AS date_activation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.statut),'\n+','n'),'[|]+',' ')),'"+','') AS statut,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.raison_statut),'\n+','n'),'[|]+',' ')),'"+','') AS raison_statut,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_changement_statut),'\n+','n'),'[|]+',' ')),'"+','') AS date_changement_statut,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.plan_localisation),'\n+','n'),'[|]+',' ')),'"+','') AS plan_localisation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.contrat_soucription),'\n+','n'),'[|]+',' ')),'"+','') AS contrat_soucription,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.disponibilite_scan),'\n+','n'),'[|]+',' ')),'"+','') AS disponibilite_scan,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.acceptation_cgv),'\n+','n'),'[|]+',' ')),'"+','') AS acceptation_cgv,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.type_piece_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS type_piece_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.numero_piece_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS numero_piece_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.nom_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS nom_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.prenom_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS prenom_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_naissance_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS date_naissance_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.date_expiration_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS date_expiration_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.adresse_tuteur),'\n+','n'),'[|]+',' ')),'"+','') AS adresse_tuteur,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.compte_client_structure),'\n+','n'),'[|]+',' ')),'"+','') AS compte_client_structure,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.nom_structure),'\n+','n'),'[|]+',' ')),'"+','') AS nom_structure,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.numero_registre_commerce),'\n+','n'),'[|]+',' ')),'"+','') AS numero_registre_commerce,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.numero_piece_representant_legal),'\n+','n'),'[|]+',' ')),'"+','') AS numero_piece_representant_legal,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.imei),'\n+','n'),'[|]+',' ')),'"+','') AS imei,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.statut_derogation),'\n+','n'),'[|]+',' ')),'"+','') AS statut_derogation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.region_administrative),'\n+','n'),'[|]+',' ')),'"+','') AS region_administrative,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.region_commerciale),'\n+','n'),'[|]+',' ')),'"+','') AS region_commerciale,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.site_name),'\n+','n'),'[|]+',' ')),'"+','') AS site_name,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.ville_site),'\n+','n'),'[|]+',' ')),'"+','') AS ville_site,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.offre_commerciale),'\n+','n'),'[|]+',' ')),'"+','') AS offre_commerciale,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.type_contrat),'\n+','n'),'[|]+',' ')),'"+','') AS type_contrat,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.segmentation),'\n+','n'),'[|]+',' ')),'"+','') AS segmentation,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.odbincomingcalls),'\n+','n'),'[|]+',' ')),'"+','') AS odbincomingcalls,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.odboutgoingcalls),'\n+','n'),'[|]+',' ')),'"+','') AS odboutgoingcalls,
regexp_replace(trim(regexp_replace(regexp_replace(trim(fl.derogation_identification),'\n+','n'),'[|]+',' ')),'"+','') AS derogation_identification,
current_timestamp() insert_date,
'###SLICE_VALUE###' original_file_date
from (
select
trim(b.MSISDN) AS MSISDN,
trim(b.CUSTOMER_ID) AS CUSTOMER_ID,
trim(b.CONTRACT_ID) AS CONTRACT_ID,
trim(b.COMPTE_CLIENT) AS COMPTE_CLIENT,
trim(b.TYPE_PERSONNE) AS TYPE_PERSONNE,
trim(b.TYPE_PIECE) AS TYPE_PIECE,
trim(b.NUMERO_PIECE) AS NUMERO_PIECE,
trim(b.ID_TYPE_PIECE) AS ID_TYPE_PIECE,
trim(b.NOM_PRENOM) AS NOM_PRENOM,
trim(b.NOM) AS NOM,
trim(b.PRENOM) AS PRENOM,
trim(b.DATE_NAISSANCE) AS DATE_NAISSANCE,
trim(b.DATE_EXPIRATION) AS DATE_EXPIRATION,
trim(b.ADRESSE) AS ADRESSE,
trim(b.VILLE) AS VILLE,
trim(b.QUARTIER) AS QUARTIER,
trim(b.DATE_SOUSCRIPTION) AS DATE_SOUSCRIPTION,
trim(b.DATE_ACTIVATION) AS DATE_ACTIVATION,
trim(b.STATUT) AS STATUT,
trim(b.RAISON_STATUT) AS RAISON_STATUT,
trim(b.DATE_CHANGEMENT_STATUT) AS DATE_CHANGEMENT_STATUT,
trim(b.PLAN_LOCALISATION) AS PLAN_LOCALISATION,
trim(b.CONTRAT_SOUCRIPTION) AS CONTRAT_SOUCRIPTION,
trim(b.DISPONIBILITE_SCAN) AS DISPONIBILITE_SCAN,
trim(b.ACCEPTATION_CGV) AS ACCEPTATION_CGV,
trim(b.TYPE_PIECE_TUTEUR) AS TYPE_PIECE_TUTEUR,
trim(b.NUMERO_PIECE_TUTEUR) AS NUMERO_PIECE_TUTEUR,
trim(b.NOM_TUTEUR) AS NOM_TUTEUR,
trim(b.PRENOM_TUTEUR) AS PRENOM_TUTEUR,
trim(b.DATE_NAISSANCE_TUTEUR) AS DATE_NAISSANCE_TUTEUR,
trim(b.DATE_EXPIRATION_TUTEUR) AS DATE_EXPIRATION_TUTEUR,
trim(b.ADRESSE_TUTEUR) AS ADRESSE_TUTEUR,
trim(b.COMPTE_CLIENT_STRUCTURE) AS COMPTE_CLIENT_STRUCTURE,
trim(b.NOM_STRUCTURE) AS NOM_STRUCTURE,
trim(b.NUMERO_REGISTRE_COMMERCE) AS NUMERO_REGISTRE_COMMERCE,
trim(b.NUMERO_PIECE_REPRESENTANT_LEGAL) AS NUMERO_PIECE_REPRESENTANT_LEGAL,
trim(b.IMEI) AS IMEI,
trim(b.STATUT_DEROGATION) AS STATUT_DEROGATION,
trim(b.REGION_ADMINISTRATIVE) AS REGION_ADMINISTRATIVE,
trim(b.REGION_COMMERCIALE) AS REGION_COMMERCIALE,
trim(b.SITE_NAME) AS SITE_NAME,
trim(b.VILLE_SITE) AS VILLE_SITE,
trim(b.OFFRE_COMMERCIALE) AS OFFRE_COMMERCIALE,
trim(b.TYPE_CONTRAT) AS TYPE_CONTRAT,
trim(b.SEGMENTATION) AS SEGMENTATION,
trim(b.odbIncomingCalls) AS odbIncomingCalls,
trim(b.odbOutgoingCalls) AS odbOutgoingCalls,
trim(b.DEROGATION_IDENTIFICATION) AS DEROGATION_IDENTIFICATION,
b.insert_date AS insert_date,
b.original_file_date AS original_file_date
from (
select a2.*,
row_number() over(partition by a2.msisdn order by a2.date_activation2 DESC NULLS LAST) AS RANG
from (
select a.*,
(CASE
WHEN trim(a.DATE_ACTIVATION) IS NULL OR trim(a.DATE_ACTIVATION) = '' THEN NULL
WHEN trim(a.DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(a.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION2
from TMP.TT_BDI_LIGNE_FLOTTE2 a
join (select *
from cdr.spark_it_bdi_zsmart
where original_file_date='###SLICE_VALUE###'
) zsm
on substr(upper(trim(a.msisdn)),-9,9) = substr(upper(trim(zsm.msisdn)),-9,9)
) a2
) b where RANG = 1
) fl