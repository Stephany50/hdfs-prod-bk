insert into CDR.SPARK_IT_BDI_LIGNE_FLOTTE
select
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(msisdn,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS msisdn,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(customer_id,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS customer_id,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(contract_id,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS contract_id,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(compte_client,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS compte_client,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(type_personne,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_personne,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(type_piece,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_piece,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(numero_piece,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_piece,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(id_type_piece,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS id_type_piece,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(nom_prenom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom_prenom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(nom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(prenom,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS prenom,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_naissance,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_naissance,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_expiration,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_expiration,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(adresse,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS adresse,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(ville,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS ville,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(quartier,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS quartier,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_souscription,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_souscription,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_activation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_activation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(statut,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS statut,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(raison_statut,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS raison_statut,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_changement_statut,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_changement_statut,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(plan_localisation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS plan_localisation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(contrat_soucription,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS contrat_soucription,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(disponibilite_scan,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS disponibilite_scan,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(acceptation_cgv,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS acceptation_cgv,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(type_piece_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_piece_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(numero_piece_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_piece_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(nom_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(prenom_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS prenom_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_naissance_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_naissance_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(date_expiration_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS date_expiration_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(adresse_tuteur,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS adresse_tuteur,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(compte_client_structure,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS compte_client_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(nom_structure,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS nom_structure,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(numero_registre_commerce,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_registre_commerce,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(numero_piece_representant_legal,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS numero_piece_representant_legal,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(imei,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS imei,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(statut_derogation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS statut_derogation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(region_administrative,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS region_administrative,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(region_commerciale,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS region_commerciale,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(site_name,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS site_name,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(ville_site,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS ville_site,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(offre_commerciale,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS offre_commerciale,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(type_contrat,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS type_contrat,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(segmentation,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS segmentation,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(odbincomingcalls,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS odbincomingcalls,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(odboutgoingcalls,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS odboutgoingcalls,
trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(nvl(derogation_identification,''),'(\r)+','r'),'(\n)+','n'),'(\t)+','t'),'\\s+',' '),'[|";]+',' ')) AS derogation_identification,
current_timestamp() AS insert_date,
'###SLICE_VALUE###' AS original_file_date
from (
select x.*,
row_number() over(partition by msisdn order by date_activation2 desc nulls last) as rang
from (
select a.*,
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
from TMP.TT_BDI_LIGNE_FLOTTE2 a
join (select *
from cdr.spark_it_bdi_zsmart
where original_file_date='###SLICE_VALUE###') b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
) x
) c where rang = 1