insert into TMP.KYC_TT_ALIGN1
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) as msisdn,upper(trim(a.nom)) as nom_telco,upper(trim(a.prenom)) as prenom_telco,
upper(trim(concat_ws(' ',nvl(a.nom,''),nvl(a.prenom,'')))) as nom_prenom_telco,
a.date_naissance as date_naissance_telco,upper(trim(a.numero_piece)) as numero_piece_telco,
a.est_suspendu,a.odbincomingcalls,a.odboutgoingcalls,a.statut_bscs,a.date_activation,a.date_changement_statut,
a.date_expiration,
upper(trim(d.adresse)) as adresse,
(case when trim(b.est_snappe)='OUI' then 'VALIDE'
when trim(b.est_snappe) = 'NON' then 'REJETE'
when trim(b.est_snappe) = 'UNKNOWN' then 'NON VERIFIE'
else 'INCONNU'
end) as statut_validation_bo_telco,
nvl(b.date_mise_a_jour,c.last_update_date) as date_mise_a_jour_bo_telco
from
(select a.msisdn,
a.nom,a.prenom,a.date_naissance,a.numero_piece,
(case when UPPER(trim(a.STATUT)) in (UPPER('Suspendu')) then 'OUI' else 'NON' end) est_suspendu,
a.STATUT statut_bscs,a.date_activation,a.date_changement_statut,
a.odbincomingcalls,a.odboutgoingcalls,a.date_expiration
from CDR.SPARK_IT_KYC_BDI_FULL a
where original_file_date = date_add('###SLICE_VALUE###',1)
) a
left join (select msisdn,est_snappe,(CASE
WHEN trim(date_mise_a_jour) IS NULL OR trim(date_mise_a_jour) = '' THEN NULL
WHEN trim(date_mise_a_jour) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_mise_a_jour), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_mise_a_jour) like '%-%' THEN  cast(SUBSTR(trim(date_mise_a_jour), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_mise_a_jour
from DIM.SPARK_DT_BASE_IDENTIFICATION) b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
left join (
select telephone, last_update_date
from TMP.KYC_TT_NOMAD_DATA
) c on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.telephone))
left join (
select msisdn,adresse
from (
select msisdn,
(case
when townname is null or trim(townname) = '' then site_name
when site_name is null or trim(site_name) = '' then townname
else concat_ws(',',nvl(townname,''),nvl(site_name,''))
end) as adresse,
row_number() over(partition by msisdn order by last_location_day desc nulls last) as rn
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY
where event_date='###SLICE_VALUE###') t1 where rn = 1
) d on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(d.msisdn))