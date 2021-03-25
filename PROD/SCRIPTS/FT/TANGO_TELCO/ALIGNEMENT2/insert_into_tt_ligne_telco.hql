insert into TMP.tt_align1
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) as msisdn,upper(trim(a.nom)) as nom_telco,upper(trim(a.prenom)) as prenom_telco,
upper(trim(a.nom_prenom)) as nom_prenom_telco,
a.date_naissance as date_naissance_telco,upper(trim(a.numero_piece)) as numero_piece_telco,
a.est_suspendu,a.odbincomingcalls,a.odboutgoingcalls,a.statut_bscs,a.date_activation,a.date_changement_statut,
a.date_expiration,
upper(trim(d.adresse)) as adresse,
(case when trim(b.est_snappe)='OUI' then 'VALIDE'
when trim(b.est_snappe) = 'NON' then 'REJETE'
when trim(b.est_snappe) = 'UNKNOWN' then 'NON VERIFIE'
else 'INCONNU'
end) as statut_validation_bo_telco,
nvl(c.last_update_date,b.date_mise_a_jour) as date_mise_a_jour_bo_telco
from
(select msisdn,nom,prenom,upper(concat_ws(' ',nvl(nom,''),nvl(prenom,''))) as nom_prenom,
date_naissance,numero_piece,est_suspendu,statut_bscs,date_activation,date_changement_statut,
odbincomingcalls,odboutgoingcalls,date_expiration
from Mon.spark_ft_bdi_1A
where event_date = '###SLICE_VALUE###') a
left join (select msisdn,est_snappe,(CASE
WHEN trim(date_mise_a_jour) IS NULL OR trim(date_mise_a_jour) = '' THEN NULL
WHEN trim(date_mise_a_jour) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_mise_a_jour), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_mise_a_jour) like '%-%' THEN  cast(SUBSTR(trim(date_mise_a_jour), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_mise_a_jour
from dim.spark_dt_base_identification) b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.msisdn))
left join (
(select telephone,(CASE
WHEN trim(last_update_date) IS NULL OR trim(last_update_date) = '' THEN NULL
WHEN trim(last_update_date) like '%/%'
THEN  cast(translate(SUBSTR(trim(last_update_date), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(last_update_date) like '%-%' THEN  cast(SUBSTR(trim(last_update_date), 1, 19) AS TIMESTAMP)
ELSE NULL
END) last_update_date
from cdr.spark_it_nomad_client_directory)
union all
(select telephone,(CASE
WHEN trim(last_update_date) IS NULL OR trim(last_update_date) = '' THEN NULL
WHEN trim(last_update_date) like '%/%'
THEN  cast(translate(SUBSTR(trim(last_update_date), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(last_update_date) like '%-%' THEN  cast(SUBSTR(trim(last_update_date), 1, 19) AS TIMESTAMP)
ELSE NULL
END) last_update_date
from cdr.spark_it_nomad_client_directory_dwh)
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