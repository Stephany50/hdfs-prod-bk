insert into TMP.tt_align1
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
nvl(c.last_update_date,b.date_mise_a_jour) as date_mise_a_jour_bo_telco
from
(select a1.msisdn,
(case when a2.nom is null or trim(a2.nom) = '' then a1.nom else a2.nom end) as nom,
(case when a2.prenom is null or trim(a2.prenom) = '' then a1.prenom else a2.prenom end) as prenom,
nvl(a2.date_naissance,a1.date_naissance) as date_naissance,
(case when a2.numero_piece is null or trim(a2.numero_piece) = '' then a1.numero_piece else a2.numero_piece end) as numero_piece,
a1.est_suspendu,a1.statut_bscs,a1.date_activation,a1.date_changement_statut,
a1.odbincomingcalls,a1.odboutgoingcalls,
nvl(a2.date_expiration,a1.date_expiration) as date_expiration
from
(select *
from Mon.spark_ft_bdi
where event_date = '###SLICE_VALUE###') a1
left join (select *
from MON.SPARK_FT_BDI_CRM_B2C
where event_date = '###SLICE_VALUE###') a2
on trim(a1.msisdn) = trim(a2.msisdn)
) a
left join (select msisdn,est_snappe,(CASE
WHEN trim(date_mise_a_jour) IS NULL OR trim(date_mise_a_jour) = '' THEN NULL
WHEN trim(date_mise_a_jour) like '%/%'
THEN  cast(translate(SUBSTR(trim(date_mise_a_jour), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(date_mise_a_jour) like '%-%' THEN  cast(SUBSTR(trim(date_mise_a_jour), 1, 19) AS TIMESTAMP)
ELSE NULL
END) date_mise_a_jour
from TMP.TT_BASE_ID_DWH_1A) b
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