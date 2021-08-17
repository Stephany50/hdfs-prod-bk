insert into TMP.KYC_TT_ALIGN2
select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) as msisdn,a.prenom_om,a.nom_om,a.nom_prenom_om,
to_date(a.birth_date) as date_naissance_om
,a.numero_piece_om,
a.modified_on,a.registered_on as date_creation_om,a.user_id,
(case when trim(a.address) = '' or a.address is null then d.adresse
when trim(d.adresse) = '' or d.adresse is null then a.address
else d.adresse end) as addresse_om,
(case when b.statut = 1 then 'OUI'
when c.ORIGINAL_FILE_DATE >= '2019-10-15' then 'OUI'
when upper(trim(c.ETAT))='VALID' and upper(trim(c.ETATDEXPORTGLOBAL))='SUCCESS' then 'OUI'
WHEN upper(trim(c.ETAT))='INVALID' then 'NON'
else 'NON'
end) as statut_bo_om,
nvl(c.date_mise_a_jour_bo_nomad,b.date_validation_bo) as date_mise_a_jour_bo_om,
(case when b.statut = 1
then (case when b.dateexpire is null then nvl(d.id_expiry_date,c.expiration)
else  b.dateexpire end)
when c.ORIGINAL_FILE_DATE >= '2019-10-15'
then (case when c.expiration is null then nvl(d.id_expiry_date,b.dateexpire)
else c.expiration end)
when d.id_expiry_date is null then nvl(c.expiration,b.dateexpire)
else d.id_expiry_date
end) as date_expiration_om
from
(select msisdn,upper(trim(user_first_name)) as prenom_om,upper(trim(user_last_name)) as nom_om,
upper(trim(concat_ws(' ',nvl(user_last_name,''),nvl(user_first_name,'')))) as nom_prenom_om,
birth_date,modified_on,registered_on,user_id,upper(trim(id_number)) as numero_piece_om,
address
from Mon.spark_ft_omny_account_snapshot
where event_date='###SLICE_VALUE###'
and upper(trim(user_type)) = 'SUBSCRIBER') a
left join MON.SPARK_FT_MYOMID b on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.phone_tango))
left join (select
FN_FORMAT_MSISDN_TO_9DIGITS(trim(telephone)) as msisdn,
ORIGINAL_FILE_DATE,etat,etatdexportglobal,
nvl((CASE
WHEN trim(last_update_date) IS NULL OR trim(last_update_date) = '' THEN NULL
WHEN trim(last_update_date) like '%/%'
THEN  cast(translate(SUBSTR(trim(last_update_date), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(last_update_date) like '%-%' THEN  cast(SUBSTR(trim(last_update_date), 1, 19) AS TIMESTAMP)
ELSE NULL
END),
(CASE
WHEN trim(majle) IS NULL OR trim(majle) = '' THEN NULL
WHEN trim(majle) like '%/%'
THEN  cast(translate(SUBSTR(trim(majle), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(majle) like '%-%' THEN  cast(SUBSTR(trim(majle), 1, 19) AS TIMESTAMP)
ELSE NULL
END)) as date_mise_a_jour_bo_nomad,
(CASE
WHEN trim(expiration) IS NULL OR trim(expiration) = '' THEN NULL
WHEN trim(expiration) LIKE '%-%'
THEN cast(substr(trim(expiration),1,10) as DATE)
WHEN trim(expiration) LIKE '%/%'
THEN cast(translate(substr(trim(expiration),1,10),'/','-') as DATE)
ELSE NULL
END) expiration
from (
select telephone,ORIGINAL_FILE_DATE,last_update_date,majle,expiration,
etat,etatdexportglobal,typedecontrat
from cdr.spark_it_nomad_client_directory
union all
select telephone,ORIGINAL_FILE_DATE,last_update_date,majle,expiration,
etat,etatdexportglobal,typedecontrat
from cdr.spark_it_nomad_client_directory_dwh
) aa
where upper(trim(typedecontrat)) like '%MONEY%'
) c on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.msisdn))
left join (select msisdn,id_expiry_date,
(case when trim(address1) = '' or address1 is null then address2
when trim(address2) = '' or address2 is null then address1
else concat_ws(' ',nvl(address1,''),nvl(address2,'')) end) as adresse
from cdr.spark_it_om_subscribers
union all
select msisdn,id_expiry_date,
(case when trim(address1) = '' or address1 is null then address2
when trim(address2) = '' or address2 is null then address1
else concat_ws(' ',nvl(address1,''),nvl(address2,'')) end) as adresse
from CDR.SPARK_IT_OMNY_USER_REGISTRATION_DWH_PART
) d  on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(d.msisdn))