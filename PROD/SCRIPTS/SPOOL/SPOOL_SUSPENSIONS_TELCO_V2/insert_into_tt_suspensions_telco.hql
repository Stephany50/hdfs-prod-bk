insert into TMP.TT_SUSPENSION_TELCO
select a.date_suspension, a.msisdn,
d.valide_bo_telco,
b.activation_date as date_activation,
c.site_name, c.townname, c.administrative_region, c.commercial_region, c.last_location_day,
a.statut as statut_zsmart,
upper(trim(a.raison_statut)) as raison_statut,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from (
select a1.*,
to_date(a2.date_changement_statut) as date_suspension, a2.statut, a2.raison_statut
from
(select *
from MON.SPARK_FT_BDI
where event_date = '###SLICE_VALUE###'
) a1 join (select * from (
select
msisdn,
date_changement_statut,
statut,
row_number() over(partition by msisdn order by date_changement_statut desc nulls last ) as rang
from TMP.tt_zsmart2
where upper(trim(statut)) = 'SUSPENDU') d1 where rang = 1) a2 on fn_format_msisdn_to_9digits(trim(a1.msisdn)) = fn_format_msisdn_to_9digits(trim(a2.msisdn))
) a
left join
(select msisdn, activation_date
from (
select msisdn, activation_date,
row_number() over(partition by msisdn order by activation_date desc nulls last) as rang
from (
select trim(access_key) as msisdn,
activation_date
from MON.SPARK_FT_CONTRACT_SNAPSHOT
where event_date = date_add('###SLICE_VALUE###',1)) a1
) a2 where rang = 1) b
on fn_format_msisdn_to_9digits(trim(a.msisdn)) = fn_format_msisdn_to_9digits(trim(b.msisdn))
left join
(select
msisdn, site_name, townname, administrative_region, commercial_region, last_location_day
from (
select msisdn, site_name, townname,
administrative_region, commercial_region, last_location_day,
row_number() over(partition by msisdn order by last_location_day desc nulls last ) as rang
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY
where event_date = '###SLICE_VALUE###' ) c1
where rang = 1) c on fn_format_msisdn_to_9digits(trim(a.msisdn)) = fn_format_msisdn_to_9digits(trim(c.msisdn))
LEFT JOIN (SELECT MSISDN,est_snappe as valide_bo_telco
FROM DIM.SPARK_DT_BASE_IDENTIFICATION) d
ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(d.msisdn))
where  a.date_suspension >='2020-01-01'
order by date_suspension desc