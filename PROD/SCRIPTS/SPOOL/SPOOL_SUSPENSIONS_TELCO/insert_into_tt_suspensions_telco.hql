insert into TMP.TT_SUSPENSION_TELCO
select to_date(a.date_changement_statut) as date_suspension, a.msisdn,
a.statut_validation_bo,
a.statut_validation_boo, b.activation_date as date_activation, a.date_validation_bo,
c.site_name, c.townname, c.administrative_region, c.commercial_region, c.last_location_day,
d.statut as statut_zm,
d.date_validation_bo as date_validation_bo_zm,
d.statut_validation_bo as statut_validation_bo_zm,
e.order_reason,
e.block_reason,
current_timestamp() as insert_date,
'###SLICE_VALUE###' as event_date
from(
select  * from (
select x1.*,x2.* from
(select *
from MON.SPARK_FT_BDI
where event_date = '###SLICE_VALUE###'
and upper(trim(statut_bscs)) = 'SUSPENDU'
) x1,
(select max(date_suspension) as date_suspension_max
from SPOOL.SPARK_SPOOL_SUSPENSIONS_TELCO
where event_date = date_sub('###SLICE_VALUE###',1)) x2
) x3 where to_date(date_changement_statut) > date_suspension_max
)a
left join
(select msisdn, activation_date
from (
select msisdn, activation_date,
row_number() over(partition by msisdn order by activation_date desc nulls last) as rang
from (
select fn_format_msisdn_to_9digits(trim(access_key)) as msisdn,
activation_date
from MON.SPARK_FT_CONTRACT_SNAPSHOT
where event_date = '###SLICE_VALUE###') a
) b where rang = 1) b
on fn_format_msisdn_to_9digits(trim(a.msisdn)) = trim(b.msisdn)
left join
(select
msisdn, site_name, townname, administrative_region, commercial_region, last_location_day
from (
select fn_format_msisdn_to_9digits(msisdn) as msisdn, site_name, townname,
administrative_region, commercial_region, last_location_day,
row_number() over(partition by fn_format_msisdn_to_9digits(msisdn) order by last_location_day desc nulls last ) as rang
from MON.SPARK_FT_CLIENT_LAST_SITE_DAY
where event_date = '###SLICE_VALUE###' ) a
where rang = 1) c on fn_format_msisdn_to_9digits(trim(a.msisdn)) = trim(c.msisdn)
left join (select * from (
select
fn_format_msisdn_to_9digits(trim(msisdn)) as msisdn,
date_validation_bo,
date_activation,
date_changement_statut,
statut_validation_bo,
statut,
row_number() over(partition by fn_format_msisdn_to_9digits(msisdn) order by date_activation desc nulls last ) as rang
from TMP.tt_zsmart2) y where rang = 1) d on fn_format_msisdn_to_9digits(trim(a.msisdn)) = trim(d.msisdn)
left join (select msisdn, order_reason,block_reason from (
select fn_format_msisdn_to_9digits(trim(accnbr)) as msisdn,
trim(order_reason) as order_reason,
trim(block_reason) as block_reason,
row_number() over(partition by fn_format_msisdn_to_9digits(trim(accnbr))  order by update_date desc nulls last) as rang
from CDR.SPARK_IT_CONT
where original_file_date in (select max(original_file_date) from CDR.SPARK_IT_CONT)
) vv where rang = 1) e
on fn_format_msisdn_to_9digits(trim(a.msisdn)) = trim(e.msisdn)
order by date_suspension desc