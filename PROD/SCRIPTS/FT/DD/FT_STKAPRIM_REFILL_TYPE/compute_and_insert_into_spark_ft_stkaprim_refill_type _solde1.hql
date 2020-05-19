insert into TMP.TT_STKAPRIM_REFILL_TYPE
select
case when parent_to_consider = 'MSISDN' then MSISDN
when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
else nvl(parent, grdparent)
end stkaprim
,partner_name partenaire
,  case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
when parent_to_consider = 'PARENT' then 'Solde_Route'
else 'Solde_Route'
end Type_Event
, 'Solde' Event_Simplifie
, sum(amount) amount
,null commission
,null capilarite
,null capat_cumul
, 'FT_REFILL_SOLDE'  AS SOURCE
,CURRENT_TIMESTAMP AS INSERT_DATE
,refill_date
from
(
select '###SLICE_VALUE###' as refill_date, sender_msisdn as msisdn, category_code, parent, grdparent, amount,b.partner_name partner_name
from
(
select distinct refill_date, sender_msisdn,
first_value(sender_post_bal) over (partition by refill_date, sender_msisdn order by refill_date, refill_time desc) amount
from MON.SPARK_ft_refill
where refill_date = DATE_SUB('###SLICE_VALUE###',1)
and termination_ind = '200'
) a
LEFT JOIN
(
select * from DIM.SPARK_DIM_STK_HIERACHY
where '###SLICE_VALUE###'  >= activ_begin_date
and '###SLICE_VALUE###' < nvl(activ_end_date,  '2025-12-31' )
) b
ON a.sender_msisdn = b.msisdn
) T
LEFT JOIN
( select distinct sender_category, parent_to_consider from dim.dt_send_rec_cat_refill ) c
where T.category_code = c.sender_category
group by
refill_date
, case when parent_to_consider = 'MSISDN' then MSISDN
when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
else nvl(parent, grdparent)
end
,  case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
when parent_to_consider = 'PARENT' then 'Solde_Route'
else 'Solde_Route'
end,partner_name