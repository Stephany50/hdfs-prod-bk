INSERT INTO TMP.TT_STKAPRIM_REFILL_TYPE
 select
case when parent_to_consider = 'MSISDN' then MSISDN
when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
else nvl(parent, grdparent) end stkaprim,
case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
when parent_to_consider = 'PARENT' then 'Solde_Route'
else 'Solde_Route'
end Type_Event,
'Solde' Event_Simplifie,
sum(amount) amount,
'FT_REFILL_SOLDE' SOURCE,
refill_date
from
(
select
a.sender_msisdn as msisdn,
category_code,
parent,
grdparent,
amount,
'2020-04-05' AS refill_date
from
(
select
distinct refill_date,
sender_msisdn,
first_value(sender_post_bal) over (partition by refill_date, sender_msisdn order by refill_date, refill_time desc) amount
from MON.SPARK_FT_REFILL
where refill_date = DATE_SUB('2020-04-05',1)
and termination_ind = '200'
) a
left join
(
select * from DIM.DIM_STK_HIERACHY
where '2020-04-05'  >= TO_DATE(activ_begin_date)
and '2020-04-05' < nvl(TO_DATE(activ_end_date),'2025-12-31')
) b
ON a.sender_msisdn =b.msisdn
) c
left join
( select distinct sender_category, parent_to_consider from dim.dt_send_rec_cat_refill ) d
ON c.category_code = d.sender_category
group by
refill_date
,case when parent_to_consider = 'MSISDN' then MSISDN
when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
else nvl(parent, grdparent)
end
,case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
when parent_to_consider = 'PARENT' then 'Solde_Route'
else 'Solde_Route' end