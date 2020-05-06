INSERT INTO MON.SPARK_FT_STKAPRIM_REFILL_TYPE
 select a.* from
(
select refill_date
, case when parent_to_consider = 'MSISDN' then MSISDN
when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
else nvl(parent, grdparent)
end stkaprim
,  case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
when parent_to_consider = 'PARENT' then 'Solde_Route'
else 'Solde_Route'
end Type_Event
, 'Solde' Event_Simplifie
, sum(amount) amount
, 'IT_ZEBRA_MASTER_BAL_SOLDE'  Source
, CURRENT_TIMESTAMP insert_date
from
(
select DATE_ADD(a.event_date,1) as refill_date, mobile_number as msisdn, category_code, parent, grdparent, AVAILABLE_BALANCE/100 as amount
from
(
select *
from CDR.SPARK_IT_ZEBRA_MASTER
where event_date = DATE_SUB('2020-04-12',1)
and event_time = '22'
) a
LEFT JOIN (select * from DIM.DIM_STK_HIERACHY
where '2020-04-12' >= activ_begin_date
and '2020-04-12' < nvl(activ_end_date, to_date('2025-12-31'))
)
ON a.mobile_number = b.msisdn
) c
LEFT JOIN
( select distinct sender_category, parent_to_consider from dim.dt_send_rec_cat_refill )  d
ON c.category_code = d.sender_category
group by
refill_date
, case when parent_to_consider = 'MSISDN' then MSISDN
when parent_to_consider = 'PARENT' then nvl(parent, grdparent)
else nvl(parent, grdparent)
end
,  case when parent_to_consider = 'MSISDN' then 'Solde_Parent'
when parent_to_consider = 'PARENT' then 'Solde_Route'
else 'Solde_Route'
end
) a
left join ( select distinct stkaprim, source  from TMP.TT_STKAPRIM_REFILL_TYPE ) b
where a.STKAPRIM = b.stkaprim
and amount > 0
and b.source is null;
else 'Solde_Route' end