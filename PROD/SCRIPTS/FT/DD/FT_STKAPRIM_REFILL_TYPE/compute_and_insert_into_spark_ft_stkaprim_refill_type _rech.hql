insert into TMP.SPARK_FT_STKAPRIM_REFILL_TYPE
select
case when road_to_consider in ( 'PARENT_SENDER', 'PARENT_RECEIVER')  then b.parent
when road_to_consider in ('SENDER', 'RECEIVER') then a.msisdn
end stkaprim,
partner_name partenaire,
type_event,
event_simplifie,
sum(amount) amount,
null commission,
null capilarite,
null capat_cumul,
 'FT_REFILL' AS Source,
 CURRENT_TIMESTAMP insert_date,
a.refill_date refill_date
from
(
select refill_date
, case when road_to_consider in ('SENDER', 'PARENT_SENDER')then Sender_msisdn
when road_to_consider in ('RECEIVER', 'PARENT_RECEIVER') then receiver_msisdn
end msisdn
, type_event,event_simplifie, road_to_consider, sum(amount) amount, null commission
from (select
refill_date,
sender_msisdn,
case when nvl(b.RECEIVER_CATEGORY, 'CUST') <> 'CUST' then RECEIVER_MSISDN else 'customer' end Receiver_msisdn,
type_event,
EVENT_SIMPLIFIE,
road_to_consider,
sum(refill_amount) amount,
null commission
from
(
select *
from MON.SPARK_ft_refill
where refill_date = '###SLICE_VALUE###'
and termination_ind = 200
) a
left join dim.dt_send_rec_cat_refill b
ON (a.refill_mean = b.refill_mean  and a.refill_type = b.refill_type  and a.sender_category = b.sender_category
and nvl(a.receiver_category, 'CUST') = b.receiver_category)
group by refill_date, sender_msisdn, type_event
, case when nvl(b.RECEIVER_CATEGORY, 'CUST') <> 'CUST' then RECEIVER_MSISDN else 'customer' end
, event_simplifie, road_to_consider) T
where refill_date = '###SLICE_VALUE###'
group by refill_date
, case when road_to_consider in ('SENDER', 'PARENT_SENDER')then Sender_msisdn
when road_to_consider in ('RECEIVER', 'PARENT_RECEIVER') then receiver_msisdn
end
, type_event, event_simplifie, road_to_consider
)   a
left join (select * from DIM.SPARK_DIM_STK_HIERACHY
where '###SLICE_VALUE###'  >= activ_begin_date
and '###SLICE_VALUE###' < nvl(activ_end_date,  '2025-12-31' )
) b
ON a.msisdn = b.msisdn
group by a.refill_date
, case when road_to_consider in ( 'PARENT_SENDER', 'PARENT_RECEIVER')  then b.parent
when road_to_consider in ('SENDER', 'RECEIVER') then a.msisdn
end
, type_event,event_simplifie,partner_name