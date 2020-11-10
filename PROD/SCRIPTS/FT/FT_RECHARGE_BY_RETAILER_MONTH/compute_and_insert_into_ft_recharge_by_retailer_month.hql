insert into MON.SPARK_FT_RECHARGE_BY_RETAILER_MONTH 
select
    t1.msisdn msisdn
    , t1.canaux canaux
    , t1.Direction Direction
    , t1.event event
    , t1.canal_type canal_type
    , t1.canal_event canal_event
    , t1.amount amount
    , t1.Commission Commission
    , t1.Revenue Revenue
    , t1.transaction_count transaction_count
    , t1.other_party_count other_party_count
    , t1.Source source
    , CURRENT_TIMESTAMP() insert_date
    , nvl(b.category, t1.category_pos) category_pos 
    , t1.event_month event_month
from (select * from TMP.REF_CATEGORY_POS) b
full outer join (
    select 
        sender_msisdn as Msisdn
        , canaux
        , 'OUT' Direction
        , a.Refill_mean event
        , canal_type
        , canal_event
        , sum(refill_amount) amount
        , sum(nvl(commission, 0)) Commission
        , 0 Revenue
        , count(*) transaction_count
        , count(distinct receiver_msisdn) other_party_count
        , 'FT_REFILL' Source
        , CURRENT_TIMESTAMP() insert_date
        , null category_pos
        , SUBSTR(refill_date,1,7) event_month
    from (
        select *
        from mon.spark_ft_refill
        where refill_date BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))
        and termination_ind = 200
    ) a
    inner join dim.dt_send_rec_cat_refill b
    on( 
        a.refill_mean = b.refill_mean
        and a.refill_type = b.refill_type
        and a.sender_category = b.sender_category
        and nvl(a.receiver_category, 'CUST') = b.receiver_category
    )
    group by SUBSTR(refill_date,1,7)
    , sender_msisdn
    , canaux
    , a.Refill_mean
    , canal_type
    , canal_event   
    union
    select 
        RECEIVER_MSISDN as msisdn
        , CANAUX
        , 'IN' DIRECTION
        , a.Refill_mean EVENT
        , CANAL_TYPE
        , CANAL_EVENT
        , SUM(REFILL_AMOUNT) AMOUNT
        , SUM(NVL(COMMISSION, 0)) COMMISSION
        , 0 Revenue
        , COUNT(*) TRANSACTION_COUNT
        , COUNT(DISTINCT SENDER_MSISDN) OTHER_PARTY_COUNT   
        , 'FT_REFILL' Source
        , CURRENT_TIMESTAMP() insert_date
        , null category_pos
        , SUBSTR(refill_date,1,7) 
    from (
        select * from mon.spark_ft_refill
        where refill_date BETWEEN TO_DATE(CONCAT('###SLICE_VALUE###','-01')) AND LAST_DAY(CONCAT('###SLICE_VALUE###','-01'))
        and termination_ind = 200
    ) a
    inner join dim.dt_send_rec_cat_refill b
    on ( 
        a.refill_mean = b.refill_mean
        and a.refill_type = b.refill_type
        and a.sender_category = b.sender_category
        and nvl(a.receiver_category, 'CUST') = b.receiver_category
        and a.Refill_mean <> 'C2S'
    )
    group by SUBSTR(refill_date,1,7), RECEIVER_MSISDN
    , CANAUX
    , a.Refill_mean 
    , CANAL_TYPE
    , CANAL_EVENT
) t1
ON ( 
    t1.amount >= b.minim
    and t1.amount < b.maxim
    and t1.event = 'C2S'
    and canal_type = 'C2S_OCM_RC'
)
