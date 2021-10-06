insert into mon.SPARK_FT_RECHARGE_BY_RETAILER
select
    t.msisdn msisdn 
    , t.canaux canaux 
    , t.direction direction
    , t.event event
    , t.canal_type canal_type
    , t.canal_event canal_event 
    , t.amount amount
    , t.commission commission
    , t.revenue revenue
    , t.transaction_count transaction_count
    , t.other_party_count other_party_count
    , t.source source
    , CURRENT_TIMESTAMP() INSERT_DATE
    , nvl(b.category_code, t.category_msisdn) category_msisdn
    , t.refill_date refill_date
from (select * from dim.spark_dim_stk_hierachy) b
full outer join (
    select
        a.msisdn msisdn 
        , a.canaux canaux 
        , a.direction direction
        , a.event event
        , null canal_type
        , a.canal_event canal_event 
        , a.amount amount
        , a.commission commission
        , a.revenu revenue
        , a.transaction_count transaction_count
        , a.subscriber_count other_party_count
        , a.source source
        , CURRENT_TIMESTAMP() INSERT_DATE
        , null category_msisdn
        , a.jour refill_date
    from (
        select e.*    --JOUR, sender_msisdn, service_type, VAL
        from (
            select 
                sender_msisdn as MSISDN
                , 'DISTRIBUTOR' Canaux
                , 'OUT' DIRECTION
                , 'OM' Event
                , service_type as Canal_Event
                , SUM(TRANSACTION_AMOUNT) AMOUNT
                , count(distinct receiver_msisdn) subscriber_count
                , count(*) transaction_count
                , 'IT_OMNY_TRANSACTIONS' Source
                , CURRENT_DATE() insert_date
                , 0 revenu
                , sum(COMMISSIONS_PAID) as commission
                , TRANSFER_DATETIME JOUR
            from cdr.spark_it_omny_transactions
            where TRANSFER_DATETIME between to_date('###SLICE_VALUE###'||' 000000')
            and to_date('###SLICE_VALUE###'||' 235959')
            and service_type = 'CASHIN'
            and receiver_category_code = 'SUBS'
            and TRANSFER_STATUS='TS'
            group by TRANSFER_DATETIME, sender_msisdn, service_type
        ) e
        union
        (
            select 
                receiver_msisdn as msisdn
                , 'DISTRIBUTOR' Canaux
                , 'IN' DIRECTION
                , 'OM' Event
                , service_type as Canal_Event
                , SUM(TRANSACTION_AMOUNT) AMOUNT
                , count(distinct sender_msisdn) subscriber_count
                , count(*) transaction_count 
                , 'IT_OMNY_TRANSACTIONS' Source
                , CURRENT_TIMESTAMP() insert_date
                , sum(service_charge_received) revenu
                , sum(COMMISSIONS_PAID) as commission
                , TRANSFER_DATETIME JOUR
            from cdr.spark_it_omny_transactions
                where transfer_datetime between to_date('###SLICE_VALUE###'||' 000000')
                and to_date('###SLICE_VALUE###'||' 235959')
                and service_type = 'CASHOUT'
                and sender_category_code = 'SUBS'
                and TRANSFER_STATUS='TS'
                group by TRANSFER_DATETIME, receiver_msisdn, service_type
        )
    ) a
    union all
    select
        a.sender_msisdn as Msisdn
        , canaux
        , 'OUT' Direction
        , a.Refill_mean event
        , canal_type
        , canal_event
        , sum(a.refill_amount) amount
        , sum(nvl(a.commission, 0)) Commission
        , 0 Revenue
        , count(*) transaction_count
        , count(distinct a.receiver_msisdn) 
        , 'FT_REFILL' Source
        , CURRENT_TIMESTAMP() insert_date
        , null category_msisdn
        , a.refill_date
    from (
        select *
        from mon.spark_ft_refill
        where refill_date = '###SLICE_VALUE###'   --'27/08/2020'
        and termination_ind = 200
    ) a
    inner join dim.dt_send_rec_cat_refill b
    on( 
        a.refill_mean = b.refill_mean
        and a.refill_type = b.refill_type
        and a.sender_category = b.sender_category
        and nvl(a.receiver_category, 'CUST') = b.receiver_category
    )
    group by 
    a.refill_date
    , a.sender_msisdn
    , canaux
    , a.Refill_mean
    , canal_type
    , canal_event 
    union
    select 
        a.RECEIVER_MSISDN as msisdn
        , CANAUX
        , 'IN' DIRECTION
        , a.Refill_mean EVENT
        , CANAL_TYPE
        , CANAL_EVENT
        , SUM(a.REFILL_AMOUNT) AMOUNT
        , SUM(NVL(a.COMMISSION, 0)) COMMISSION
        , 0 Revenue
        , COUNT(*) TRANSACTION_COUNT
        , COUNT(DISTINCT a.SENDER_MSISDN) OTHER_PARTY_COUNT   
        , 'FT_REFILL' Source
        , CURRENT_TIMESTAMP() insert_date
        , null category_msisdn
        , a.REFILL_DATE
    from ( 
        select * from mon.spark_ft_refill 
        where refill_date = '###SLICE_VALUE###'   --'27/08/2020'  
        and termination_ind = 200
    ) a
    inner join dim.dt_send_rec_cat_refill b
    on(
        a.refill_mean = b.refill_mean
        and a.refill_type = b.refill_type
        and a.sender_category = b.sender_category
        and nvl(a.receiver_category, 'CUST') = b.receiver_category
        and a.Refill_mean <> 'C2S'
    )
    group by
    a.REFILL_DATE
    , a.RECEIVER_MSISDN
    , CANAUX
    , a.Refill_mean
    , CANAL_TYPE
    , CANAL_EVENT
) t
on ( 
    t.msisdn = b.msisdn
    and t.refill_date >= b.activ_begin_date
    and t.refill_date < nvl(b.activ_end_date, CURRENT_DATE())
    and t.refill_date = '###SLICE_VALUE##' --'28/09/2020'
)