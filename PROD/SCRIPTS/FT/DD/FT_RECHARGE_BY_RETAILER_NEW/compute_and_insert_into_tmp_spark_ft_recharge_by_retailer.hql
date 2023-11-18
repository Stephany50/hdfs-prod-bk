insert into DD.TMP_SPARK_FT_RECHARGE_BY_RETAILER
select
    MSISDN 
    , CANAUX 
    , DIRECTION
    , EVENT
    , CANAL_TYPE
    , CANAL_EVENT 
    , AMOUNT
    , COMMISSION
    , REVENUE
    , TRANSACTION_COUNT
    , OTHER_PARTY_COUNT
    , SOURCE
    , INSERT_DATE
    , REFILL_DATE
    , CATEGORY_MSISDN
    , PARENT 
    , GRDPARENT
    , PARTNER_NAME
    --, '' SITE
from 
    (   select 
            a.msisdn msisdn 
            , a.canaux canaux 
            , a.direction direction
            , a.event event
            , '' canal_type
            , a.canal_event canal_event 
            , a.amount amount
            , a.commission commission
            , a.revenue revenue
            , a.transaction_count transaction_count
            , a.subscriber_count other_party_count
            , a.source source
            , a.insert_date INSERT_DATE
            , a.refill_date refill_date
            , b.user_grade_name CATEGORY_MSISDN
            , b.parent_user_msisdn PARENT 
            , b.owner_msisdn GRDPARENT
            , b.partner_name PARTNER_NAME
        from (
            select  MSISDN, Canaux, DIRECTION, Event, Canal_Event, AMOUNT, subscriber_count, transaction_count, Source, insert_date, revenue, commission, refill_date            
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
                    , 0 revenue
                    , sum(COMMISSIONS_PAID) as commission
                    , TRANSFER_DATETIME refill_date
                from cdr.spark_it_omny_transactions
                where transfer_datetime between to_date('###SLICE_VALUE###'||' 000000')
                    and to_date('###SLICE_VALUE###'||' 235959')
                    and service_type = 'CASHIN'
                    and receiver_category_code = 'SUBS'
                    and TRANSFER_STATUS='TS'
                group by TRANSFER_DATETIME, sender_msisdn, service_type
            ) 
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
                    , sum(service_charge_received) revenue
                    , sum(COMMISSIONS_PAID) as commission
                    , TRANSFER_DATETIME refill_date
                from cdr.spark_it_omny_transactions
                where transfer_datetime between to_date('###SLICE_VALUE###'||' 000000')
                    and to_date('###SLICE_VALUE###'||' 235959')
                    and service_type = 'CASHOUT'
                    and sender_category_code = 'SUBS'
                    and TRANSFER_STATUS='TS'
                group by TRANSFER_DATETIME, receiver_msisdn, service_type
            )
        ) a
        inner join 
        (select a.MSISDN, ACTIV_BEGIN_DATE, ACTIV_END_DATE, USER_GRADE_NAME, PARENT_USER_MSISDN, OWNER_MSISDN, 
            NVL(PARTNER_ZONE_LIB, PARTNER_NAME_DD), PARTNER_NAME, PARTNER_TYPE from
            (
                select * from DIM.REF_PDVOM_HIERARCHY x
                join (select * from DIM.REF_PARTNER_OM) y 
                on x.OWNER_LAST_NAME = y.PARTNER_NAME
            ) a
            join (select * from DIM.REF_ROUTES_DISTRI) b
            on a.OWNER_MSISDN = b.STKAPRIM
        )b
        on ( a.msisdn = b.msisdn
        and a.refill_date >= b.activ_begin_date
        and a.refill_date < nvl(b.activ_end_date, CURRENT_DATE())
        and a.refill_date = '###SLICE_VALUE###')
    )
    union all
    (   select
        a.msisdn msisdn 
        , a.canaux canaux 
        , a.direction direction
        , a.event event
        , a.canal_type
        , a.canal_event canal_event 
        , a.amount amount
        , a.commission commission
        , a.revenue revenue
        , a.transaction_count transaction_count
        , a.other_party_count other_party_count
        , a.source source
        , a.insert_date INSERT_DATE
        , a.refill_date refill_date
        , b.category_code CATEGORY_MSISDN
        , NVL(b.parent, b.msisdn) PARENT 
        , b.grdparent GRDPARENT
        , b.partner_name PARTNER_NAME
    from (
        select
            a.sender_msisdn as msisdn
            , canaux
            , 'OUT' Direction
            , a.Refill_mean event
            , canal_type
            , canal_event
            , sum(a.refill_amount) amount
            , sum(nvl(a.commission, 0)) Commission
            , 0 Revenue
            , count(*) transaction_count
            , count(distinct a.receiver_msisdn) other_party_count
            , 'FT_REFILL' Source
            , CURRENT_TIMESTAMP() insert_date
            --, null category_msisdn
            , a.refill_date
        from (
            select distinct * from MON.SPARK_FT_REFILL
            where refill_date = '###SLICE_VALUE###' 
            and termination_ind = 200
        ) a
        inner join DIM.REF_SEND_REC_CAT_REFILL b
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
            --, null category_msisdn
            , a.refill_date
        from ( 
            select * from MON.SPARK_FT_REFILL
            where refill_date = '###SLICE_VALUE###'  
            and termination_ind = 200
        ) a
        inner join DIM.REF_SEND_REC_CAT_REFILL b
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
    ) a 
    left join 
    (select * from DIM.SPARK_STK_HIERACHY) b 
    on ( a.msisdn = b.msisdn
    and a.refill_date >= b.activ_begin_date
    and a.refill_date < nvl(b.activ_end_date, CURRENT_DATE())
    and a.refill_date = '###SLICE_VALUE###')
    )
    union all
    (   select
       c.msisdn msisdn 
        , c.canaux canaux 
        , c.direction direction
        , c.event event
        , c.canal_type
        , c.canal_event canal_event 
        , c.amount amount
        , 0 commission
        , 0 revenue
        , c.transaction_count transaction_count
        , '' other_party_count
        , c.source source
        , c.insert_date INSERT_DATE
        , c.refill_date refill_date
        , '' CATEGORY_MSISDN
        , '' PARENT 
        , '' GRDPARENT
        , '' PARTNER_NAME
        from ( 
            select refill_date, '699999998' msisdn, 'DISTRIBUTOR' canaux, 'OUT' direction, 'C2S' event, 'C2S_SCRATCH_RC' canal_type, 'C2S_SCRATCH_OCM_CUST' canal_event , sum(refill_amount) as amount, sum(transaction_count) as transaction_count,'FT_RETAIL_BASE_DETAILLANT' source, CURRENT_TIMESTAMP() insert_date
                from MON.SPARK_FT_RETAIL_BASE_DETAILLANT 
                where refill_date = '###SLICE_VALUE###'   
                and category = 'SCRATCH'   
                group by refill_date
            union
            select refill_date, '699999999' msisdn, 'DISTRIBUTOR' canaux, 'OUT' direction, 'C2S' event, 'C2S_DATAVIAOM_RC' canal_type, 'C2S_DATAVIAOM_OM_CUST' canal_event, sum(refill_amount) as amount, sum(transaction_count) as transaction_count, 'FT_RETAIL_BASE_DETAILLANT' source, CURRENT_TIMESTAMP() insert_date
            from MON.SPARK_FT_RETAIL_BASE_DETAILLANT
            where refill_date = '###SLICE_VALUE###'
            and source_type = 'SUBS'
            group by refill_date
        ) c
    )