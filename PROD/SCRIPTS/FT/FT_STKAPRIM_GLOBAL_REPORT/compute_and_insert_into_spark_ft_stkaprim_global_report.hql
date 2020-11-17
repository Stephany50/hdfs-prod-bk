insert into mon.SPARK_FT_STKAPRIM_GLOBAL_REPORT
select
    nvl(b.region_commerciale, t3.region_commerciale) region_commerciale
    , nvl(b.region_administrative, t3.region_administrative) region_administrative
    , t3.partner_name partner_name 
    , t3.canaux canaux
    , t3.canal_type canal_type
    , t3.parent parent
    , t3.category_parent category_parent
    , t3.category_enfant category_enfant
    , t3.amount_parent amount_parent
    , t3.amount_cumul_parent amount_cumul_parent
    , t3.category_pos category_pos
    , t3.msisdn_count msisdn_count
    , t3.amount amount
    , t3.msisdn_cumul msisdn_cumul
    , t3.amount_cumul amount_cumul
    , CURRENT_TIMESTAMP() insert_date
    , t3.refill_date refill_date
from (select * from tmp.ref_routes_distri) b
full outer join (
    select
        t2.region_commerciale region_commerciale
        , t2.region_administrative region_administrative
        , t2.partner_name partner_name 
        , t2.canaux canaux
        , t2.canal_type canal_type
        , t2.parent parent
        , nvl(b.category_code, t2.category_parent) category_parent
        , t2.category_enfant category_enfant
        , t2.amount_parent amount_parent
        , t2.amount_cumul_parent amount_cumul_parent
        , t2.category_pos category_pos
        , t2.msisdn_count msisdn_count
        , t2.amount amount
        , t2.msisdn_cumul msisdn_cumul
        , t2.amount_cumul amount_cumul
        , CURRENT_TIMESTAMP() insert_date
        , t2.refill_date refill_date
    from (select * from dim.spark_dim_stk_hierachy) b
    full outer join (
        select
            t1.region_commerciale region_commerciale
            , t1.region_administrative region_administrative
            , t1.partner_name partner_name 
            , t1.canaux canaux
            , t1.canal_type canal_type
            , t1.parent parent
            , t1.category_parent category_parent
            , t1.category_enfant category_enfant
            , nvl(b.amount_day, t1.amount_parent) amount_parent
            , nvl(b.amount_cumul, t1.amount_cumul_parent) amount_cumul_parent
            , t1.category_pos category_pos
            , t1.msisdn_count msisdn_count
            , t1.amount amount
            , t1.msisdn_cumul msisdn_cumul
            , t1.amount_cumul amount_cumul
            , CURRENT_TIMESTAMP() insert_date
            , t1.refill_date refill_date
        from (
            select 
                canaux
                , msisdn
                , canal_type
                , sum(amount +nvl(commission, 0)) Amount_cumul
                , sum(case when refill_date = '###SLICE_VALUE###' then amount+nvl(commission, 0) end ) amount_day
                , '###SLICE_VALUE###' refill_date
            from mon.spark_ft_recharge_by_retailer
            where refill_date = '###SLICE_VALUE###' 
            and ((canal_type in ('C2C_DW_TRANSFERT', 'O2C_ND') and direction = 'IN') or event = 'C2S')
            group by canaux, msisdn, canal_type
        ) b
        full outer join (
            select
                null region_commerciale
                , null region_administrative
                , null partner_name
                , canaux
                , null canal_type
                , nvl(parent, b.msisdn) parent
                , null category_parent
                , null category_enfant
                , 0 amount_parent
                , 0 amount_cumul_parent
                , category_msisdn category_pos
                , count(distinct case when refill_date = '###SLICE_VALUE###' then a.msisdn end ) msisdn_count
                , sum(case when refill_date = '###SLICE_VALUE###' then amount_com end ) amount
                , count(distinct a.msisdn ) msisdn_cumul
                , sum(amount_com) amount_cumul
                , '###SLICE_VALUE###' refill_date
            from (
                select 
                    canaux
                    , msisdn
                    , category_msisdn
                    , canal_type
                    , sum(amount +nvl(commission, 0)) Amount_com
                    , refill_date
                from mon.spark_ft_recharge_by_retailer
                where refill_date = '###SLICE_VALUE###'
                and canal_type in ('C2C_DW_TRANSFERT', 'O2C_ND')
                and direction = 'IN'
                group by refill_date, canaux, msisdn, category_msisdn, canal_type
            ) a
            inner join dim.spark_dim_stk_hierachy b
            on a.msisdn = b.msisdn
            and refill_date >= activ_begin_date
            and refill_date < nvl(activ_end_date, CURRENT_DATE())
            group by canaux, partner_name, canal_type, nvl(parent, b.msisdn), category_msisdn
            union all
            select 
                null region_commerciale
                , null region_administrative
                , partner_name
                , canaux 
                , canal_type
                , nvl(parent, b.msisdn) parent
                , null category_parent
                , null category_enfant
                , 0 amount_parent
                , 0 amount_cumul_parent
                , category_msisdn
                , count(distinct case when refill_date = '###SLICE_VALUE###' then a.msisdn end ) msisdn_count
                , sum(case when refill_date = '###SLICE_VALUE###' then amount end ) amount
                , count(distinct a.msisdn ) msisdn_cumul
                , sum(amount) amount_cumul
                , '###SLICE_VALUE###' refill_date
            from (
                select 
                    a.*
                    , category_pos
                from(
                    select 
                        canaux
                        , msisdn
                        , canal_type
                        , category_msisdn
                        , sum(amount) amount
                        , refill_date
                    from mon.spark_ft_recharge_by_retailer
                    where refill_date = '###SLICE_DATE###' 
                    and canal_type = 'C2S_VAS_RC'
                    and event = 'C2S'
                    and canal_type <> 'C2S_OM_RC'
                    group by refill_date, canaux, msisdn, canal_type, category_msisdn
                ) a
                left join (
                    select 
                        event_month
                        , msisdn
                        , category_pos
                    from mon.SPARK_FT_RECHARGE_BY_RETAILER_MONTH
                    where event_month = substr(add_months('###SLICE_VALUE###',-1),1,7)
                    and event = 'C2S'
                    and canal_type = 'C2S_OCM_RC'
                ) b
                on a.msisdn = b.msisdn
            ) a
            inner join dim.spark_dim_stk_hierachy b
            on a.msisdn = b.msisdn
            and '###SLICE_VALUE###' >= activ_begin_date
            and '###SLICE_VALUE###' < nvl(activ_end_date, CURRENT_DATE())
            group by canaux, partner_name, canal_type, nvl(parent, b.msisdn), category_msisdn, category_pos
        ) t1
        on ( 
            b.msisdn = t1.parent
            and t1.refill_date = b.refill_date
            and t1.canal_type = b.canal_type
            and t1.category_enfant = 'ORNGPTNR'
        )
    ) t2
    on ( 
        t2.parent = b.msisdn
        and t2.refill_date >= activ_begin_date
        and t2.refill_date < nvl(activ_end_date, CURRENT_DATE())
        and refill_date = '###SLICE_VALUE###' --'28/09/2020'
    )
) t3
on (
    t3.parent = b.stkaprim
    and refill_date = '###SLICE_VALUE###'
)
