
insert into mon.spark_ft_msisdn_bal_ppm
select
    msisdn
    , bal_id
    , (nvl(bal_revenu, 0) - nvl(revenu_already_dispatched, 0))/(nvl(estimated_conso_to_end, 0) - nvl(sum_conso_until_yesterday, 0)) ppm
    , revenu_already_dispatched  -- Svalorisé[i-1]
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        a.msisdn
        , a.bal_id
        , (nvl(revenu_already_dispatched_yesterday, 0) + nvl(ppm_yesterday, 0)*nvl(conso_of_yesterday, 0)) revenu_already_dispatched -- Svalorisé[i-1]
        , nvl(bal_life_time, 0)*nvl(sum_conso_until_day, 0)/index_of_day estimated_conso_to_end -- Consô[i]
        , bal_revenu -- Nouveau Prix bundle
        , sum_conso_until_yesterday -- Sconso[i-1]
    from
    (
        select
            msisdn
            , bal_id
            , bal_revenu
            , bal_life_time
            , index_of_day
        from mon.spark_ft_msisdn_bal_constants
        where event_date = '###SLICE_VALUE###'
    ) a
    left join
    (
        select
            msisdn
            , bal_id
            , conso_of_yesterday
            , sum_conso_until_day
            , sum_conso_until_yesterday
        from mon.spark_ft_msisdn_bal_usage_day
        where event_date = '###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn and a.bal_id = b.bal_id
    left join
    (
        select
            msisdn
            , bal_id
            , revenu_already_dispatched revenu_already_dispatched_yesterday -- Svalorisé[i-2]
            , ppm ppm_yesterday -- ppm[i-1]
        from mon.spark_ft_msisdn_bal_ppm
        where event_date = date_sub('###SLICE_VALUE###', 1)
    ) c on a.msisdn = c.msisdn and a.bal_id = c.bal_id
) z
