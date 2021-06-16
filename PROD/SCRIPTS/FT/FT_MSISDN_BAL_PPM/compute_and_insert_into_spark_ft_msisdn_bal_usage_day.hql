
insert into mon.spark_ft_msisdn_bal_usage_day
select
    a.msisdn
    , a.bal_id
    , (nvl(conso_of_day, 0) - nvl(volume_erased_by_new_sub, 0) + nvl(sum_conso_until_yesterday, 0)) as sum_conso_until_day -- Sconso[i]
    , nvl(sum_conso_until_yesterday, 0) sum_conso_until_yesterday -- Sconso[i-1]
    , nvl(conso_of_yesterday, 0) conso_of_yesterday -- conso[i-1]
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        distinct msisdn
        , bal_id
    from mon.spark_ft_msisdn_da_status
    where event_date = '###SLICE_VALUE###'
        and da_name != 'Main Balance'
) a
left join
(
    select
        msisdn
        , bal_id
        , sum(used_volume) conso_of_day -- en Mo pour la data et en seconde pour la voix
    from mon.spark_ft_msisdn_bal_usage_hour
    where event_date = '###SLICE_VALUE###'
    group by msisdn, bal_id
) b on a.msisdn = b.msisdn and a.bal_id = b.bal_id
left join
(
    select
        msisdn
        , bal_id
        , sum_conso_until_day sum_conso_until_yesterday
        , (sum_conso_until_day - sum_conso_until_yesterday) conso_of_yesterday
    from mon.spark_ft_msisdn_bal_usage_day
    where event_date = date_sub('###SLICE_VALUE###', 1)
) c on a.msisdn = c.msisdn and a.bal_id = c.bal_id
left join
(
    select
        d0.msisdn
        , d0.bal_id
        , sum(used_volume) volume_erased_by_new_sub
    from
    (
        select
            msisdn
            , bal_id
            , TRANSACTION_TIME
        from
        (
            select
                msisdn
                , bdle_name
                , bal_id
                , TRANSACTION_TIME
                , BEN_ACCT_ID
                , row_number() over(partition by msisdn, bal_id order by TRANSACTION_TIME desc) line_number
            from mon.spark_ft_msisdn_subs_bal
            where event_date = '###SLICE_VALUE###'
        ) d00
        left join dim.dt_politique_forfaits d01 on trim(upper(d00.bdle_name)) = trim(upper(d01.OFFER_NAME)) and d00.BEN_ACCT_ID = d01.std_code
        where upper(d01.politic) = 'ECRASE' and d00.line_number = 1
    ) d0
    left join
    (
        select
            msisdn
            , bal_id
            , hour_period
            , used_volume
        from mon.spark_ft_msisdn_bal_usage_hour
        where event_date = '###SLICE_VALUE###'
    ) d1 on d0.msisdn = d1.msisdn and d0.bal_id = d1.bal_id
    where hour_period < substr(TRANSACTION_TIME, 1, 2)
    group by d0.msisdn, d0.bal_id
) d on a.msisdn = d.msisdn and a.bal_id = d.bal_id

