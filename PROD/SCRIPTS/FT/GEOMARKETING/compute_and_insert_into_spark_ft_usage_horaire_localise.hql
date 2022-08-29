insert into mon.spark_ft_usage_horaire_localise
select
    a.msisdn msisdn
    , substr(session_time, 1, 2) hour_period
    , last_lac
    , last_ci
    , bal_id
    , acct_res_id
    , acct_res_name
    , a.acct_item_type_id acct_item_type_id
    , service
    , sum(used_volume)/1000 usage_horaire
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        msisdn
        , session_time
        , (
            case
                when bal_number = 'bal_id1' then bal_id1
                when bal_number = 'bal_id2' then bal_id2
                when bal_number = 'bal_id3' then bal_id3
                when bal_number = 'bal_id4' then bal_id4
            end
        ) bal_id
        , (
            case
                when bal_number = 'bal_id1' then charge1_corrected
                when bal_number = 'bal_id2' then charge2_corrected
                when bal_number = 'bal_id3' then charge3_corrected
                when bal_number = 'bal_id4' then charge4_corrected
            end
        ) used_volume
        , (
            case
                when bal_number = 'bal_id1' then acct_item_type_id1
                when bal_number = 'bal_id2' then acct_item_type_id2
                when bal_number = 'bal_id3' then acct_item_type_id3
                when bal_number = 'bal_id4' then acct_item_type_id4
            end
        ) acct_item_type_id
        , service
    from
    (
        select 'bal_id1' as bal_number
        union select 'bal_id2' as bal_number
        union select 'bal_id3' as bal_number
        union select 'bal_id4' as bal_number
    ) a0,
    (
        SELECT
            substring(calling_nbr,-9) msisdn
            , date_format(start_time,'HHmmss') session_time
            , acct_item_type_id1
            , acct_item_type_id2
            , acct_item_type_id3
            , acct_item_type_id4
            , bal_id1
            , bal_id2
            , bal_id3
            , bal_id4
            , if(charge1 < 0, 0, nvl(charge1,0)) charge1_corrected
            , if(charge2 < 0, 0, nvl(charge2,0)) charge2_corrected
            , if(charge3 < 0, 0, nvl(charge3,0)) charge3_corrected
            , if(charge4 < 0, 0, nvl(charge4,0)) charge4_corrected
            , 'DATA' service
        from cdr.spark_it_zte_data
        where start_date = '###SLICE_VALUE###'
        union all
        select
            substring(billing_nbr, -9) msisdn
            , date_format(start_time,'HHmmss') session_time
            , acct_item_type_id1
            , acct_item_type_id2
            , acct_item_type_id3
            , acct_item_type_id4
            , bal_id1
            , bal_id2
            , bal_id3
            , bal_id4
            , IF(charge1 < 0, 0, NVL(charge1,0)) charge1_corrected
            , IF(charge2 < 0, 0, NVL(charge2,0)) charge2_corrected
            , IF(charge3 < 0, 0, NVL(charge3,0)) charge3_corrected
            , IF(charge4 < 0, 0, NVL(charge4,0)) charge4_corrected
            , (
                case
                    when nvl(rating_event_service, cast(re_id as string)) = 'SMS' then 'SMS'
                    when upper(nvl(rating_event_service, cast(re_id as string))) in ('SMSMO','SMSRMG') then 'SMS'
                    when nvl(rating_event_service, cast(re_id as string)) = 'TEL' then 'TEL'
                    when upper(nvl(rating_event_service, cast(re_id as string))) in ('OC','OCFWD','OCRMG','TCRMG') then 'TEL'
                    when upper(nvl(rating_event_service, cast(re_id as string))) like '%FNF%MODIFICATION%' then 'TEL'
                    when upper(nvl(rating_event_service, cast(re_id as string))) like '%ACCOUNT%INTERRO%' then 'TEL'
                    else 'OTHER'
                end
            ) service
        from cdr.spark_it_zte_voice_sms a11
        left join 
        (
            select distinct rating_event_id
                , rating_event_service
            from dim.spark_dt_rating_event
        ) a12 
        on a11.re_id = a12.rating_event_id
        where start_date = '###SLICE_VALUE###'
    ) a1
) a -- on capte de trafi a ce niveau
left join --ici on rajoute les infos supplementaires sur les sous comptes
(
    select
        acct_res_id
        , acct_item_type_id
        , upper(acct_res_name) acct_res_name
    from dim.dt_balance_type_item
) b on a.acct_item_type_id = b.acct_item_type_id
left join -- ici on localise le trafic capté
(
    select msisdn
        , hour_period
        , last_ci
        , last_lac
    from mon.spark_ft_client_cell_traffic_hour
    where event_date='###SLICE_VALUE###'
) c on a.msisdn=c.msisdn and substr(a.session_time, 1, 2)=c.hour_period
where bal_id is not null and acct_res_id != 1
group by a.msisdn
    , substr(session_time, 1, 2)
    , last_lac
    , last_ci
    , bal_id
    , acct_res_id
    , acct_res_name
    , a.acct_item_type_id
    , service