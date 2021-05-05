
insert into mon.spark_ft_msisdn_bal_constants
select
    z.msisdn
    , z.bal_id
    , (
        case
            when upper(d.politic) in ('ECRASE', 'INSTANCE') then nvl(a.last_sub_revenu_for_bal, 0)
            when upper(d.politic) = 'CUMUL' then nvl(c.bal_revenu, 0) + nvl(a.all_day_revenu_for_bal, 0)
            else nvl(c.bal_revenu, 0) + nvl(a.all_day_revenu_for_bal, 0)
        end
    ) bal_revenu -- Nouveau Prix bundle
    , (
        case
            when upper(d.politic) in ('CUMUL', 'INSTANCE') then datediff(expiry_date, EFF_DATE) + 1
            when upper(d.politic) = 'ECRASE' then datediff(expiry_date, '###SLICE_VALUE###') + 1
            else datediff(expiry_date, EFF_DATE) + 1
        end
    ) bal_life_time -- Nouvelle durée bundle
    , (
        case
            when upper(d.politic) in ('ECRASE', 'INSTANCE') then 1
            else nvl(index_of_yesterday, 0) + 1
        end
    ) index_of_day -- Indice i de la journée
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        msisdn
        , bal_id
        , expiry_date
        , eff_date
    from mon.spark_ft_msisdn_da_status
    where event_date = '###SLICE_VALUE###'
) z
left join
(
    select
        msisdn
        , bdle_name
        , bal_id
        , revenu_for_bal last_sub_revenu_for_bal
        , duree_bundle last_sub_duree_bundle
        , all_day_revenu_for_bal
    from
    (
        select
            msisdn
            , bdle_name
            , bal_id
            , revenu_for_bal
            , validite duree_bundle
            , sum(revenu_for_bal) over(partition by msisdn, bal_id) all_day_revenu_for_bal
            , row_number() over(partition by msisdn, bal_id order by TRANSACTION_TIME desc) line_number
        from mon.spark_ft_msisdn_subs_bal
        where event_date = '###SLICE_VALUE###'
    ) a0
    where line_number = 1 --- Nous avons là, pour chaque msisdn, bal_id de la journée la dernière subs de la journée qui a été faite qui affecte ce bal_id de ce msisdn
) a on z.msisdn = a.msisdn and z.bal_id = a.bal_id
left join
(
    select
        msisdn
        , bal_id
        , bal_revenu
        , bal_life_time
        , index_of_day index_of_yesterday
    from mon.spark_ft_msisdn_bal_constants
    where event_date = date_sub('###SLICE_VALUE###', 1)
) c on z.msisdn = c.msisdn and z.bal_id = c.bal_id
left join dim.dt_politique_forfaits d on trim(upper(a.bdle_name)) = trim(upper(d.OFFER_NAME)) --- Ici nous avons pour chacune des dernières sousciptions qui affectent chaque msisdn, bal_id la rêgle de gestion

