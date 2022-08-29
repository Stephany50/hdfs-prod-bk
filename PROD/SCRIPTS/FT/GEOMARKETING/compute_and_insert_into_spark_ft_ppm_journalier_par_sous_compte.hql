insert into mon.spark_ft_ppm_journalier_par_sous_compte
select a.msisdn msisdn
    , a.bal_id bal_id
    , a.acct_res_id acct_res_id
    , a.acct_res_rating_unit acct_res_rating_unit
    , a.politic politic
    , a.bdle_name bdle_name
    , a.initial_date initial_date
    , a.expiration_date expiration_date
    , a.volume_total volume_total
    , a.volume_restant volume_restant
    , a.prix_total prix_total
    , a.valorisation_restante valorisation_restante
    , (
        CASE 
            WHEN nvl(a.cumul_usage, 0)>nvl(a.volume_total, 0) THEN nvl(a.valorisation_restante, 0) / ((nvl(a.cumul_usage, 0)*nvl(a.duree_bundle, 0))/nvl(a.nb_jours_ecoules, 1) - nvl(cumul_usage_j_moins_un, 0))
            ELSE nvl(a.valorisation_restante, 0) / ( least(nvl(a.volume_total, 0), (nvl(a.cumul_usage, 0)*nvl(a.duree_bundle, 0))/nvl(a.nb_jours_ecoules, 1)) - nvl(cumul_usage_j_moins_un, 0))
        END
    ) ppm_journalier
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select msisdn
        , bal_id
        , acct_res_id
        , acct_res_rating_unit
        , initial_date
        , expiration_date
        , politic
        , bdle_name
        , prix_total
        , valorisation_restante
        , volume_total
        , cumul_usage
        , volume_restant
        , duree_bundle
        , nb_jours_ecoules
    from mon.spark_ft_photo_journaliere_des_sous_comptes
    where event_date = '###SLICE_VALUE###'
) a
left join
(
    select msisdn
        , bal_id
        , cumul_usage cumul_usage_j_moins_un
    from mon.spark_ft_photo_journaliere_des_sous_comptes
    where event_date = date_sub('###SLICE_VALUE###', 1)
) b
on a.msisdn=b.msisdn and a.bal_id=b.bal_id