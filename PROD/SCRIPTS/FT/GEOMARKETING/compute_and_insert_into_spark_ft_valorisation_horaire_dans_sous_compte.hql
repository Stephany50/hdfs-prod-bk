insert into mon.spark_ft_valorisation_horaire_dans_sous_compte
select a.msisdn msisdn
    , b.bdle_name 
    , a.hour_period tranche_horaire
    , a.last_lac last_lac
    , a.last_ci last_ci
    , a.bal_id bal_id
    , b.acct_res_rating_unit acct_res_rating_unit
    , a.usage_horaire
    , a.usage_horaire*nvl(b.ppm_journalier, 0) valorisation_conso_horaire
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from 
(
    select *
    from mon.spark_ft_usage_horaire_localise
    where event_date='###SLICE_VALUE###'
) a
left join
(
    select *
    from mon.spark_ft_ppm_journalier_par_sous_compte
    where event_date='###SLICE_VALUE###'
) b
on a.msisdn = b.msisdn and a.bal_id = b.bal_id

union all

select c.msisdn msisdn
    , c.bdle_name 
    , e.hour_period tranche_horaire
    , e.last_lac last_lac
    , e.last_ci last_ci
    , c.bal_id bal_id
    , c.acct_res_rating_unit acct_res_rating_unit
    , 0 usage_horaire
    , c.valorisation_restante valorisation_conso_horaire
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select *
    from mon.spark_ft_photo_journaliere_des_sous_comptes
    where event_date='###SLICE_VALUE###' and to_date(expiration_date)='###SLICE_VALUE###'
) c
left join
(
    select *
    from mon.spark_ft_usage_horaire_localise
    where event_date='###SLICE_VALUE###'
) d on c.msisdn=d.msisdn and c.bal_id=d.bal_id
left join
(
    select *
    from mon.spark_ft_client_cell_traffic_hour
    where event_date='###SLICE_VALUE###' and hour_period='23'
) e on e.msisdn=c.msisdn
where d.bal_id is null and c.msisdn is not null