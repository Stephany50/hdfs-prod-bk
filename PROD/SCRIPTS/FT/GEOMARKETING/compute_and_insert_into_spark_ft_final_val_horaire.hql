insert into mon.spark_ft_final_valorisation_horaire
select 
    msisdn
    , tranche_horaire
    , site_name
    , last_lac
    , last_ci
    , REVENU_SUBS_VOIX
    , REVENU_SUBS_SMS
    , REVENU_SUBS_DATA
    , REVENU_SUBS_OTHER
    , valorisation_conso_horaire
    , insert_date
    , event_date
from
(
    select a.msisdn msisdn
        , a.tranche_horaire tranche_horaire
        , max(b.site_name) site_name
        , a.bdl_name
        , a.last_lac
        , a.last_ci
        , sum(valorisation_conso_horaire) * nvl(max(total_coeff_voix), 0) REVENU_SUBS_VOIX
        , sum(valorisation_conso_horaire) * nvl(max(total_coeff_sms), 0) REVENU_SUBS_SMS
        , sum(valorisation_conso_horaire) * nvl(max(total_coeff_data), 0) REVENU_SUBS_DATA
        , sum(valorisation_conso_horaire) * (1 - nvl(max(total_coeff), 0)) REVENU_SUBS_OTHER
        , sum(valorisation_conso_horaire) valorisation_conso_horaire
        , current_timestamp insert_date
        , '###SLICE_VALUE###' event_date
    from
    (
        select *
        from mon.spark_ft_valorisation_horaire_dans_sous_compte
        where event_date = '###SLICE_VALUE###'
    ) a
    left join
    (
        select ci, lac, max(site_name) site_name from dim.spark_dt_gsm_cell_code_new group by ci, lac
    ) b
    on cast(a.last_ci as int)=cast(b.ci as int) and cast(a.last_lac as int)=cast(b.lac as int)
    left join 
    (
        select
            trim(upper(BDLE_NAME)) BDLE_NAME, 
            max(nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming_voix, 0)) / 100 total_coeff_voix,
            max(nvl(coef_sms, 0) + nvl(coef_sva, 0) + nvl(coeff_roaming_sms, 0)) / 100 total_coeff_sms,
            max(nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0)) / 100 total_coeff_data,
            max(nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_data, 0) + nvl(coef_sms, 0) + nvl(coef_sva, 0)  + nvl(coeff_roaming_voix, 0) + nvl(coeff_roaming_data, 0) + nvl(coeff_roaming_sms, 0)) / 100 total_coeff
        from DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
        group by trim(upper(BDLE_NAME)) 
    ) ref_souscription on trim(upper(a.bdl_name)) = trim(upper(ref_souscription.BDLE_NAME)) 
    group by msisdn
        , tranche_horaire
        , acct_res_rating_unit
        , a.last_lac
        , a.last_ci
        , a.bdl_name
)