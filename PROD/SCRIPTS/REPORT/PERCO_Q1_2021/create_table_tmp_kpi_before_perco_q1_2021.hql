create table tmp.kpi_before_perco_q1_2021 as
select
    nvl(z.msisdn,c.msisdn) msisdn,
    avg(revenu_subs_total) avg_revenu_subs_total,
    avg(revenu_subs_voice) avg_revenu_subs_voice,
    avg(revenu_subs_data) avg_revenu_subs_data,
    avg(revenu_subs_sms)  avg_revenu_subs_sms,

    avg(revenu_paygo_total) avg_revenu_paygo_total,
    avg(revenu_paygo_voice) avg_revenu_paygo_voice,
    avg(revenu_paygo_sms) avg_revenu_paygo_sms,

    avg(usage_voice) avg_usage_voice,
    avg(usage_data) avg_usage_data,
    avg(usage_sms) avg_usage_sms
from(
 select 
    msisdn,
    event_date,
    sum(nvl(total_cost,0)) revenu_subs_total,
    sum(nvl(total_cost_voice,0)) revenu_subs_voice,
    sum(nvl(total_cost_data,0)) revenu_subs_data,
    sum(nvl(total_cost_sms,0)) revenu_subs_sms
 from (
     select
         msisdn,
         nber_purchase,
         total_cost,
         total_cost*nvl(coeff_voice,0) total_cost_voice,
         total_cost*nvl(coeff_data,0) total_cost_data,
         total_cost*nvl(coeff_sms,0) total_cost_sms,
         event_date
     from
     (
        select
            msisdn,
            bdle_name,
            nber_purchase,
            bdle_cost total_cost,
            period event_date
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        where period between '2021-01-01' and '2021-02-28'
    ) a0
    left join
    (
        select
            (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_voice,
            (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0))/100 coeff_data,
            (nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_sms,
            bdle_name
        from DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
    ) b ON UPPER(TRIM(a0.bdle_name)) = UPPER(TRIM(b.bdle_name))
 ) a
 group by msisdn,event_date
)z
full join (
    select
        msisdn,
        event_date,
        max(nvl(og_total_call_duration,0)/60) usage_voice,
        max(nvl((data_bytes_received + data_bytes_sent),0)/(1024*1024)) usage_data,
        max(nvl(og_sms_total_count,0)) usage_sms,
        max(nvl(main_rated_tel_amount, 0)+nvl(main_rated_sms_amount, 0)) revenu_paygo_total,
        max(nvl(main_rated_tel_amount, 0)) revenu_paygo_voice,
        max(nvl(main_rated_sms_amount, 0)) revenu_paygo_sms
    from mon.spark_ft_marketing_datamart
    where event_date between '2021-01-01' and '2021-02-28'
    group by msisdn, event_date
) c on z.msisdn = c.msisdn and z.event_date = c.event_date
group by nvl(z.msisdn,c.msisdn)