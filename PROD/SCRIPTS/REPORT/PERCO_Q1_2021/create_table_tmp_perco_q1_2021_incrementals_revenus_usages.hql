create table tmp.perco_q1_2021_incrementals_revenus_usages as
select
    z.msisdn,
    avg(revenu_best_deal) avg_revenu_best_deal,
    avg(revenu_voice_best_deal) avg_revenu_voice_best_deal,
    avg(revenu_data_best_deal) avg_revenu_data_best_deal,
    avg(revenu_myway_plus) avg_revenu_myway_plus,
    avg(revenu_voice_myway_plus) avg_revenu_voice_myway_plus,
    avg(revenu_data_myway_plus) avg_revenu_data_myway_plus,
    avg(usage_voix) avg_usage_voix,
    avg(usage_data) avg_usage_data,
    avg(revenu_paygo) avg_revenu_paygo
from
(
    select
        MSISDN,
        event_date,
        
        sum(
            case when offer_type = 'Best Deal' then bdle_cost else 0 end
        ) revenu_best_deal,
        sum(
            case when offer_type = 'Best Deal' then bdle_cost*coeff_voice else 0 end
        ) revenu_voice_best_deal,
        sum(
            case when offer_type = 'Best Deal' then bdle_cost*coeff_data else 0 end
        ) revenu_data_best_deal,
        
        sum(
            case when offer_type = 'Myway Plus' then bdle_cost else 0 end
        ) revenu_myway_plus,
        sum(
            case when offer_type = 'Myway Plus' then bdle_cost*coeff_voice else 0 end
        ) revenu_voice_myway_plus,
        sum(
            case when offer_type = 'Myway Plus' then bdle_cost*coeff_data else 0 end
        ) revenu_data_myway_plus
    from
    (
        select
            msisdn,
            bdle_cost,
            nber_purchase,
            bdle_name,
            event_date,
            (
                case
                    when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') and upper(subscription_channel) like '%GOS SDP%' then 'Myway Plus'
                    when upper(bdle_name) not in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') and subscription_channel = 'Third-Party CRM' then 'Best Deal'
                    else null
                end
            ) offer_type
        from
        (
            select
                msisdn,
                bdle_cost,
                nber_purchase,
                bdle_name,
                period event_date,
                subscription_channel
            from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
            where period between '2020-11-01' and '2021-02-25' and month(period)<>12
        ) a0
        right join dim.spark_bundles_perco_q1_2021 a1 ON UPPER(TRIM(a0.BDLE_NAME)) = UPPER(TRIM(a1.offer_name))
    ) a
    left join
    (
        select
            (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_voice,
            (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0))/100 coeff_data,
            (
                case
                    when nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0) = 100.0 then 'voix'
                    when nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0) = 100.0 then 'data'
                    else 'combo'
                end
            ) ipp_category,
            bdle_name
        from DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
    ) b ON UPPER(TRIM(a.bdle_name)) = UPPER(TRIM(b.bdle_name))
    where offer_type is not null
    group by msisdn, event_date
) z
left join
(
    select
        msisdn,
        event_date,
        max(og_total_call_duration/60) usage_voix,
        max((data_bytes_received + data_bytes_sent)/(1024*1024)) usage_data,
        max(nvl(main_rated_tel_amount, 0)) revenu_paygo
    from mon.spark_ft_marketing_datamart
    where event_date between '2020-11-01' and '2021-02-25' and month(event_date)<>12
    group by msisdn, event_date
) c on z.msisdn = c.msisdn and z.event_date = c.event_date
group by z.msisdn
