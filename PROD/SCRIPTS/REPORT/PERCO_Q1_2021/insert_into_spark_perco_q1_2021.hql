insert into mon.spark_ft_perco_q1_2021
select
    a.site_name,
    users_backend_daily,
    users_backend_mtd,
    takers_best_deal_offer_daily,
    takers_best_deal_offer_mtd,
    takers_best_deal_voice_offer_daily,
    takers_best_deal_voice_offer_mtd,
    takers_best_deal_data_offer_daily,
    takers_best_deal_data_offer_mtd,
    takers_best_deal_combo_offer_daily,
    takers_best_deal_combo_offer_mtd,
    takers_myway_plus_daily,
    takers_myway_plus_mtd,
    takers_myway_plus_voice_offer_daily,
    takers_myway_plus_voice_offer_mtd,
    takers_myway_plus_data_offer_daily,
    takers_myway_plus_data_offer_mtd,
    takers_myway_plus_combo_offer_daily,
    takers_myway_plus_combo_offer_mtd,
    null takers_myway_plus_via_om_daily,
    null takers_myway_plus_via_om_mtd,
    subscriptions_best_deal_offer_daily,
    subscriptions_best_deal_voice_offer_daily,
    subscriptions_best_deal_data_offer_daily,
    subscriptions_best_deal_combo_offer_daily,
    subscriptions_myway_plus_daily,
    subscriptions_myway_plus_voice_offer_daily,
    subscriptions_myway_plus_data_offer_daily,
    subscriptions_myway_plus_combo_offer_daily,
    null subscriptions_myway_plus_via_om_daily,
    revenu_best_deal_offer_daily,
    revenu_best_deal_voice_offer_daily,
    revenu_best_deal_data_offer_daily,
    revenu_myway_plus_daily,
    revenu_myway_plus_voice_offer_daily,
    revenu_myway_plus_data_offer_daily,
    null revenu_myway_plus_via_om_daily,
    usage_takers_voice_offer_daily,
    usage_takers_data_offer_daily,
    (takers_best_deal_offer_daily - avg_takers_best_deal_offer_daily) takers_incremental_best_deal_offer_daily,
    (takers_best_deal_voice_offer_daily - avg_takers_best_deal_voice_offer_daily) takers_incremental_best_deal_voice_offer_daily,
    (takers_best_deal_data_offer_daily - avg_takers_best_deal_data_offer_daily) takers_incremental_best_deal_data_offer_daily,
    (takers_best_deal_combo_offer_daily - avg_takers_best_deal_combo_offer_daily) takers_incremental_best_deal_combo_offer_daily,
    (takers_myway_plus_daily - avg_takers_myway_plus_daily) takers_incremental_myway_plus_daily,
    (takers_myway_plus_voice_offer_daily - avg_takers_myway_plus_voice_offer_daily) takers_incremental_myway_plus_voice_offer_daily,
    (takers_myway_plus_data_offer_daily - avg_takers_myway_plus_data_offer_daily) takers_incremental_myway_plus_data_offer_daily,
    (takers_myway_plus_combo_offer_daily - avg_takers_myway_plus_combo_offer_daily) takers_incremental_myway_plus_combo_offer_daily,
    null takers_incremental_myway_plus_via_om_daily,
    (takers_best_deal_offer_mtd - avg_takers_best_deal_offer_mtd) takers_incremental_best_deal_offer_mtd,
    (takers_best_deal_voice_offer_mtd - avg_takers_best_deal_voice_offer_mtd) takers_incremental_best_deal_voice_offer_mtd,
    (takers_best_deal_data_offer_mtd - avg_takers_best_deal_data_offer_mtd) takers_incremental_best_deal_data_offer_mtd,
    (takers_best_deal_combo_offer_mtd - avg_takers_best_deal_combo_offer_mtd) takers_incremental_best_deal_combo_offer_mtd,
    (takers_myway_plus_mtd - avg_takers_myway_plus_mtd) takers_incremental_myway_plus_mtd,
    (takers_myway_plus_voice_offer_mtd - avg_takers_myway_plus_voice_offer_mtd) takers_incremental_myway_plus_voice_offer_mtd,
    (takers_myway_plus_data_offer_mtd - avg_takers_myway_plus_data_offer_mtd) takers_incremental_myway_plus_data_offer_mtd,
    (takers_myway_plus_combo_offer_mtd - avg_takers_myway_plus_combo_offer_mtd) takers_incremental_myway_plus_combo_offer_mtd,
    null takers_incremental_myway_plus_via_om_mtd,
    (subscriptions_best_deal_offer_daily - avg_subscriptions_best_deal_offer_daily) subscriptions_incremental_best_deal_offer_daily,
    (subscriptions_best_deal_voice_offer_daily - avg_subscriptions_best_deal_voice_offer_daily) subscriptions_incremental_best_deal_voice_offer_daily,
    (subscriptions_best_deal_data_offer_daily - avg_subscriptions_best_deal_data_offer_daily) subscriptions_incremental_best_deal_data_offer_daily,
    (subscriptions_best_deal_combo_offer_daily - avg_subscriptions_best_deal_combo_offer_daily) subscriptions_incremental_best_deal_combo_offer_daily,
    (subscriptions_myway_plus_daily - avg_subscriptions_myway_plus_daily) subscriptions_incremental_myway_plus_daily,
    (subscriptions_myway_plus_voice_offer_daily - avg_subscriptions_myway_plus_voice_offer_daily) subscriptions_incremental_myway_plus_voice_offer_daily,
    (subscriptions_myway_plus_data_offer_daily - avg_subscriptions_myway_plus_data_offer_daily) subscriptions_incremental_myway_plus_data_offer_daily,
    (subscriptions_myway_plus_combo_offer_daily - avg_subscriptions_myway_plus_combo_offer_daily) subscriptions_incremental_myway_plus_combo_offer_daily,
    null subscriptions_incremental_myway_plus_via_om_daily,
    null ca_voix_incremental_daily,
    null ca_data_incremental_daily,
    null ca_combo_incremental_daily,
    null ca_paygo_incremental_daily,
    null ca_global_incremental_daily,
    null usage_incremental_takers_voice_offer_daily,
    null usage_incremental_takers_data_offer_daily,
    current_date insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select
        b5.site_name site_name,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b1.msisdn is not null then b0.msisdn end
        ) users_backend_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b1.msisdn is not null then b0.msisdn end
        ) users_backend_mtd,
        
        -----------------------------------------------
        ------              takers               ------
        -----------------------------------------------

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_voice_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_voice_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_voice_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_voice_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_data_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_data_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_data_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_data_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_combo_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_combo_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_combo_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_combo_offer_mtd,

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_voice_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_voice_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_voice_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_voice_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_data_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_data_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_data_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_data_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b9.nber_subscription_combo_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_combo_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b9.nber_subscription_combo_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_combo_offer_mtd,

        -----------------------------------------------
        ------           subscriptions           ------
        -----------------------------------------------
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_best_deal else 0 end
        ) subscriptions_best_deal_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_voice_best_deal else 0 end
        ) subscriptions_best_deal_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_data_best_deal else 0 end
        ) subscriptions_best_deal_data_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_combo_best_deal else 0 end
        ) subscriptions_best_deal_combo_offer_daily,

        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_myway_plus else 0 end
        ) subscriptions_myway_plus_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_voice_myway_plus else 0 end
        ) subscriptions_myway_plus_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_data_myway_plus else 0 end
        ) subscriptions_myway_plus_data_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.nber_subscription_combo_myway_plus else 0 end
        ) subscriptions_myway_plus_combo_offer_daily,

        -----------------------------------------------
        ------              revenu               ------
        -----------------------------------------------
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.revenu_best_deal else 0 end
        ) revenu_best_deal_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.revenu_voice_best_deal else 0 end
        ) revenu_best_deal_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.revenu_data_best_deal else 0 end
        ) revenu_best_deal_data_offer_daily,
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.revenu_myway_plus else 0 end
        ) revenu_myway_plus_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.revenu_voice_myway_plus else 0 end
        ) revenu_myway_plus_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null then b9.revenu_data_myway_plus else 0 end
        ) revenu_myway_plus_data_offer_daily,

        -----------------------------------------------
        ------              usage                ------
        -----------------------------------------------
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null and (b9.nber_subscription_voice_best_deal > 0 or b9.nber_subscription_voice_myway_plus > 0) then b0.usage_voix else 0 end
        ) usage_takers_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b9.msisdn is not null and (b9.nber_subscription_data_best_deal > 0 or b9.nber_subscription_data_myway_plus > 0) then b0.usage_data else 0 end
        ) usage_takers_data_offer_daily

    from
    (
        select
            msisdn,
            event_date,
            max(activation_date) activation_date,
            max(total_revenue) revenu_global,
            max(total_data_revenue) revenu_data,
            max(total_voice_revenue + total_sms_revenue) revenu_voix,
            max(og_total_call_duration/60) usage_voix,
            max((data_bytes_received + data_bytes_sent)/(1024*1024)) usage_data
        from mon.spark_ft_marketing_datamart
        where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
        group by msisdn, event_date
    ) B0
    left join
    (
        select
            distinct msisdn,
            event_date
        from CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND
        where event_date between substring('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###'
    ) B1 on b0.msisdn = b0.msisdn and b0.event_date = b1.event_date
    left join
    (
        select
            MSISDN,
            event_date,

            sum(
                case when offer_type = 'Best Deal' then amount_voix + amount_data else 0 end
            ) revenu_best_deal,
            sum(
                case when offer_type = 'Best Deal' then amount_voix else 0 end
            ) revenu_voice_best_deal,
            sum(
                case when offer_type = 'Best Deal' then amount_data else 0 end
            ) revenu_data_best_deal,
            sum(
                case when offer_type = 'Best Deal' then 1 else 0 end
            ) nber_subscription_best_deal,
            sum(
                case when offer_type = 'Best Deal' and ipp_category = 'voix' then 1 else 0 end
            ) nber_subscription_voice_best_deal,
            sum(
                case when offer_type = 'Best Deal' and ipp_category = 'data' then 1 else 0 end
            ) nber_subscription_data_best_deal,
            sum(
                case when offer_type = 'Best Deal' and ipp_category = 'combo' then 1 else 0 end
            ) nber_subscription_combo_best_deal,

            sum(
                case when offer_type = 'Myway Plus' then amount_voix + amount_data else 0 end
            ) revenu_myway_plus,
            sum(
                case when offer_type = 'Myway Plus' then amount_voix else 0 end
            ) revenu_voice_myway_plus,
            sum(
                case when offer_type = 'Myway Plus' then amount_data else 0 end
            ) revenu_data_myway_plus,
            sum(
                case when offer_type = 'Myway Plus' then 1 else 0 end
            ) nber_subscription_myway_plus,
            sum(
                case when offer_type = 'Myway Plus' and ipp_category = 'voix' then 1 else 0 end
            ) nber_subscription_voice_myway_plus,
            sum(
                case when offer_type = 'Myway Plus' and ipp_category = 'data' then 1 else 0 end
            ) nber_subscription_data_myway_plus,
            sum(
                case when offer_type = 'Myway Plus' and ipp_category = 'combo' then 1 else 0 end
            ) nber_subscription_combo_myway_plus
        from
        (
            select
                msisdn,
                event_date,
                offer_type,
                ipp_category,
                amount_data,
                amount_voix
            from
            (
                select
                    msisdn,
                    event_date,
                    'Best Deal' offer_type,
                    (
                        case
                            when coeff_voice = 100.0 then 'voix'
                            when coeff_data = 100.0 then 'data'
                            else 'combo'
                        end
                    ) ipp_category,
                    nvl(B9000.price, B9002.prix)*nvl(coeff_data, 0) amount_data,
                    nvl(B9000.price, B9002.prix)*nvl(coeff_voice, 0) amount_voix
                from
                (
                    select
                        msisdn,
                        SUBS_DATE,
                        ipp_code,
                        price,
                        suggestion,
                        event_date
                    from CDR.SPARK_IT_ZEMBLAREPORT
                    where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
                ) B9000
                left join dim.spark_bundles_perco_q1_2021 B9001 on B9000.ipp_code = B9001.offer_code
                LEFT JOIN
                (
                    select
                        (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0)) coeff_voice,
                        (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0)) coeff_data,
                        prix,
                        bdle_name
                    from DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
                ) B9002 ON UPPER(TRIM(B9001.offer_name)) = UPPER(TRIM(B9002.BDLE_NAME))
            ) B900
            union all
            select
                sb_msisdn MSISDN,
                event_date,
                'Myway Plus' offer_type,
                sb_type ipp_category,
                sb_amount_data amount_data,
                (sb_amount_onnet + sb_amount_allnet) amount_voix
            from CDR.SPARK_IT_MYWAY_REPORT
            where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and sb_status_in = 'SUCCESSFULL'
        ) B90
        group by msisdn, event_date
    ) B9 on b0.msisdn = B9.msisdn and b0.event_date = B9.event_date
    left JOIN
    (
        SELECT
            nvl(b50.MSISDN, b51.MSISDN) MSISDN,
            UPPER(NVL(b51.SITE_NAME, b50.SITE_NAME)) SITE_NAME
        FROM
        (
            select
                msisdn,
                first_value(site_name) over(partition by msisdn order by nbre_apparition_msisdn_site desc) site_name
            from
            (
                SELECT
                    MSISDN,
                    SITE_NAME,
                    count(*) nbre_apparition_msisdn_site
                FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
                GROUP BY MSISDN, SITE_NAME
            ) b500
        ) b50
        FULL JOIN
        (
            select
                msisdn,
                first_value(site_name) over(partition by msisdn order by nbre_apparition_msisdn_site desc) site_name
            from
            (
                SELECT
                    MSISDN,
                    SITE_NAME,
                    count(*) nbre_apparition_msisdn_site
                FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
                WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
                GROUP BY MSISDN, SITE_NAME
            ) b510
        ) b51 ON b50.MSISDN = b51.MSISDN
    ) b5 on b0.msisdn = b5.msisdn
    group by b5.site_name
) B
left join
(
    select
        site_name,
        avg(takers_best_deal_offer_daily) avg_takers_best_deal_offer_daily,
        avg(takers_best_deal_voice_offer_daily) avg_takers_best_deal_voice_offer_daily,
        avg(takers_best_deal_data_offer_daily) avg_takers_best_deal_data_offer_daily,
        avg(takers_best_deal_combo_offer_daily) avg_takers_best_deal_combo_offer_daily,

        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_offer_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_best_deal_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_voice_offer_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_best_deal_voice_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_data_offer_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_best_deal_data_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_combo_offer_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_best_deal_combo_offer_mtd,

        avg(subscriptions_best_deal_offer_daily) avg_subscriptions_best_deal_offer_daily,
        avg(subscriptions_best_deal_voice_offer_daily) avg_subscriptions_best_deal_voice_offer_daily,
        avg(subscriptions_best_deal_data_offer_daily) avg_subscriptions_best_deal_data_offer_daily,
        avg(subscriptions_best_deal_combo_offer_daily) avg_subscriptions_best_deal_combo_offer_daily,

        avg(takers_myway_plus_daily) avg_takers_myway_plus_daily,
        avg(takers_myway_plus_voice_offer_daily) avg_takers_myway_plus_voice_offer_daily,
        avg(takers_myway_plus_data_offer_daily) avg_takers_myway_plus_data_offer_daily,
        avg(takers_myway_plus_combo_offer_daily) avg_takers_myway_plus_combo_offer_daily,

        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_voice_offer_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_voice_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_data_offer_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_data_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_combo_offer_daily end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_combo_offer_mtd,

        avg(subscriptions_myway_plus_daily) avg_subscriptions_myway_plus_daily,
        avg(subscriptions_myway_plus_voice_offer_daily) avg_subscriptions_myway_plus_voice_offer_daily,
        avg(subscriptions_myway_plus_data_offer_daily) avg_subscriptions_myway_plus_data_offer_daily,
        avg(subscriptions_myway_plus_combo_offer_daily) avg_subscriptions_myway_plus_combo_offer_daily
    from tmp.perco_q1_2021_incrementals
    group by site_name
) a on b.site_name = a.site_name
