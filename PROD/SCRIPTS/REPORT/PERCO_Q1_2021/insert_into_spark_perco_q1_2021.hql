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
    takers_myway_plus_via_om_daily,
    takers_myway_plus_via_om_mtd,
    subscriptions_best_deal_offer_daily,
    subscriptions_best_deal_voice_offer_daily,
    subscriptions_best_deal_data_offer_daily,
    subscriptions_best_deal_combo_offer_daily,
    subscriptions_myway_plus_daily,
    subscriptions_myway_plus_voice_offer_daily,
    subscriptions_myway_plus_data_offer_daily,
    subscriptions_myway_plus_combo_offer_daily,
    subscriptions_myway_plus_via_om_daily,
    revenu_best_deal_offer_daily,
    revenu_best_deal_voice_offer_daily,
    revenu_best_deal_data_offer_daily,
    revenu_myway_plus_daily,
    revenu_myway_plus_voice_offer_daily,
    revenu_myway_plus_data_offer_daily,
    revenu_myway_plus_via_om_daily,
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
    (takers_myway_plus_via_om_daily - avg_takers_myway_plus_via_om_daily) takers_incremental_myway_plus_via_om_daily,
    (takers_best_deal_offer_mtd - avg_takers_best_deal_offer_mtd) takers_incremental_best_deal_offer_mtd,
    (takers_best_deal_voice_offer_mtd - avg_takers_best_deal_voice_offer_mtd) takers_incremental_best_deal_voice_offer_mtd,
    (takers_best_deal_data_offer_mtd - avg_takers_best_deal_data_offer_mtd) takers_incremental_best_deal_data_offer_mtd,
    (takers_best_deal_combo_offer_mtd - avg_takers_best_deal_combo_offer_mtd) takers_incremental_best_deal_combo_offer_mtd,
    (takers_myway_plus_mtd - avg_takers_myway_plus_mtd) takers_incremental_myway_plus_mtd,
    (takers_myway_plus_voice_offer_mtd - avg_takers_myway_plus_voice_offer_mtd) takers_incremental_myway_plus_voice_offer_mtd,
    (takers_myway_plus_data_offer_mtd - avg_takers_myway_plus_data_offer_mtd) takers_incremental_myway_plus_data_offer_mtd,
    (takers_myway_plus_combo_offer_mtd - avg_takers_myway_plus_combo_offer_mtd) takers_incremental_myway_plus_combo_offer_mtd,
    (takers_myway_plus_via_om_mtd - avg_takers_myway_plus_via_om_mtd) takers_incremental_myway_plus_via_om_mtd,
    (subscriptions_best_deal_offer_daily - avg_subscriptions_best_deal_offer_daily) subscriptions_incremental_best_deal_offer_daily,
    (subscriptions_best_deal_voice_offer_daily - avg_subscriptions_best_deal_voice_offer_daily) subscriptions_incremental_best_deal_voice_offer_daily,
    (subscriptions_best_deal_data_offer_daily - avg_subscriptions_best_deal_data_offer_daily) subscriptions_incremental_best_deal_data_offer_daily,
    (subscriptions_best_deal_combo_offer_daily - avg_subscriptions_best_deal_combo_offer_daily) subscriptions_incremental_best_deal_combo_offer_daily,
    (subscriptions_myway_plus_daily - avg_subscriptions_myway_plus_daily) subscriptions_incremental_myway_plus_daily,
    (subscriptions_myway_plus_voice_offer_daily - avg_subscriptions_myway_plus_voice_offer_daily) subscriptions_incremental_myway_plus_voice_offer_daily,
    (subscriptions_myway_plus_data_offer_daily - avg_subscriptions_myway_plus_data_offer_daily) subscriptions_incremental_myway_plus_data_offer_daily,
    (subscriptions_myway_plus_combo_offer_daily - avg_subscriptions_myway_plus_combo_offer_daily) subscriptions_incremental_myway_plus_combo_offer_daily,
    (subscriptions_myway_plus_via_om_daily - avg_subscriptions_myway_plus_via_om_daily) subscriptions_incremental_myway_plus_via_om_daily,
    (nvl(revenu_voice_best_deal_incremental, 0) + nvl(revevenu_voice_myway_plus_incremental, 0)) ca_voix_incremental_daily,
    (nvl(revenu_data_best_deal_incremental, 0) + nvl(revenu_data_myway_plus_incremental, 0)) ca_data_incremental_daily,
    revenu_paygo_incremental ca_paygo_incremental_daily,
    (nvl(revenu_voice_best_deal_incremental, 0) + nvl(revevenu_voice_myway_plus_incremental, 0) + nvl(revenu_data_best_deal_incremental, 0) + nvl(revenu_data_myway_plus_incremental, 0) + nvl(revenu_paygo_incremental, 0)) ca_global_incremental_daily,
    usage_voix_incremental usage_incremental_takers_voice_offer_daily,
    usage_data_incremental usage_incremental_takers_data_offer_daily,
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
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b10.nber_subs_om > 0 then b0.msisdn end
        ) takers_myway_plus_via_om_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b10.nber_subs_om > 0 then b0.msisdn end
        ) takers_myway_plus_via_om_mtd,

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
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b10.msisdn is not null then b10.nber_subs_om else 0 end
        ) subscriptions_myway_plus_via_om_daily,

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
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b10.msisdn is not null then b10.ca_subs_om else 0 end
        ) revenu_myway_plus_via_om_daily,

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
            max(og_total_call_duration/60) usage_voix,
            max((nvl(data_bytes_received, 0) + nvl(data_bytes_sent, 0))/(1024*1024)) usage_data
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
                case when offer_type = 'Best Deal' then nvl(amount_voix, 0) + nvl(amount_data, 0) else 0 end
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
                case when offer_type = 'Myway Plus' then nvl(amount_voix, 0) + nvl(amount_data, 0) else 0 end
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
                        (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_voice,
                        (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0))/100 coeff_data,
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
                (nvl(sb_amount_onnet, 0) + nvl(sb_amount_allnet, 0)) amount_voix
            from CDR.SPARK_IT_MYWAY_REPORT
            where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and sb_status_in = 'SUCCESSFULL'
        ) B90
        group by msisdn, event_date
    ) B9 on b0.msisdn = B9.msisdn and b0.event_date = B9.event_date
    left join
    (
        select
            sender_msisdn msisdn,
            transfer_datetime event_date,
            count(*) nber_subs_om,
            sum(transaction_amount) ca_subs_om
        from cdr.spark_it_omny_transactions
        where transfer_datetime between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
            and TRANSFER_STATUS='TS'
            and  receiver_msisdn in ('656907599','655844658','694056170','696844033','695846041','698161416','655578102','656230098','656300836','694645057')
        group by sender_msisdn, transfer_datetime
    ) b10 on b0.msisdn = B10.msisdn and b0.event_date = b10.event_date
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
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_voice_offer_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_best_deal_voice_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_data_offer_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_best_deal_data_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_best_deal_combo_offer_mtd end
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
        avg(takers_myway_plus_via_om_daily) avg_takers_myway_plus_via_om_daily,

        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_voice_offer_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_voice_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_data_offer_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_data_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_combo_offer_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_combo_offer_mtd,
        sum(
            case when day(event_date) = day('###SLICE_VALUE###') then takers_myway_plus_via_om_mtd end
        ) / count(
            distinct case when day(event_date) = day('###SLICE_VALUE###') then event_date end
        ) avg_takers_myway_plus_via_om_mtd,

        avg(subscriptions_myway_plus_daily) avg_subscriptions_myway_plus_daily,
        avg(subscriptions_myway_plus_voice_offer_daily) avg_subscriptions_myway_plus_voice_offer_daily,
        avg(subscriptions_myway_plus_data_offer_daily) avg_subscriptions_myway_plus_data_offer_daily,
        avg(subscriptions_myway_plus_combo_offer_daily) avg_subscriptions_myway_plus_combo_offer_daily,
        avg(subscriptions_myway_plus_via_om_daily) avg_subscriptions_myway_plus_via_om_daily
    from tmp.perco_q1_2021_incrementals
    group by site_name
) a on b.site_name = a.site_name
left join
(
    select
        site_name,
        sum(revenu_voice_best_deal_day - nvl(avg_revenu_voice_best_deal, 0)) revenu_voice_best_deal_incremental,
        sum(revenu_data_best_deal_day - nvl(avg_revenu_data_best_deal, 0)) revenu_data_best_deal_incremental,

        sum(revenu_voice_myway_plus_day - nvl(avg_revenu_voice_myway_plus, 0)) revevenu_voice_myway_plus_incremental,
        sum(revenu_data_myway_plus_day - nvl(avg_revenu_data_myway_plus, 0)) revenu_data_myway_plus_incremental,

        sum(nvl(usage_voix_day, 0) - nvl(avg_usage_voix, 0)) usage_voix_incremental,
        sum(nvl(usage_data_day, 0) - nvl(avg_usage_data, 0)) usage_data_incremental,

        sum(nvl(revenu_paygo_day, 0) - nvl(avg_revenu_paygo, 0)) revenu_paygo_incremental
    from
    (
        select
            MSISDN,
            
            sum(
                case when offer_type = 'Best Deal' then amount_voix else 0 end
            ) revenu_voice_best_deal_day,
            sum(
                case when offer_type = 'Best Deal' then amount_data else 0 end
            ) revenu_data_best_deal_day,

            sum(
                case when offer_type = 'Myway Plus' then amount_voix else 0 end
            ) revenu_voice_myway_plus_day,
            sum(
                case when offer_type = 'Myway Plus' then amount_data else 0 end
            ) revenu_data_myway_plus_day
        from
        (
            select
                msisdn,
                offer_type,
                ipp_category,
                amount_data,
                amount_voix
            from
            (
                select
                    msisdn,
                    'Best Deal' offer_type,
                    (
                        case
                            when coeff_voice = 100.0 then 'voix'
                            when coeff_data = 100.0 then 'data'
                            else 'combo'
                        end
                    ) ipp_category,
                    nvl(c0000.price, c0002.prix)*nvl(coeff_data, 0) amount_data,
                    nvl(c0000.price, c0002.prix)*nvl(coeff_voice, 0) amount_voix
                from
                (
                    select
                        msisdn,
                        ipp_code,
                        price,
                        suggestion
                    from CDR.SPARK_IT_ZEMBLAREPORT
                    where event_date = '###SLICE_VALUE###'
                ) c0000
                left join dim.spark_bundles_perco_q1_2021 c0001 on c0000.ipp_code = c0001.offer_code
                LEFT JOIN
                (
                    select
                        (nvl(coeff_onnet, 0) + nvl(coeff_offnet, 0) + nvl(coeff_inter, 0) + nvl(coeff_roaming, 0) + nvl(coef_sms, 0) + nvl(coeff_roaming_sms, 0))/100 coeff_voice,
                        (nvl(coeff_data, 0) + nvl(coeff_roaming_data, 0))/100 coeff_data,
                        prix,
                        bdle_name
                    from DIM.DT_CBM_REF_SOUSCRIPTION_PRICE
                ) c0002 ON UPPER(TRIM(c0001.offer_name)) = UPPER(TRIM(c0002.BDLE_NAME))
            ) c000
            union all
            select
                sb_msisdn MSISDN,
                'Myway Plus' offer_type,
                sb_type ipp_category,
                sb_amount_data amount_data,
                (nvl(sb_amount_onnet, 0) + nvl(sb_amount_allnet, 0)) amount_voix
            from CDR.SPARK_IT_MYWAY_REPORT
            where event_date = '###SLICE_VALUE###' and sb_status_in = 'SUCCESSFULL'
        ) c00
        group by msisdn
    ) c0
    left join
    (
        select
            msisdn,
            max(og_total_call_duration/60) usage_voix_day,
            max((nvl(data_bytes_received, 0) + nvl(data_bytes_sent, 0))/(1024*1024)) usage_data_day,
            max(nvl(main_rated_tel_amount, 0)) revenu_paygo_day
        from mon.spark_ft_marketing_datamart
        where event_date = '###SLICE_VALUE###'
        group by msisdn
    ) c3 on c0.msisdn = c3.msisdn
    left join tmp.perco_q1_2021_incrementals_revenus_usages c1 on c0.msisdn = c1.msisdn
    left JOIN
    (
        SELECT
            c20.MSISDN,
            UPPER(NVL(c21.SITE_NAME, c20.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) c20
        RIGHT JOIN
        (
            SELECT
                MSISDN,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN
        ) c21
        ON c20.MSISDN = c21.MSISDN
    ) c2 on c0.msisdn = c2.msisdn
    group by c2.site_name
) c on b.site_name = c.site_name
