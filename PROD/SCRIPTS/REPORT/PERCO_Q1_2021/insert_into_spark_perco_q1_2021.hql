insert into mon.spark_ft_perco_q1_2021
select
    b.site_name,
    b.region,
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
    takers_best_deal_offer_daily_perco,
    takers_best_deal_offer_mtd_perco,
    takers_best_deal_voice_offer_daily_perco,
    takers_best_deal_voice_offer_mtd_perco,
    takers_best_deal_data_offer_daily_perco,
    takers_best_deal_data_offer_mtd_perco,
    takers_best_deal_combo_offer_daily_perco,
    takers_best_deal_combo_offer_mtd_perco,
    subscriptions_best_deal_offer_daily_perco,
    subscriptions_best_deal_voice_offer_daily_perco,
    subscriptions_best_deal_data_offer_daily_perco,
    subscriptions_best_deal_combo_offer_daily_perco,
    revenu_best_deal_offer_daily_perco,
    revenu_best_deal_voice_offer_daily_perco,
    revenu_best_deal_data_offer_daily_perco,
    current_date insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select
        b5.site_name site_name,
        b5.region region,
        count(
            distinct case when nvl(b0.event_date,b1.event_date) = '###SLICE_VALUE###' and b1.msisdn is not null then b1.msisdn end
        ) users_backend_daily,
        count(
            distinct case when nvl(b0.event_date,b1.event_date) between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b1.msisdn is not null then b1.msisdn end
        ) users_backend_mtd,
        
        -----------------------------------------------
        ------              takers               ------
        -----------------------------------------------

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_voice_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_voice_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_voice_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_voice_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_data_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_data_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_data_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_data_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_combo_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_combo_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_combo_best_deal > 0 then b0.msisdn end
        ) takers_best_deal_combo_offer_mtd,

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_voice_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_voice_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_voice_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_voice_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_data_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_data_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_data_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_data_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_combo_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_combo_offer_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_combo_myway_plus > 0 then b0.msisdn end
        ) takers_myway_plus_combo_offer_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and upper(trim(b0.sb_status_tango)) = upper('SUCCESSFULL') then b0.msisdn end
        ) takers_myway_plus_via_om_daily,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and upper(trim(b0.sb_status_tango)) = upper('SUCCESSFULL') then b0.msisdn end
        ) takers_myway_plus_via_om_mtd,

        -----------------------------------------------
        ------           subscriptions           ------
        -----------------------------------------------
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_best_deal else 0 end
        ) subscriptions_best_deal_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_voice_best_deal else 0 end
        ) subscriptions_best_deal_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_data_best_deal else 0 end
        ) subscriptions_best_deal_data_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_combo_best_deal else 0 end
        ) subscriptions_best_deal_combo_offer_daily,

        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_myway_plus else 0 end
        ) subscriptions_myway_plus_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_voice_myway_plus else 0 end
        ) subscriptions_myway_plus_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_data_myway_plus else 0 end
        ) subscriptions_myway_plus_data_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_combo_myway_plus else 0 end
        ) subscriptions_myway_plus_combo_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b10.msisdn is not null then b10.nber_subs_om else 0 end
        ) subscriptions_myway_plus_via_om_daily,

        -----------------------------------------------
        ------              revenu               ------
        -----------------------------------------------
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_best_deal else 0 end
        ) revenu_best_deal_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_voice_best_deal else 0 end
        ) revenu_best_deal_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_data_best_deal else 0 end
        ) revenu_best_deal_data_offer_daily,
        
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_myway_plus else 0 end
        ) revenu_myway_plus_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_voice_myway_plus else 0 end
        ) revenu_myway_plus_voice_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_data_myway_plus else 0 end
        ) revenu_myway_plus_data_offer_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b10.msisdn is not null then b10.ca_subs_om else 0 end
        ) revenu_myway_plus_via_om_daily,
        
        -----------------------------------------------
        ------            Perco kpi              ------
        -----------------------------------------------
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_offer_daily_perco,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_offer_mtd_perco,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_voice_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_voice_offer_daily_perco,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_voice_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_voice_offer_mtd_perco,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_data_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_data_offer_daily_perco,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_data_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_data_offer_mtd_perco,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b0.nber_subscription_combo_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_combo_offer_daily_perco,
        count(
            distinct case when b0.event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and b0.nber_subscription_combo_best_deal_perco > 0 then b0.msisdn end
        ) takers_best_deal_combo_offer_mtd_perco,

        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_best_deal_perco else 0 end
        ) subscriptions_best_deal_offer_daily_perco,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_voice_best_deal_perco else 0 end
        ) subscriptions_best_deal_voice_offer_daily_perco,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_data_best_deal_perco else 0 end
        ) subscriptions_best_deal_data_offer_daily_perco,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.nber_subscription_combo_best_deal_perco else 0 end
        ) subscriptions_best_deal_combo_offer_daily_perco,

        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_best_deal_perco else 0 end
        ) revenu_best_deal_offer_daily_perco,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_voice_best_deal_perco else 0 end
        ) revenu_best_deal_voice_offer_daily_perco,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b0.msisdn is not null then b0.revenu_data_best_deal_perco else 0 end
        ) revenu_best_deal_data_offer_daily_perco

    from
    (
        select
            MSISDN,
            ipp_name,
            sb_status_tango,
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
            ) nber_subscription_combo_myway_plus,


            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') then nvl(amount_voix, 0) + nvl(amount_data, 0) else 0 end
            ) revenu_best_deal_perco,
            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') then amount_voix else 0 end
            ) revenu_voice_best_deal_perco,
            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') then amount_data else 0 end
            ) revenu_data_best_deal_perco,
            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') then 1 else 0 end
            ) nber_subscription_best_deal_perco,
            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') and ipp_category = 'voix' then 1 else 0 end
            ) nber_subscription_voice_best_deal_perco,
            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') and ipp_category = 'data' then 1 else 0 end
            ) nber_subscription_data_best_deal_perco,
            sum(
                case when offer_type = 'Best Deal' and upper(ipp_name) like upper('Perco-%') and ipp_category = 'combo' then 1 else 0 end
            ) nber_subscription_combo_best_deal_perco
        from
        (
            select
                msisdn,
                event_date,
                offer_type,
                ipp_category,
                ipp_name,
                sb_status_tango,
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
                            when coeff_voice = 1 then 'voix'
                            when coeff_data = 1 then 'data'
                            else 'combo'
                        end
                    ) ipp_category,
                    B9001.offer_name ipp_name,
                    'EMPTY' sb_status_tango,
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
                ) B9002 ON regexp_replace(UPPER(TRIM(B9001.offer_name)),UPPER("Perco-"),"") = UPPER(TRIM(B9002.BDLE_NAME))
            ) B900
            union all
            select
                sb_msisdn MSISDN,
                event_date,
                'Myway Plus' offer_type,
                sb_type ipp_category,
                'EMPTY' ipp_name,
                sb_status_tango,
                sb_amount_data amount_data,
                (nvl(sb_amount_onnet, 0) + nvl(sb_amount_allnet, 0)) amount_voix
            from CDR.SPARK_IT_MYWAY_REPORT
            where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###' and sb_status_in = 'SUCCESSFULL'
            and upper(sb_canal)=upper('myorange')
        ) B90
        group by msisdn,ipp_name,sb_status_tango,event_date
    ) B0
    full join
    (
        select
            distinct msisdn,
            event_date
        from CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND
        where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' AND '###SLICE_VALUE###'
    ) B1 on b0.msisdn = b1.msisdn and b0.event_date = b1.event_date
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
            UPPER(NVL(b51.SITE_NAME, b50.SITE_NAME)) SITE_NAME,
            "REGION" REGION
        FROM
        (
            select
                msisdn,
                site_name
            from
            (
                select
                    msisdn,
                    site_name,
                    row_number() over(partition by msisdn order by nbre_apparition_msisdn_site desc) line_number
                from
                (
                    SELECT
                        MSISDN,
                        SITE_NAME,
                        count(*) nbre_apparition_msisdn_site
                    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
                    WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
                    GROUP BY MSISDN, SITE_NAME, administrative_region
                ) b500
            ) x
            where line_number = 1
        ) b50
        FULL JOIN
        (
            select
                msisdn,
                site_name
            from
            (
                select
                    msisdn,
                    site_name,
                    row_number() over(partition by msisdn order by nbre_apparition_msisdn_site desc) line_number
                from
                (
                    SELECT
                        MSISDN,
                        SITE_NAME,
                        count(*) nbre_apparition_msisdn_site
                    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
                    WHERE EVENT_DATE between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
                    GROUP BY MSISDN, SITE_NAME , administrative_region
                ) b510
            ) x
            where line_number = 1
        ) b51
        ON b50.MSISDN = b51.MSISDN
    ) b5 on b0.msisdn = b5.msisdn
    --left join dim.dt_gsm_cell_code b96 on upper(b96.site_name) = upper(b5.site_name)
    group by b5.site_name, b5.region
) B