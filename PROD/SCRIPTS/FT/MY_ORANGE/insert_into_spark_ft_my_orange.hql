insert INTO MON.SPARK_FT_MY_ORANGE
select
    site_name,
    users_backend_daily,
    users_backend_since_perco_start,
    new_users_backend_daily,
    new_users_backend_since_perco_start,
    new_users_backend_daily*100/users_backend_daily new_users_backend_daily_percentage,
    new_users_backend_since_perco_start*100/users_backend_since_perco_start new_users_backend_since_perco_start_percentage,
    welcome_pack_takers_global_daily,
    welcome_pack_takers_global_since_perco_start,
    welcome_pack_takers_new_backend_daily,
    welcome_pack_takers_new_backend_since_perco_start,
    welcome_pack_takers_new_backend_daily*100/welcome_pack_takers_global_daily welcome_pack_takers_new_backend_daily_percentage,
    welcome_pack_takers_new_backend_since_perco_start*100/welcome_pack_takers_global_since_perco_start welcome_pack_takers_new_backend_since_perco_start_percentage,
    welcome_pack_data_usage_daily,
    welcome_pack_data_usage_since_perco_start,
    myway_plus_takers_backend_daily,
    myway_plus_takers_backend_since_perco_start,
    myway_plus_subs_backend_daily,
    myway_plus_subs_backend_since_perco_start,
    myway_plus_revenu_backend_daily,
    myway_plus_revenu_backend_since_perco_start,
    revenu_global_backend_daily,
    revenu_global_backend_since_perco_start,
    revenu_data_backend_daily,
    revenu_data_backend_since_perco_start,
    revenu_voix_backend_daily,
    revenu_voix_backend_since_perco_start,
    usage_voix_backend_daily,
    usage_voix_backend_since_perco_start,
    usage_data_backend_daily,
    usage_data_backend_since_perco_start,
    revenu_global_subs_myorange_daily,
    revenu_global_subs_myorange_since_perco_start,
    revenu_data_subs_myorange_daily,
    revenu_data_subs_myorange_since_perco_start,
    revenu_voix_subs_myorange_daily,
    revenu_voix_subs_myorange_since_perco_start,
    active_om_users_daily,
    active_om_users_since_perco_start,

    users_backend_not_appeared_before_perco_daily,
    users_backend_not_appeared_before_perco_since_perco_start,
    users_backend_appeared_three_months_before_perco_daily,
    users_backend_appeared_three_months_before_perco_since_perco_start,
    users_backend_appeared_not_three_months_before_perco_daily,
    users_backend_appeared_not_three_months_before_perco_since_perco_start,
    welcome_pack_takers_not_appeared_before_perco_daily,
    welcome_pack_takers_not_appeared_before_perco_since_perco_start,
    welcome_pack_takers_appeared_three_months_before_perco_daily,
    welcome_pack_takers_appeared_three_months_before_perco_since_perco_start,
    welcome_pack_takers_appeared_not_three_months_before_perco_daily,
    welcome_pack_takers_appeared_not_three_months_before_perco_since_perco_start,
    myway_plus_takers_backend_not_appeared_before_perco_daily,
    myway_plus_takers_backend_not_appeared_before_perco_since_perco_start,
    myway_plus_takers_backend_appeared_three_months_before_perco_daily,
    myway_plus_takers_backend_appeared_three_months_before_perco_since_perco_start,
    myway_plus_takers_backend_appeared_not_three_months_before_perco_daily,
    myway_plus_takers_backend_appeared_not_three_months_before_perco_since_perco_start,
    users_backend_migrator_daily,
    users_backend_migrator_since_perco_start,
    welcome_pack_takers_migrator_daily,
    welcome_pack_takers_migrator_since_perco_start,
    myway_plus_takers_backend_migrator_daily,
    myway_plus_takers_backend_migrator_since_perco_start,

    current_timestamp() insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select
        b5.site_name site_name,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' then b0.msisdn end
        ) users_backend_daily,
        count(distinct b0.msisdn) users_backend_since_perco_start,


        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b6.msisdn is null then b0.msisdn end
        ) users_backend_not_appeared_before_perco_daily, -- Not appeared before perco
        count(
            distinct case when b6.msisdn is null then b0.msisdn end
        ) users_backend_not_appeared_before_perco_since_perco_start, -- Not appeared before perco
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b6.msisdn is not null and last_month_indice between 8 and 10 then b0.msisdn end
        ) users_backend_appeared_three_months_before_perco_daily, -- Appeared three months before perco
        count(
            distinct case when b6.msisdn is not null and last_month_indice between 8 and 10 then b0.msisdn end
        ) users_backend_appeared_three_months_before_perco_since_perco_start, -- Appeared three months before perco
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b6.msisdn is not null and last_month_indice between 1 and 7 then b0.msisdn end
        ) users_backend_appeared_not_three_months_before_perco_daily, -- Appeared before perco but not three months before
        count(
            distinct case when b6.msisdn is not null and last_month_indice between 1 and 7 then b0.msisdn end
        ) users_backend_appeared_not_three_months_before_perco_since_perco_start, -- Appeared before perco but not three months before

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b7.msisdn is not null and b6.msisdn is null then b0.msisdn end
        ) users_backend_migrator_daily,
        count(
            distinct case when b7.msisdn is not null and b6.msisdn is null then b0.msisdn end
        ) users_backend_migrator_since_perco_start,


        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b1.activation_date >= '${hivevar:date_debut_perco}' then b0.msisdn end
        ) new_users_backend_daily,
        count(
            distinct case when b1.activation_date >= '${hivevar:date_debut_perco}' then b0.msisdn end
        ) new_users_backend_since_perco_start,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 then b0.msisdn end
        ) welcome_pack_takers_global_daily,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 then b0.msisdn end
        ) welcome_pack_takers_global_since_perco_start,


        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b6.msisdn is null then b0.msisdn end
        ) welcome_pack_takers_not_appeared_before_perco_daily, -- Not appeared before perco
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b6.msisdn is null then b0.msisdn end
        ) welcome_pack_takers_not_appeared_before_perco_since_perco_start, -- Not appeared before perco
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b6.msisdn is not null and last_month_indice between 8 and 10 then b0.msisdn end
        ) welcome_pack_takers_appeared_three_months_before_perco_daily, -- Appeared three months before perco
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b6.msisdn is not null and last_month_indice between 8 and 10 then b0.msisdn end
        ) welcome_pack_takers_appeared_three_months_before_perco_since_perco_start, -- Appeared three months before perco
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b6.msisdn is not null and last_month_indice between 1 and 7 then b0.msisdn end
        ) welcome_pack_takers_appeared_not_three_months_before_perco_daily, -- Appeared before perco but not three months before
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b6.msisdn is not null and last_month_indice between 1 and 7 then b0.msisdn end
        ) welcome_pack_takers_appeared_not_three_months_before_perco_since_perco_start, -- Appeared before perco but not three months before

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b7.msisdn is not null and b6.msisdn is null then b0.msisdn end
        ) welcome_pack_takers_migrator_daily,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b7.msisdn is not null and b6.msisdn is null then b0.msisdn end
        ) welcome_pack_takers_migrator_since_perco_start,


        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b1.activation_date >= '${hivevar:date_debut_perco}' then b0.msisdn end
        ) welcome_pack_takers_new_backend_daily,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b1.activation_date >= '${hivevar:date_debut_perco}' then b0.msisdn end
        ) welcome_pack_takers_new_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b3.msisdn is not null then b3.used_amt else 0 end
        ) welcome_pack_data_usage_daily,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b3.msisdn is not null then b3.used_amt else 0 end
        ) welcome_pack_data_usage_since_perco_start,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b0.msisdn end
        ) myway_plus_takers_backend_daily,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_myway > 0 then b0.msisdn end
        ) myway_plus_takers_backend_since_perco_start,


        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 and b6.msisdn is null then b0.msisdn end
        ) myway_plus_takers_backend_not_appeared_before_perco_daily, -- Not appeared before perco
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_myway > 0 and b6.msisdn is null then b0.msisdn end
        ) myway_plus_takers_backend_not_appeared_before_perco_since_perco_start, -- Not appeared before perco
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 and b6.msisdn is not null and last_month_indice between 8 and 10 then b0.msisdn end
        ) myway_plus_takers_backend_appeared_three_months_before_perco_daily, -- Appeared three months before perco
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_myway > 0 and b6.msisdn is not null and last_month_indice between 8 and 10 then b0.msisdn end
        ) myway_plus_takers_backend_appeared_three_months_before_perco_since_perco_start, -- Appeared three months before perco
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 and b6.msisdn is not null and last_month_indice between 1 and 7 then b0.msisdn end
        ) myway_plus_takers_backend_appeared_not_three_months_before_perco_daily, -- Appeared before perco but not three months before
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_myway > 0 and b6.msisdn is not null and last_month_indice between 1 and 7 then b0.msisdn end
        ) myway_plus_takers_backend_appeared_not_three_months_before_perco_since_perco_start, -- Appeared before perco but not three months before

        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 and b7.msisdn is not null and b6.msisdn is null then b0.msisdn end
        ) myway_plus_takers_backend_migrator_daily,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_myway > 0 and b7.msisdn is not null and b6.msisdn is null then b0.msisdn end
        ) myway_plus_takers_backend_migrator_since_perco_start,


        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.nber_subs_myway else 0 end
        ) myway_plus_subs_backend_daily,
        sum(
            case when b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.nber_subs_myway else 0 end
        ) myway_plus_subs_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.revenu_myway else 0 end
        ) myway_plus_revenu_backend_daily,
        sum(
            case when b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.revenu_myway else 0 end
        ) myway_plus_revenu_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.revenu_global else 0 end
        ) revenu_global_backend_daily,
        sum(b1.revenu_global) revenu_global_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.revenu_data else 0 end
        ) revenu_data_backend_daily,
        sum(b1.revenu_data) revenu_data_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.revenu_voix else 0 end
        ) revenu_voix_backend_daily,
        sum(b1.revenu_voix) revenu_voix_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.usage_voix else 0 end
        ) usage_voix_backend_daily,
        sum(b1.usage_voix) usage_voix_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.usage_data else 0 end
        ) usage_data_backend_daily,
        sum(b1.usage_data) usage_data_backend_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_global else 0 end
        ) revenu_global_subs_myorange_daily,
        sum(
            case when b2.msisdn is not null then b2.revenu_subs_global else 0 end
        ) revenu_global_subs_myorange_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_data else 0 end
        ) revenu_data_subs_myorange_daily,
        sum(
            case when b2.msisdn is not null then b2.revenu_subs_data else 0 end
        ) revenu_data_subs_myorange_since_perco_start,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_voix else 0 end
        ) revenu_voix_subs_myorange_daily,
        sum(
            case when b2.msisdn is not null then b2.revenu_subs_voix else 0 end
        ) revenu_voix_subs_myorange_since_perco_start,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b4.msisdn is not NULL then b0.msisdn end
        ) active_om_users_daily,
        count(
            distinct case when b4.msisdn is not NULL then b0.msisdn end
        ) active_om_users_since_perco_start
    from
    (   
        select
            distinct msisdn,
            event_date
        from CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND
        where event_date between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
    ) B0
    left join
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
        where event_date between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
        group by msisdn, event_date
    ) B1 on b0.msisdn = b1.msisdn and b0.event_date = b1.event_date
    left join
    (
        select
            msisdn,
            period,
            sum(
                case when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') then bdle_cost else 0 end
            ) revenu_myway,
            sum(
                case when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') then nber_purchase else 0 end
            ) nber_subs_myway,
            sum(
                case when upper(bdle_name) in ('IPP WELCOME MYORANGE') then nber_purchase else 0 end
            ) nber_subs_welcome_pack,
            sum(bdle_cost) revenu_subs_global,
            sum(amount_data) revenu_subs_data,
            sum(amount_voice_onnet + amount_voice_offnet + amount_voice_inter + amount_voice_roaming + amount_sms_onnet + amount_sms_offnet + amount_sms_inter + amount_sms_roaming) revenu_subs_voix
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        where period between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
            and upper(subscription_channel) like '%GOS SDP%'
        group by msisdn, period
    ) B2 on b0.msisdn = b2.msisdn and b0.event_date = b2.period
    left join
    (
        select
            msisdn,
            period,
            sum(used_amt) used_amt
        from MON.SPARK_FT_CBM_DA_USAGE_DAILY
        where period between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
            and upper(da_id) in ('SET_DATA_MOBILE')
            and activity_type = 'DATA'
        group by msisdn, period
    ) b3 on b0.msisdn = b3.msisdn and b0.event_date = b3.period
    left join
    (
        select
            distinct receiver_msisdn msisdn,
            transfer_datetime event_date
        from cdr.spark_it_omny_transactions
        where transfer_datetime between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
            and transfer_status = 'TS'
            and service_type in ('CASHIN', 'CASHOUT', 'ENT2REG', 'RC', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG')
        union
        select
            distinct sender_msisdn msisdn,
            transfer_datetime event_date
        from cdr.spark_it_omny_transactions
        where transfer_datetime between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
            and transfer_status = 'TS'
            and service_type in ('CASHIN', 'CASHOUT', 'ENT2REG', 'RC', 'MERCHPAY', 'BILLPAY', 'P2P', 'P2PNONREG')
    ) b4 on b0.msisdn = b4.msisdn and b0.event_date = b4.event_date
    left join
    (
        select
            msisdn,
            --lpad(last_month, 2, '0') last_month,
            last_month last_month_indice
        from tmp.ancien_user_myo
    ) b6 on b0.msisdn = b6.msisdn
    left join
    (
        select msisdn
        from tmp.prior_myway_takers
    ) b7 on b0.msisdn = b7.msisdn
    left JOIN
    (
        SELECT
            nvl(b50.MSISDN, b51.MSISDN) MSISDN,
            UPPER(NVL(b50.SITE_NAME, b51.SITE_NAME)) SITE_NAME
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
                WHERE EVENT_DATE between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
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
                WHERE EVENT_DATE between '${hivevar:date_debut_perco}' and '###SLICE_VALUE###'
                GROUP BY MSISDN, SITE_NAME
            ) b510
        ) b51 ON b50.MSISDN = b51.MSISDN
    ) b5 on b0.msisdn = b5.msisdn
    group by b5.site_name
) B
