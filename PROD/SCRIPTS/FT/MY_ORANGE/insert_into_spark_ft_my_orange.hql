insert INTO MON.SPARK_FT_MY_ORANGE
select
    users_follow_daily,
    users_follow_weekly,
    users_follow_mtd,
    users_backend_daily,
    users_backend_weekly,
    users_backend_mtd,
    new_users_backend_daily,
    new_users_backend_weekly,
    new_users_backend_mtd,
    new_users_backend_daily*100/users_backend_daily new_users_backend_daily_percentage,
    new_users_backend_weekly*100/users_backend_weekly new_users_backend_weekly_percentage,
    new_users_backend_mtd*100/users_backend_mtd new_users_backend_mtd_percentage,
    welcome_pack_takers_global_daily,
    welcome_pack_takers_global_weekly,
    welcome_pack_takers_global_mtd,
    welcome_pack_takers_new_backend_daily,
    welcome_pack_takers_new_backend_weekly,
    welcome_pack_takers_new_backend_mtd,
    welcome_pack_takers_new_backend_daily*100/welcome_pack_takers_global_daily welcome_pack_takers_new_backend_daily_percentage,
    welcome_pack_takers_new_backend_weekly*100/welcome_pack_takers_global_weekly welcome_pack_takers_new_backend_weekly_percentage,
    welcome_pack_takers_new_backend_mtd*100/welcome_pack_takers_global_mtd welcome_pack_takers_new_backend_mtd_percentage,
    welcome_pack_data_usage_daily,
    welcome_pack_data_usage_weekly,
    welcome_pack_data_usage_mtd,
    myway_plus_takers_backend_daily,
    myway_plus_takers_backend_weekly,
    myway_plus_takers_backend_mtd,
    myway_plus_subs_backend_daily,
    myway_plus_subs_backend_weekly,
    myway_plus_subs_backend_mtd,
    myway_plus_revenu_backend_daily,
    myway_plus_revenu_backend_weekly,
    myway_plus_revenu_backend_mtd,
    revenu_global_backend_daily,
    revenu_global_backend_weekly,
    revenu_global_backend_mtd,
    revenu_data_backend_daily,
    revenu_data_backend_weekly,
    revenu_data_backend_mtd,
    revenu_voix_backend_daily,
    revenu_voix_backend_weekly,
    revenu_voix_backend_mtd,
    usage_voix_backend_daily,
    usage_voix_backend_weekly,
    usage_voix_backend_mtd,
    usage_data_backend_daily,
    usage_data_backend_weekly,
    usage_data_backend_mtd,
    revenu_global_subs_myorange_daily,
    revenu_global_subs_myorange_weekly,
    revenu_global_subs_myorange_mtd,
    revenu_data_subs_myorange_daily,
    revenu_data_subs_myorange_weekly,
    revenu_data_subs_myorange_mtd,
    revenu_voix_subs_myorange_daily,
    revenu_voix_subs_myorange_weekly,
    revenu_voix_subs_myorange_mtd,
    current_timestamp() insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select
        sum(
            case when event_date = '###SLICE_VALUE###' then nbre_users_follow else 0 end
        ) users_follow_daily,
        sum(
            case when event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then nbre_users_follow else 0 end
        ) users_follow_weekly,
        sum(nbre_users_follow) users_follow_mtd
    from CDR.SPARK_IT_MY_ORANGE_USERS_FOLLOW
    where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
) A,
(
    select
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' then b0.msisdn end
        ) users_backend_daily,
        count(
            distinct case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then b0.msisdn end
        ) users_backend_weekly,
        count(distinct b0.msisdn) users_backend_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b1.activation_date >= '${date_debut_perco}' then b0.msisdn end
        ) new_users_backend_daily,
        count(
            distinct case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b1.activation_date >= '${date_debut_perco}' then b0.msisdn end
        ) new_users_backend_weekly,
        count(
            distinct case when b1.activation_date >= '${date_debut_perco}' then b0.msisdn end
        ) new_users_backend_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 then b0.msisdn end
        ) welcome_pack_takers_global_daily,
        count(
            distinct case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 then b0.msisdn end
        ) welcome_pack_takers_global_weekly,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 then b0.msisdn end
        ) welcome_pack_takers_global_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b1.activation_date >= '${date_debut_perco}' then b0.msisdn end
        ) welcome_pack_takers_new_backend_daily,
        count(
            distinct case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b1.activation_date >= '${date_debut_perco}' then b0.msisdn end
        ) welcome_pack_takers_new_backend_weekly,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_welcome_pack > 0 and b1.activation_date >= '${date_debut_perco}' then b0.msisdn end
        ) welcome_pack_takers_new_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b3.msisdn is not null then b3.used_amt else 0 end
        ) welcome_pack_data_usage_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b3.msisdn is not null then b3.used_amt else 0 end
        ) welcome_pack_data_usage_weekly,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b3.msisdn is not null then b3.used_amt else 0 end
        ) welcome_pack_data_usage_mtd,
        count(
            distinct case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b0.msisdn end
        ) myway_plus_takers_backend_daily,
        count(
            distinct case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b0.msisdn end
        ) myway_plus_takers_backend_weekly,
        count(
            distinct case when b2.msisdn is not null and b2.nber_subs_myway > 0 then b0.msisdn end
        ) myway_plus_takers_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.nber_subs_myway else 0 end
        ) myway_plus_subs_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.nber_subs_myway else 0 end
        ) myway_plus_subs_backend_weekly,
        sum(
            case when b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.nber_subs_myway else 0 end
        ) myway_plus_subs_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.revenu_myway else 0 end
        ) myway_plus_revenu_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.revenu_myway else 0 end
        ) myway_plus_revenu_backend_weekly,
        sum(
            case when b2.msisdn is not null and b2.nber_subs_myway > 0 then b2.revenu_myway else 0 end
        ) myway_plus_revenu_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.revenu_global else 0 end
        ) revenu_global_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then b1.revenu_global else 0 end
        ) revenu_global_backend_weekly,
        sum(b1.revenu_global) revenu_global_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.revenu_data else 0 end
        ) revenu_data_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then b1.revenu_data else 0 end
        ) revenu_data_backend_weekly,
        sum(b1.revenu_data) revenu_data_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.revenu_voix else 0 end
        ) revenu_voix_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then b1.revenu_voix else 0 end
        ) revenu_voix_backend_weekly,
        sum(b1.revenu_voix) revenu_voix_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.usage_voix else 0 end
        ) usage_voix_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then b1.usage_voix else 0 end
        ) usage_voix_backend_weekly,
        sum(b1.usage_voix) usage_voix_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' then b1.usage_data else 0 end
        ) usage_data_backend_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then b1.usage_data else 0 end
        ) usage_data_backend_weekly,
        sum(b1.usage_data) usage_data_backend_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_global else 0 end
        ) revenu_global_subs_myorange_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_global else 0 end
        ) revenu_global_subs_myorange_weekly,
        sum(
            case when b2.msisdn is not null then b2.revenu_subs_global else 0 end
        ) revenu_global_subs_myorange_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_data else 0 end
        ) revenu_data_subs_myorange_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_data else 0 end
        ) revenu_data_subs_myorange_weekly,
        sum(
            case when b2.msisdn is not null then b2.revenu_subs_data else 0 end
        ) revenu_data_subs_myorange_mtd,
        sum(
            case when b0.event_date = '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_voix else 0 end
        ) revenu_voix_subs_myorange_daily,
        sum(
            case when b0.event_date between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and b2.msisdn is not null then b2.revenu_subs_voix else 0 end
        ) revenu_voix_subs_myorange_weekly,
        sum(
            case when b2.msisdn is not null then b2.revenu_subs_voix else 0 end
        ) revenu_voix_subs_myorange_mtd
    from
    (   
        select
            distinct msisdn,
            event_date
        from CDR.SPARK_IT_MY_ORANGE_USERS_BACKEND
        where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
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
        where event_date between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
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
        where period between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
            and upper(subscription_channel) like '%GOS SDP%'
            --and upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL', 'IPP WELCOME MYORANGE')
        group by msisdn, period
    ) B2 on b0.msisdn = b2.msisdn and b0.event_date = b2.period
    left join
    (
        select
            msisdn,
            period,
            sum(used_amt) used_amt
        from MON.SPARK_FT_CBM_DA_USAGE_DAILY
        where period between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
            and upper(da_id) in ('SET_DATA_MOBILE')
            and activity_type = 'DATA'
        group by msisdn, period
    ) b3 on b0.msisdn = b3.msisdn and b0.event_date = b3.period
) B
