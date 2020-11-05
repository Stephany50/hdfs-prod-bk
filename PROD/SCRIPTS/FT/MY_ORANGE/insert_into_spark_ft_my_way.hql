insert INTO MON.SPARK_FT_MY_WAY
select
    takers_global_daily,
    takers_global_weekly,
    takers_global_mtd,
    takers_myway_plus_daily,
    takers_myway_plus_weekly,
    takers_myway_plus_mtd,
    takers_myway_ussd_daily,
    takers_myway_ussd_weekly,
    takers_myway_ussd_mtd,
    (subs_myway_plus_daily + subs_myway_ussd_daily) as subs_global_daily,
    (subs_myway_plus_weekly + subs_myway_ussd_weekly) as subs_global_weekly,
    (subs_myway_plus_mtd + subs_myway_ussd_mtd) as subs_global_mtd,
    subs_myway_plus_daily,
    subs_myway_plus_weekly,
    subs_myway_plus_mtd,
    subs_myway_ussd_daily,
    subs_myway_ussd_weekly,
    subs_myway_ussd_mtd,
    (revenu_myway_plus_daily + revenu_myway_ussd_daily) as revenu_global_daily,
    (revenu_myway_plus_weekly + revenu_myway_ussd_weekly) as revenu_global_weekly,
    (revenu_myway_plus_mtd + revenu_myway_ussd_mtd) as revenu_globlal_mtd,
    revenu_myway_plus_daily,
    revenu_myway_plus_weekly,
    revenu_myway_plus_mtd,
    revenu_myway_ussd_daily,
    revenu_myway_ussd_weekly,
    revenu_myway_ussd_mtd,
    current_timestamp() insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select
        count(
            distinct case when a0.period = '###SLICE_VALUE###' then a0.msisdn end
        ) takers_global_daily,
        count(
            distinct case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' then a0.msisdn end
        ) takers_global_weekly,
        count(distinct a0.msisdn) takers_global_mtd,
        count(
            distinct case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.msisdn end
        ) takers_myway_plus_daily,
        count(
            distinct case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.msisdn end
        ) takers_myway_plus_weekly,
        count(
            distinct case when a0.nber_subs_myway_plus > 0 then a0.msisdn end
        ) takers_myway_plus_mtd,
        count(
            distinct case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.msisdn end
        ) takers_myway_ussd_daily,
        count(
            distinct case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.msisdn end
        ) takers_myway_ussd_weekly,
        count(
            distinct case when a0.nber_subs_myway_simple > 0 then a0.msisdn end
        ) takers_myway_ussd_mtd,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.nber_subs_myway_plus else 0 end
        ) subs_myway_plus_daily,
        sum(
            case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.nber_subs_myway_plus else 0 end
        ) subs_myway_plus_weekly,
        sum(
            case when a0.nber_subs_myway_plus > 0 then a0.nber_subs_myway_plus else 0 end
        ) subs_myway_plus_mtd,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.nber_subs_myway_simple else 0 end
        ) subs_myway_ussd_daily,
        sum(
            case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.nber_subs_myway_simple else 0 end
        ) subs_myway_ussd_weekly,
        sum(
            case when a0.nber_subs_myway_simple > 0 then a0.nber_subs_myway_simple else 0 end
        ) subs_myway_ussd_mtd,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.revenu_myway_plus else 0 end
        ) revenu_myway_plus_daily,
        sum(
            case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.revenu_myway_plus else 0 end
        ) revenu_myway_plus_weekly,
        sum(
            case when a0.nber_subs_myway_plus > 0 then a0.revenu_myway_plus else 0 end
        ) revenu_myway_plus_mtd,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.revenu_myway_simple else 0 end
        ) revenu_myway_ussd_daily,
        sum(
            case when a0.period between date_sub('###SLICE_VALUE###', 6) and '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.revenu_myway_simple else 0 end
        ) revenu_myway_ussd_weekly,
        sum(
            case when a0.nber_subs_myway_simple > 0 then a0.revenu_myway_simple else 0 end
        ) revenu_myway_ussd_mtd
    from
    (
        select
            msisdn,
            period,
            sum(
                case when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL', 'IPP FLAG MYWAY DATA') then bdle_cost else 0 end
            ) revenu_myway_plus,
            sum(
                case when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL', 'IPP FLAG MYWAY DATA') then nber_purchase else 0 end
            ) nber_subs_myway_plus,
            sum(
                case when upper(bdle_name) in ('IPP ON DEMAND BLACK MAIN', 'IPP PREPAID MYWAY DATA1', 'IPP PREPAID MYWAY DATA2', 'IPP PREPAID MYWAY DATA3', 'IPP PREPAID MYWAY DATA4', 'IPP PREPAID MYWAY DATA5', 'IPP PREPAID MYWAY DATA6', 'IPP PREPAID MYWAY DATA', 'IPP PREPAID MYWAY DATA5 PROMO') then bdle_cost else 0 end
            ) revenu_myway_simple,
            sum(
                case when upper(bdle_name) in ('IPP ON DEMAND BLACK MAIN', 'IPP PREPAID MYWAY DATA1', 'IPP PREPAID MYWAY DATA2', 'IPP PREPAID MYWAY DATA3', 'IPP PREPAID MYWAY DATA4', 'IPP PREPAID MYWAY DATA5', 'IPP PREPAID MYWAY DATA6', 'IPP PREPAID MYWAY DATA', 'IPP PREPAID MYWAY DATA5 PROMO') then nber_purchase else 0 end
            ) nber_subs_myway_simple
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        where period between substr('###SLICE_VALUE###', 1, 7)||'-01' and '###SLICE_VALUE###'
            and upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL', 'IPP FLAG MYWAY DATA', 'IPP ON DEMAND BLACK MAIN', 'IPP PREPAID MYWAY DATA1', 'IPP PREPAID MYWAY DATA2', 'IPP PREPAID MYWAY DATA3', 'IPP PREPAID MYWAY DATA4', 'IPP PREPAID MYWAY DATA5', 'IPP PREPAID MYWAY DATA6', 'IPP PREPAID MYWAY DATA', 'IPP PREPAID MYWAY DATA5 PROMO')
        group by msisdn, period
    ) a0
) a
