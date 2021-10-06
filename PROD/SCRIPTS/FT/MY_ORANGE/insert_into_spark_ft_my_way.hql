insert INTO MON.SPARK_FT_MY_WAY
select
    site_name,
    (subs_myway_plus_daily + subs_myway_ussd_daily) as subs_global_daily,
    subs_myway_plus_daily,
    subs_myway_ussd_daily,
    (revenu_myway_plus_daily + revenu_myway_ussd_daily) as revenu_global_daily,
    revenu_myway_plus_daily,
    revenu_myway_ussd_daily,
    current_timestamp() insert_date,
    '###SLICE_VALUE###' event_date
from
(
    select
        a2.site_name site_name,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.nber_subs_myway_plus else 0 end
        ) subs_myway_plus_daily,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.nber_subs_myway_simple else 0 end
        ) subs_myway_ussd_daily,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_plus > 0 then a0.revenu_myway_plus else 0 end
        ) revenu_myway_plus_daily,
        sum(
            case when a0.period = '###SLICE_VALUE###' and a0.nber_subs_myway_simple > 0 then a0.revenu_myway_simple else 0 end
        ) revenu_myway_ussd_daily
    from
    (
        select
            msisdn,
            period,
            sum(
                case when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') and upper(subscription_channel) like '%GOS SDP%' then bdle_cost else 0 end
            ) revenu_myway_plus,
            sum(
                case when upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') and upper(subscription_channel) like '%GOS SDP%' then nber_purchase else 0 end
            ) nber_subs_myway_plus,
            sum(
                case when upper(bdle_name) in ('IPP ON DEMAND BLACK MAIN', 'IPP PREPAID MYWAY DATA1', 'IPP PREPAID MYWAY DATA2', 'IPP PREPAID MYWAY DATA3', 'IPP PREPAID MYWAY DATA4', 'IPP PREPAID MYWAY DATA5', 'IPP PREPAID MYWAY DATA6', 'IPP PREPAID MYWAY DATA', 'IPP PREPAID MYWAY DATA5 PROMO') or (upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') and upper(subscription_channel) not like '%GOS SDP%') then bdle_cost else 0 end
            ) revenu_myway_simple,
            sum(
                case when upper(bdle_name) in ('IPP ON DEMAND BLACK MAIN', 'IPP PREPAID MYWAY DATA1', 'IPP PREPAID MYWAY DATA2', 'IPP PREPAID MYWAY DATA3', 'IPP PREPAID MYWAY DATA4', 'IPP PREPAID MYWAY DATA5', 'IPP PREPAID MYWAY DATA6', 'IPP PREPAID MYWAY DATA', 'IPP PREPAID MYWAY DATA5 PROMO') or (upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL') and upper(subscription_channel) not like '%GOS SDP%') then nber_purchase else 0 end
            ) nber_subs_myway_simple
        from mon.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
        where period = '###SLICE_VALUE###'
            and upper(bdle_name) in ('IPP MYWAY DATA DIGITAL', 'IPP MYWAY VOICE DIGITAL', 'IPP MYWAY COMBO DIGITAL', 'IPP ON DEMAND BLACK MAIN', 'IPP PREPAID MYWAY DATA1', 'IPP PREPAID MYWAY DATA2', 'IPP PREPAID MYWAY DATA3', 'IPP PREPAID MYWAY DATA4', 'IPP PREPAID MYWAY DATA5', 'IPP PREPAID MYWAY DATA6', 'IPP PREPAID MYWAY DATA', 'IPP PREPAID MYWAY DATA5 PROMO')
        group by msisdn, period
    ) a0
    left JOIN
    (
        SELECT
            a20.MSISDN,
            a20.event_date,
            UPPER(NVL(a20.SITE_NAME, a21.SITE_NAME)) SITE_NAME
        FROM
        (
            SELECT
                MSISDN,
                event_date,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN, event_date
        ) a20
        LEFT JOIN
        (
            SELECT
                MSISDN,
                event_date,
                MAX(SITE_NAME) SITE_NAME
            FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
            WHERE EVENT_DATE = '###SLICE_VALUE###'
            GROUP BY MSISDN, event_date
        ) a21 ON a20.MSISDN = a21.MSISDN and a20.event_date = a21.event_date
    ) a2 on a0.msisdn = a2.msisdn and a0.period = a2.event_date
    group by a2.site_name
) a
