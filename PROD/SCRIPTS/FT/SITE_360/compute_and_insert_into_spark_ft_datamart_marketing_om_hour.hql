insert into MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
select
    sender_msisdn
    , receiver_msisdn
    , a.hour_period event_hour
    , transaction_amount
    , revenu_om
    , service_type
    , merchant_code
    , merchant_fist_name
    , merchant_last_name
    , merchant_short_name
    , site_name
    , town
    , region
    , commercial_region
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    select
        sender_msisdn,
        receiver_msisdn,
        from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH') hour_period,
        service_type,
        sum(transaction_amount) transaction_amount,
        sum(service_charge_received) revenu_om
    from cdr.spark_it_omny_transactions
    where transfer_datetime = '###SLICE_VALUE###'
        and transfer_status='TS'
        and service_type in ('CASHIN', 'CASHOUT', 'P2P', 'P2PNONREG', 'ENT2REG', 'RC', 'MERCHPAY', 'BILLPAY')
    group by sender_msisdn,
        receiver_msisdn,
        from_unixtime(unix_timestamp(transfer_datetime_nq,'yy-MM-dd HH:mm:ss'), 'HH'),
        service_type
) a
left join
(
    select
        msisdn,
        hour_period,
        site_name,
        town,
        region,
        commercial_region
    from mon.spark_ft_client_site_traffic_hour
    where event_date = '###SLICE_VALUE###'
) b on a.sender_msisdn = b.msisdn and a.hour_period = b.hour_period
left join
(
    select
        msisdn,
        user_grade_code,
        agent_code merchant_code,
        nvl(user_first_name, '') merchant_fist_name,
        nvl(user_last_name, '') merchant_last_name,
        nvl(user_short_name, '') merchant_short_name
    from cdr.spark_it_om_all_users
    where original_file_date = '###SLICE_VALUE###' and trim(upper(user_grade_code)) in (select trim(upper(user_grade_code)) from dim.dt_om_merchant_user_grade_codes)
) c on a.receiver_msisdn = c.msisdn
