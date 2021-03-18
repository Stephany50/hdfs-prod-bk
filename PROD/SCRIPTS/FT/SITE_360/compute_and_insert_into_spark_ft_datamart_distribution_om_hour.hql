insert into MON.spark_ft_datamart_distribution_om_hour
select
    a.msisdn
    , a.HOUR_PERIOD event_hour
    , transaction_amount
    , service_type
    , site_name
    , town
    , region
    , commercial_region
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
from
(
    SELECT
        SENDER_MSISDN MSISDN
        , SERVICE_TYPE
        , FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME_NQ,'yy-MM-dd HH:mm:ss'), 'HH') HOUR_PERIOD
        , SUM(TRANSACTION_AMOUNT) TRANSACTION_AMOUNT
    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME = '###SLICE_VALUE###'
        AND TRANSFER_STATUS='TS'
        AND SERVICE_TYPE IN ('CASHIN')
        AND SENDER_MSISDN NOT IN ('691102773', '691100052', '691104129', '691104173', '691201081', '691201209', '691104176', '691100084', '691100068', '691100025', '691100118', '656981241', '656981242', '656981107', '656981500', '656981504', '656981155', '656981156', '656980927', '656980928', '694257819', '698156555', '698157555')
    GROUP BY SENDER_MSISDN
        , SERVICE_TYPE
        , FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME_NQ,'yy-MM-dd HH:mm:ss'), 'HH')
    UNION ALL
    SELECT
        RECEIVER_MSISDN MSISDN
        , SERVICE_TYPE
        , FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME_NQ,'yy-MM-dd HH:mm:ss'), 'HH') HOUR_PERIOD
        , SUM(TRANSACTION_AMOUNT) TRANSACTION_AMOUNT
    FROM CDR.SPARK_IT_OMNY_TRANSACTIONS
    WHERE TRANSFER_DATETIME = '###SLICE_VALUE###'
        AND TRANSFER_STATUS='TS'
        AND SERVICE_TYPE IN ('CASHOUT', 'COUTBYCODE')
        AND SENDER_MSISDN NOT IN ('691102773', '691100052', '691104129', '691104173', '691201081', '691201209', '691104176', '691100084', '691100068', '691100025', '691100118', '656981241', '656981242', '656981107', '656981500', '656981504', '656981155', '656981156', '656980927', '656980928', '694257819', '698156555', '698157555')
    GROUP BY RECEIVER_MSISDN
        , SERVICE_TYPE
        , FROM_UNIXTIME(UNIX_TIMESTAMP(TRANSFER_DATETIME_NQ,'yy-MM-dd HH:mm:ss'), 'HH')
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
) b
on a.msisdn = b.msisdn and a.hour_period = b.hour_period
