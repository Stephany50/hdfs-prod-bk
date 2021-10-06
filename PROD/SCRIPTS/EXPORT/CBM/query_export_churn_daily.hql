SELECT
    from_unixtime(unix_timestamp(EVENT_DATE), 'dd/MM/yyyy') event_date,
    msisdn,
    lost_type,
    site,
    ville,
    region,
    from_unixtime(unix_timestamp(DATE_DERNIERE_LOCALISATION), 'dd/MM/yyyy') date_derniere_localisation,
    from_unixtime(unix_timestamp(ACTIVATION_DATE), 'dd/MM/yyyy') activation_date
FROM MON.SPARK_FT_CBM_CHURN_DAILY
WHERE EVENT_DATE = "###SLICE_VALUE###"