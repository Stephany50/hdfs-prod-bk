SELECT
    from_unixtime(unix_timestamp(EVENT_DATE), 'dd/MM/yyyy') EVENT_DATE,
    MSISDN,
    LOST_TYPE,
    SITE,
    VILLE,
    REGION,
    from_unixtime(unix_timestamp(DATE_DERNIERE_LOCALISATION), 'dd/MM/yyyy') DATE_DERNIERE_LOCALISATION,
    from_unixtime(unix_timestamp(ACTIVATION_DATE), 'dd/MM/yyyy') ACTIVATION_DATE
FROM MON.SPARK_FT_CBM_CHURN_DAILY
WHERE EVENT_DATE = "###SLICE_VALUE###"