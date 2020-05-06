insert into mon.spark_FT_ACTIVATION_BY_IDENTIF_DAY
    select
    identificateur,
     count(distinct served_party_msisdn) msisdn_count,
     CURRENT_TIMESTAMP  INSERT_DATE,
     transaction_date activation_date
    from
    (
    select distinct transaction_date, served_party_msisdn from MON.SPARK_ft_subscription
    where transaction_date = '###SLICE_VALUE###'
        and upper(subscription_service) = 'PPS FIRST DIAL'
    ) A
    left join (select * from DIM.spark_DT_BASE_IDENTIFICATION) B
    where A.served_party_msisdn = B.msisdn
    group by transaction_date, identificateur