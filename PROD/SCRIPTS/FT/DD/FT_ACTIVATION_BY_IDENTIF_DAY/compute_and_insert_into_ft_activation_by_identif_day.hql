insert into mon.spark_FT_ACTIVATION_BY_IDENTIF_DAY
    select
    identificateur,
     count(distinct served_party_msisdn),
     CURRENT_TIMESTAMP  INSERT_DATE,
     transaction_date,
    from
    (
    select distinct transaction_date, served_party_msisdn from MON.SPARK_ft_subscription
    where transaction_date = d_slice_value
        and upper(subscription_service) = 'PPS FIRST DIAL'
    ) A
    left join (select * from DIM.spark_DT_BASE_IDENTIFICATION) B
    where served_party_msisdn = msisdn