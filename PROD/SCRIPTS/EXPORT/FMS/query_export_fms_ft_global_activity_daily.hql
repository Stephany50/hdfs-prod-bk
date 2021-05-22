select date_format(transaction_date,'dd/MM/yyyy'),
    commercial_offer_code,
    transaction_type,
    sub_account,
    transaction_sign,
    source_platform,
    source_data,
    served_service,
    service_code,
    destination,
    other_party_zone,
    measurement_unit,
    sum(rated_count),
    cast(sum(rated_volume) as decimal(19,2)),
    cast(sum(taxed_amount) as decimal(19,2)),
    cast(sum(untaxed_amount) as decimal(19,2)),
    traffic_mean,
    operator_code
FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY
WHERE TRANSACTION_DATE ='###SLICE_VALUE###'
group by transaction_date,
          commercial_offer_code,
          transaction_type,
          sub_account,
          transaction_sign,
          source_platform,
          source_data,
          served_service,
          service_code,
          destination,
          other_party_zone,
          measurement_unit,
          traffic_mean,
          operator_code