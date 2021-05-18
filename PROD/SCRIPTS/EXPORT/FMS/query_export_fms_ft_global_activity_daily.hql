select
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
    rated_count,
    rated_volume,
    taxed_amount,
    untaxed_amount,
    traffic_mean,
    operator_code,
    location_ci,
    transaction_date
FROM AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY
WHERE TRANSACTION_DATE ='###SLICE_VALUE###'