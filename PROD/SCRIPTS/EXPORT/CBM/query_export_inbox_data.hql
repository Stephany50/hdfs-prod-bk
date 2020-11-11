SELECT
    event_date
    , msisdn
    , bytes_used_in_bundle
    , bytes_used_out_bundle
    , (MAIN_RATED_AMOUNT  - ( GOS_DEBIT_AMOUNT + GOS_REFUND_AMOUNT +  GOS_SESSION_AMOUNT)) main_rated_amount_pyg
    , (GOS_DEBIT_AMOUNT + GOS_REFUND_AMOUNT +  GOS_SESSION_AMOUNT) main_rated_amount_gos_sva
FROM MON.SPARK_FT_DATA_CONSO_MSISDN_DAY
WHERE EVENT_DATE = '###SLICE_VALUE###'
