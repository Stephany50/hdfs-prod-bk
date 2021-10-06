insert into TMP.TMP_RECEIVED_NOTIFICATION_SMS11
select
    a.*
    , upper(site_name) site_name
from
(
    select
        transaction_date
        , lpad(substr(transaction_time, 1, 4), 4, '0') Hour_Min
        , served_msisdn
        , max(substr(served_party_location,14,5)) location_ci
        , max(substr(served_party_location,8,5)) location_lac
    from
    (
        select
            transaction_date
            , transaction_time
            , transaction_type
            , served_msisdn
            , served_party_location
            , other_party
        from Mon.Spark_Ft_msc_transaction
        where transaction_date = '###SLICE_VALUE###'	--'2020-11-24'
            and other_party in ('8917', '8918', '393337', '393338')
    ) a1
    group by transaction_date, lpad(substr(transaction_time, 1, 4), 4, '0'), served_msisdn
) a
left join
(
    select ci, lac, max(site_name) as site_name from DIM.Spark_dt_gsm_cell_code where technologie in ('2G', '3G') group by ci, lac
) b
on (location_ci = ci )

