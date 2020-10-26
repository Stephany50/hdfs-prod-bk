insert INTO TMP.TMP_RECEIVED_NOTIFICATION_SMS
select
    a.*
    , upper(site_name) site_name
from 
(
    select
        lpad(substr(transaction_time, 1, 4), 4, '0') Hour_Min
        , served_msisdn
        , max(substr(served_party_location,14,5)) location_ci
        , max(substr(served_party_location,8,5)) location_lac
    from
    (
        select
            transaction_time
            , transaction_type
            , served_msisdn
            , served_party_location
            , other_party
        from Mon.Spark_Ft_msc_transaction
        where transaction_date = '###SLICE_VALUE###'
            and other_party in ('393337', '393338')
    ) a0
    group by substr(transaction_time, 1, 4), served_msisdn
) a
left join 
(
    select
        ci
        , lac
        , site_name
    from DIM.Spark_dt_gsm_cell_code
) b
on (location_ci = ci and location_lac = lac)
