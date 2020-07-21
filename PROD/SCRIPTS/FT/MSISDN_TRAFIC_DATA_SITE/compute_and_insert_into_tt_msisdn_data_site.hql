insert into tmp.TT_MSISDN_DATA_SITE
SELECT
    distinct session_date,
    msisdn,
    first_value(imei) over (partition by session_date, msisdn order by bytes_used desc) imei,
    first_value(site_name)over(partition by session_date, msisdn order by bytes_used desc) site_name,
    sum(Bytes_used)over(partition by session_date, msisdn) Bytes_Used,
    sum(total_cost)over(partition by session_date, msisdn) total_cost,
    sum(bundle_volume_used)over(partition by session_date,
    msisdn) bundle_volume_used,
    current_timestamp insert_date
FROM
(
    SELECT
       session_date,
       served_party_msisdn msisdn,
       substr(served_party_imei, 1, 14) imei,
       site_name,
       sum(bytes_sent+bytes_Received) Bytes_Used,
       sum(total_cost)total_cost,
       sum(main_cost) Main_cost,
       sum(promo_cost) Promo_cost,
       sum(bundle_bytes_used_volume) bundle_Volume_used,
       sum(total_count) transaction_count,
       sum(rated_count) rated_count
    FROM MON.SPARK_FT_MSISDN_IMEI_DATA_LOCATION a
    left join
    (
        SELECT
            ci,
            site_name
        FROM default.spark_VW_SDT_CI_INFO_new
    ) b
    ON location_ci = ci      --dim.dt_gsm_cell_code
    WHERE session_date="###SLICE_VALUE###"
    GROUP BY
        session_date,
        served_party_msisdn,
        substr(served_party_imei, 1, 14),
        site_name
) a




