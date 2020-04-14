
-- insert info data


INSERT INTO MON.SPARK_FT_REVENU_LOCALISE

SELECT

    nvl(a.IMEI, b.IMEI) IMEI,
    nvl(a.MSISDN, b.MSISDN) MSISDN,
    a.SITE_VOIX SITE_VOIX,
    a.SITE_DATA SITE_DATA,
    a.TECHNOLOGIE TECHNOLOGIE,
    a.TERMINAL_TYPE TERMINAL_TYPE,
    a.MSISDN_COUNT MSISDN_COUNT,
    a.NOMBRE_TRANSACTIONS_ENTRANT NOMBRE_TRANSACTIONS_ENTRANT,
    a.NOMBRE_TRANSACTIONS_SORTANT NOMBRE_TRANSACTIONS_SORTANT,
    a.DUREE_ENTRANT DUREE_ENTRANT,
    a.DUREE_SORTANT DUREE_SORTANT,
    a.VOLUME_DATA_GPRS VOLUME_DATA_GPRS,
    a.VOLUME_DATA_GPRS_2G VOLUME_DATA_GPRS_2G,
    a.VOLUME_DATA_GPRS_3G VOLUME_DATA_GPRS_3G,
    a.VOLUME_DATA_GPRS_4G VOLUME_DATA_GPRS_4G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE, b.VOLUME_DATA), b.VOLUME_DATA) VOLUME_DATA_OTARIE,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE_2G, b.VOLUME_DATA_OTARIE_2G), b.VOLUME_DATA_OTARIE_2G) VOLUME_DATA_OTARIE_2G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE_3G, b.VOLUME_DATA_OTARIE_3G), b.VOLUME_DATA_OTARIE_3G) VOLUME_DATA_OTARIE_3G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, nvl(a.VOLUME_DATA_OTARIE_4G, b.VOLUME_DATA_OTARIE_4G), b.VOLUME_DATA_OTARIE_4G) VOLUME_DATA_OTARIE_4G,
    IF(a.msisdn IS NULL OR b.msisdn IS NULL OR a.event_date IS NULL OR b.event_date IS NULL OR substr(a.imei, 1, 8) IS NULL OR substr(b.imei, 1, 8) IS NULL, 'OTARIE|', a.src_table||'OTARIE|') src_table,
    CURRENT_TIMESTAMP INSERT_DATE,
    nvl(a.EVENT_DATE, b.EVENT_DATE) EVENT_DATE

FROM
MON.SPARK_FT_IMEI_TRANSACTION  A
FULL OUTER JOIN
(
    select transaction_date EVENT_DATE ,MSISDN ,IMEI
    ,sum(case when RADIO_ACCESS_TECHNO in ('2G', 'Unknown') then NBYTEST else 0 end) VOLUME_DATA_OTARIE_2G
    ,sum(case when RADIO_ACCESS_TECHNO in ('3G', 'HSPA') then NBYTEST else 0 end) VOLUME_DATA_OTARIE_3G
    ,sum(case when RADIO_ACCESS_TECHNO in ('4G', 'LTE') then NBYTEST else 0 end) VOLUME_DATA_OTARIE_4G
    ,sum(NBYTEST) VOLUME_DATA
    from MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
    where transaction_date = '###SLICE_VALUE###'
    group by transaction_date, MSISDN, IMEI
) B

ON (  A.MSISDN = B.MSISDN AND A.EVENT_DATE = B.EVENT_DATE  AND SUBSTR(A.IMEI, 1, 8) = SUBSTR(B.IMEI, 1, 8))









MERGE INTO FT_REVENU_LOCALISE a
                USING
                (
                    select distinct event_date, upper(a.site_code) site_code, upper(a.site_name) site_name, upper(profile_code) profile_code, upper(Destination_type) Destination_type, upper(Destination) Destination, MBytes_used, (Mbytes_used/Mbytes_all)*AMOUNT_DATA AMOUNT_DATA
                    from
                    (
                        select event_date, upper(site_code) site_code, upper(site_name) site_name, upper(PROFILE_CODE) PROFILE_CODE
                        , 'OnNet' Destination_type, 'Orange' Destination,  sum(BYTES_SENT+BYTES_RECEIVED)/(1024*1024) MBytes_used--, sum(Main_cost) Main_cost
                        from FT_GPRS_SITE_REVENU_DAILY
                        where event_date = TO_DATE(s_slice_value, 'yyyymmdd')   --'03/10/2019'
                        group by event_date, upper(site_code), upper(site_name), upper(profile_code)
                    ) a
                    ,
                    (
                        select distinct site_code, site_name, (sum(BYTES_SENT+BYTES_RECEIVED)over(partition by event_date))/(1024*1024) Mbytes_All
                        from FT_GPRS_SITE_REVENU_DAILY
                        where event_date = TO_DATE(s_slice_value, 'yyyymmdd')   -- '03/10/2019'
                    ) b
                    ,
                    (
                        select  transaction_date, sum(AMOUNT_DATA) AMOUNT_DATA
                        from AGG.SPARK_FT_A_SUBSCRIPTION  
                        where transaction_date = TO_DATE(s_slice_value, 'yyyymmdd') --'03/10/2019'
                        group by transaction_date
                    )
                    where a.site_name = b.site_name(+)
                         and a.event_date = transaction_date(+)
                ) b
                on (a.event_date = b.event_date
                    and upper(a.site_code) = upper(b.site_code)
                    and upper(a.site_name) = upper(b.site_name)
                    and upper(a.OFFER_PROFILE_CODE) = upper(b.profile_code)
                    and upper(a.Destination_type) = upper(b.Destination_type)
                    and upper(a.Destination) = upper(b.Destination)
                    )
                when matched then
                    update set revenu_data = amount_data
                            , a.MBYTES_USED = b.MBytes_used
                            , a.src_table = a.src_table||'DATA|'
                when not matched then
                    insert (a.event_date, a.site_name, a.OFFER_PROFILE_CODE, a.destination_type, a.destination, a.REVENU_DATA, a.MBYTES_USED, a.SRC_TABLE)
                    values (b.event_date, b.site_name, b.profile_code, b.destination_type, b.destination, b.amount_data, b.MBYTES_USED, 'DATA|')  ;
