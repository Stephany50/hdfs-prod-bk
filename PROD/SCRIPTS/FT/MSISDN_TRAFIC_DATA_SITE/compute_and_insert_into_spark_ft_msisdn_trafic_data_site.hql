insert into MON.SPARK_FT_MSISDN_TRAFIC_DATA_SITE
SELECT
    nvl(D.msisdn, S.msisdn) msisdn,
    nvl(D.imei, S.imei) imei,
    nvl(D.Site_name, S.site_name) site_name,
    duree_tel_Entrant,
    Duree_tel_sortant,
    Nbre_tel_entrant,
    nbre_tel_Sortant,
    nbre_sms_entrant,
    nbre_sms_sortant,
    bytes_used,
    total_cost payasyougo,
    bundle_volume_used,
    nvl(d.source, 'Voix/SMS') source,
    current_timestamp insert_date,
    Otarie_Bytes_2G,
    Otarie_Bytes_3G,
    Otarie_Bytes_4G,
    Otarie_Bytes_Ukn,
    nvl(D.session_date, S.event_date) event_date
FROM (
    SELECT
        nvl(a.session_date, transaction_date) session_date,
        nvl(a.msisdn, Src.msisdn) msisdn,
        imei,
        site_name,
        bytes_used,
        total_cost,
        bundle_volume_used,
        Src.Otarie_Bytes_2G,
        Src.Otarie_Bytes_3G,
        Src.Otarie_Bytes_4G,
        Src.Otarie_Bytes_Ukn,
        'DATA' Source
    FROM tmp.TT_MSISDN_DATA_SITE a
    FULL OUTER JOIN
    (
        SELECT
            TRANSACTION_DATE,
            MSISDN,
            sum(case when RADIO_ACCESS_TECHNO = '2G' then Nbytest else 0 end) Otarie_BYTES_2G,
            sum(case when RADIO_ACCESS_TECHNO in ('3G', 'HSPA') then Nbytest else 0 end) Otarie_Bytes_3G,
            sum(case when RADIO_ACCESS_TECHNO = 'LTE' then Nbytest else 0 end) Otarie_Bytes_4G,
            sum(case when RADIO_ACCESS_TECHNO = 'Unknown' then Nbytest else 0 end) Otarie_Bytes_Ukn
        FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
        WHERE transaction_date="###SLICE_VALUE###"
        GROUP BY
            TRANSACTION_DATE,
            MSISDN
    ) Src
    ON
       a.msisdn = Src.msisdn and
       src.transaction_date=a.session_date
    WHERE a.session_date="###SLICE_VALUE###"
) D
   FULL OUTER JOIN
   (
    SELECT
        event_date,
        a.msisdn,
        imei,
        site_name,
        duree_sortant duree_tel_sortant,
        duree_entrant duree_tel_entrant,
        nbre_tel_sortant,
        nbre_tel_entrant,
        nbre_sms_sortant,
        nbre_sms_entrant
    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY a
    JOIN
    (
       SELECT
          sdate,
          msisdn,
          imei,
          row_number() over(partition by sdate, msisdn order by transaction_count desc) rang
        FROM MON.SPARK_FT_IMEI_ONLINE
        WHERE sdate="###SLICE_VALUE###"
    ) b
    ON rang = 1 and
       a.msisdn =b.msisdn and
       a.event_date= b.sdate
    WHERE a.event_date="###SLICE_VALUE###"
)  S
ON D.msisdn = S.msisdn and
   D.session_date= S.event_date
