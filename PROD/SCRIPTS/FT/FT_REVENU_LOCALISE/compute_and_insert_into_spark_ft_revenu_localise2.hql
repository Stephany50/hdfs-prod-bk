
-- insert info msc


INSERT INTO TMP.SPARK_FT_REVENU_LOCALISE_2

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
                    select sdate, upper(site_code) site_code, upper(site_name) site_name
                    , case when type_abonne = 'PREP' then 'PREPAID PLENTY'    --Forcer aux valeurs les plus preponderante du fait que granularite moindre inexistante dans AG_INTERCO
                        when type_abonne = 'POST' then  'PREPAID FLEX PLUS'
                        else 'PREPAID PLENTY'
                        end subscriber_type
                    ,( case when trunck_in in ('Zain Tchad', 'Zain Gabon', 'Orange CI', 'BELG','FTLD', 'IBGAP' ) then 'INTERNATIONAL'
                            when trunck_in in ('CAMTEL', 'MTN', 'LMT', 'VIETTEL' ) then 'OFFNET'
                            else 'ONNET'
                       end
                     ) destination_type
                    , upper( CASE
                        WHEN  trunck_in IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_in
                        WHEN  trunck_in = 'CAMTEL' THEN 'Camtel National'
                        WHEN  trunck_in = 'Zain Gabon' THEN  'Zain Gabon'
                        WHEN  trunck_in = 'Zain Tchad' THEN  'Zain Tchad'
                        WHEN  trunck_in = 'Orange CI' THEN  'Orange CI'
                        --
                        WHEN  NVL (trunck_in, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
                        ELSE 'Orange'
                       END ) Destination
                    , sum(CRA_COUNT) NBre_appel
                    , sum(DURATION) Duree
                    from agg.spark_ft_ag_interco
                        ,(select * from dim.dt_gsm_cell_code where technologie in ('2G', '3G')) --Du fait de la présence des doublons en ajoutant les ci de la 4G.
                    where sdate = TO_DATE(s_slice_value, 'yyyymmdd') --'01/08/2019'  --x --'11/08/2019' -- '10/08/2019'
                        --and sdate <= '31/08/2019'
                     and USAGE_APPEL = 'TEL'
                    and substr(msc_location,14,5)=to_char(ci(+))
                    group by sdate, upper(site_code), upper(site_name)
                    , case when type_abonne = 'PREP' then 'PREPAID PLENTY'    --Forcer aux valeurs les plus preponderante du fait que granularite moindre inexistante dans AG_INTERCO
                        when type_abonne = 'POST' then  'PREPAID FLEX PLUS'
                        else 'PREPAID PLENTY'
                        end
                    ,( case when trunck_in in ('Zain Tchad', 'Zain Gabon', 'Orange CI', 'BELG','FTLD', 'IBGAP' ) then 'INTERNATIONAL'
                            when trunck_in in ('CAMTEL', 'MTN', 'LMT', 'VIETTEL' ) then 'OFFNET'
                            else 'ONNET'
                       end
                     )
                    , upper( CASE
                        WHEN  trunck_in IN  ('IBGAP', 'BELG','FTLD','MTN', 'LMT', 'VIETTEL') THEN trunck_in
                        WHEN  trunck_in = 'CAMTEL' THEN 'Camtel National'
                        WHEN  trunck_in = 'Zain Gabon' THEN  'Zain Gabon'
                        WHEN  trunck_in = 'Zain Tchad' THEN  'Zain Tchad'
                        WHEN  trunck_in = 'Orange CI' THEN  'Orange CI'
                        --
                        WHEN  NVL (trunck_in, 'NA') = 'NA' THEN 'NOT_INTERCONNECT'
                        ELSE 'Orange'
                       END )
                ) b
                on ( a.EVENT_DATE = b.sdate
                    and upper(a.SITE_CODE) = upper(b.SITE_CODE)
                    and upper(a.SITE_NAME) = upper(b.SITE_NAME)
                    and upper(a.offer_profile_code) = upper(b.SUBSCRIBER_TYPE)
                    and upper(a.destination_type) = upper(b.DESTINATION_TYPE)
                    and upper(a.destination) = upper(b.destination)
                    )
                when matched then
                    update set a.IN_CALL_COUNT = NBre_appel
                            , a.IN_DURATION = Duree
                            , a.src_table = a.src_table||'MSC|'
                when not matched then
                    insert (a.EVENT_DATE, a.SITE_CODE, a.SITE_NAME, a.offer_profile_code, a.destination_type, a.destination
                            , a.IN_CALL_COUNT, a.IN_DURATION, a.src_table)
                        values(b.SDATE, b.SITE_CODE, b.SITE_NAME, b.SUBSCRIBER_TYPE, b.DESTINATION_TYPE, b.DESTINATION, Nbre_appel, Duree, 'MSC|');


