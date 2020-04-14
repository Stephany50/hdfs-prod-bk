
-- insert info voix


INSERT INTO TMP.SPARK_FT_REVENU_LOCALISE_1

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
                    SELECT distinct TRANSACTION_DATE AS EVENT_DATE,
                        upper(SITE_CODE) SITE_CODE,
                        upper(SITE_NAME) SITE_NAME,
                        upper(OFFER_PROFILE_CODE) OFFER_PROFILE_CODE,
                        upper(CASE
                            WHEN DESTINATION = 'Orange' THEN 'OnNet'
                            WHEN DESTINATION IN ('MTN', 'Camtel', 'NEXTTEL')  THEN 'OffNet'
                            WHEN DESTINATION = 'International' THEN 'International'
                        END) AS DESTINATION_TYPE,
                        upper(DESTINATION) DESTINATION,
                        SUM(RATED_TEL_TOTAL_COUNT) AS RATED_TEL_TOTAL_COUNT, SUM(RATED_SMS_TOTAL_COUNT) AS RATED_SMS_TOTAL_COUNT,
                        SUM(RATED_DURATION) AS RATED_DURATION, SUM(VOICE_MAIN_RATED_AMOUNT) AS VOICE_MAIN_RATED_AMOUNT,
                        SUM(VOICE_PROMO_RATED_AMOUNT) AS VOICE_PROMO_RATED_AMOUNT, SUM(SMS_MAIN_RATED_AMOUNT) AS SMS_MAIN_RATED_AMOUNT,
                        SUM(SMS_PROMO_RATED_AMOUNT) AS SMS_PROMO_RATED_AMOUNT
                        FROM
                        (
                            select   transaction_date
                               , SITE_CODE
                               ,nvl(vdci.SITE_NAME, LPAD(fn_hex2deci(upper(NSL_CI)),5,0))  AS site_name
                               ,CASE WHEN  b.dest_short='Orange' THEN 'Orange'
                                     WHEN  b.dest_short='MTN'   THEN 'MTN'
                                     WHEN  b.dest_short='International' THEN 'International'
                                     WHEN  b.dest_short='SVA' THEN 'SVA'
                                     WHEN  b.dest_short LIKE '%MVNO%' THEN 'Orange'
                                     WHEN  b.dest_short = 'CTPhone' THEN 'Camtel'
                                     WHEN  b.dest_short LIKE '%Roam%' THEN 'roaming'
                                     WHEN  b.dest_short = 'NEXTTEL' THEN 'NEXTTEL'
                                     ELSE 'AUTRES' END destination
                               , OFFER_PROFILE_CODE
                               ,sum(CASE WHEN service_code='VOI_VOX' THEN rated_total_count ELSE 0 end) RATED_TEL_TOTAL_COUNT
                               ,sum(CASE WHEN service_code='NVX_SMS' THEN rated_total_count ELSE 0 end) RATED_SMS_TOTAL_COUNT
                               ,sum(CASE WHEN service_code='VOI_VOX' THEN rated_duration ELSE 0 END ) RATED_DURATION
                               ,sum(CASE WHEN service_code='VOI_VOX' THEN promo_rated_amount ELSE 0 end) VOICE_PROMO_RATED_AMOUNT
                               ,sum(CASE WHEN service_code='NVX_SMS' THEN main_rated_amount ELSE 0 end) SMS_MAIN_RATED_AMOUNT
                               ,sum(CASE WHEN service_code='VOI_VOX' THEN main_rated_amount ELSE 0 end) VOICE_MAIN_RATED_AMOUNT
                               ,sum(CASE WHEN service_code='NVX_SMS' THEN promo_rated_amount ELSE 0 end) SMS_PROMO_RATED_AMOUNT
                            from MON.SPARK_FT_GSM_LOCATION_REVENUE_DAILY,
                                ( select * from dim.dt_gsm_cell_code where technologie in ('2G', '3G')) vdci, --VW_CI_LAC_SITE_AMN
                                (select dest_id, dest_short from dim.dt_destinations ) b
                            where TRANSACTION_DATE = TO_DATE(s_slice_value, 'yyyymmdd') --'11/09/2019'  -- '01/06/2019'   --TO_DATE(s_slice_value, 'yyyymmdd')
                                AND LPAD(fn_hex2deci(upper(NSL_CI)),5,0) =vdci.CI(+)
                                AND b.dest_id(+)=destination
                            group BY transaction_date
                                    , SITE_CODE
                                   ,nvl(vdci.SITE_NAME, LPAD(fn_hex2deci(upper(NSL_CI)),5,0))   --vdci.SITE_NAME
                                   ,CASE WHEN  b.dest_short='Orange' THEN 'Orange'
                                         WHEN  b.dest_short='MTN'   THEN 'MTN'
                                         WHEN  b.dest_short='International' THEN 'International'
                                         WHEN  b.dest_short='SVA' THEN 'SVA'
                                         WHEN  b.dest_short LIKE '%MVNO%' THEN 'Orange'
                                         WHEN  b.dest_short = 'CTPhone' THEN 'Camtel'
                                         WHEN  b.dest_short LIKE '%Roam%' THEN 'roaming'
                                         WHEN  b.dest_short = 'NEXTTEL' THEN 'NEXTTEL'
                                         ELSE 'AUTRES' END
                                   , OFFER_PROFILE_CODE
                        )
                        WHERE DESTINATION IN ('Orange', 'International', 'MTN', 'Camtel', 'NEXTTEL')
                        GROUP BY TRANSACTION_DATE,
                            SITE_CODE,
                            SITE_NAME,
                            DESTINATION,
                            OFFER_PROFILE_CODE,
                            CASE
                                WHEN DESTINATION = 'Orange' THEN 'OnNet'
                                WHEN DESTINATION IN ('MTN', 'Camtel', 'NEXTTEL') THEN 'OffNet'
                                WHEN DESTINATION = 'International' THEN 'International'
                            END
                ) b
                on ( a.EVENT_DATE = b.event_date
                    and upper(a.SITE_CODE) = upper(b.SITE_CODE)
                    and upper(a.SITE_NAME) = upper(b.SITE_NAME)
                    and upper(a.offer_profile_code) = upper(b.offer_profile_code)
                    and upper(a.destination_type) = upper(b.destination_type)
                    and upper(a.destination) = upper(b.destination)
                    )
                when matched then
                    update set a.rated_tel_total_count = b.rated_tel_total_count
                            , a.rated_sms_total_count = b.rated_sms_total_count
                            , a.rated_duration = b.rated_duration
                            , a.voice_main_rated_amount = b.voice_main_rated_amount
                            , a.voice_promo_rated_amount = b.voice_promo_rated_amount
                            , a.sms_main_rated_amount = b.sms_main_rated_amount
                            , a.src_table = a.src_table||'VOIX|'
                when not matched then
                    insert (a.EVENT_DATE, a.SITE_CODE, a.SITE_NAME, a.offer_profile_code, a.destination_type, a.destination
                            , a.rated_tel_total_count, a.rated_sms_total_count, a.rated_duration, a.voice_main_rated_amount, a.voice_promo_rated_amount, a.sms_main_rated_amount
                            , a.src_table)
                        values(b.EVENT_DATE, b.SITE_CODE, b.SITE_NAME, b.offer_profile_code, b.destination_type, b.destination
                            , b.rated_tel_total_count, b.rated_sms_total_count, b.rated_duration, b.voice_main_rated_amount, b.voice_promo_rated_amount, b.sms_main_rated_amount
                            , 'VOIX|');


