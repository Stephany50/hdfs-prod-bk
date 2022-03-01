INSERT INTO AGG.SPARK_FT_A_REVENU_SITE_NURAN
select
    CI,  
    servicecode AS TYPE, 
    '' AS DESTINATION,  'NULL' AS NB_APPELS,
    case when servicecode = 'SOUSCRIPTION_DATA' then RATED_data_COUNt 
    else RATED_voice_sms_COUNt end AS VOLUME_TOTAL,    
    case when servicecode = 'SOUSCRIPTION_DATA' then rated_data/1000 
    else rated_voice_sms/1000 end AS REVENU_MAIN_KFCFA,    
    'NULL' AS REVENU_PROMO_KFCFA,
    CURRENT_TIMESTAMP() AS INSERT_DATE, 'PREPAID' AS CONTRACT_TYPE , EVENT_DATE
from
(
    select 'SOUSCRIPTION_DATA' as servicecode
    union
    select 'SOUSCRIPTION_VOIX_SMS' as servicecode
) R
cross join
(
    SELECT
        event_date,
        vdci.CI CI,
        rated_data,
        rated_voice_sms,
        RATED_data_COUNt,
        RATED_voice_sms_COUNt
    FROM
    (
        SELECT 
            transaction_date EVENT_DATE, location_ci,
            SUM(DATA_COMBO) + SUM(DATA_PUR) AS rated_data,
            -- sum(nvl(amount_voice_onnet, 0) + nvl(amount_voice_offnet, 0) + nvl(amount_voice_inter, 0) + nvl(amount_voice_roaming, 0)+
            -- nvl(amount_sms_onnet, 0)+nvl(amount_sms_offnet, 0) + nvl(amount_sms_inter, 0) + nvl(amount_sms_roaming, 0)) as rated_voice_sms,
            sum(VOIX_COMBO) + sum(VOIX_PUR) + sum(SMS_COMBO) + sum(SMS_PUR) rated_voice_sms,
            sum(case when nvl(DATA_COMBO, 0) + nvl(DATA_PUR, 0) > 0 then 1 end) AS RATED_data_COUNt,
            -- sum(case when nvl(amount_voice_onnet, 0) + nvl(amount_voice_offnet, 0) + nvl(amount_voice_inter, 0) + nvl(amount_voice_roaming, 0)+
            -- nvl(amount_sms_onnet, 0)+nvl(amount_sms_offnet, 0) + nvl(amount_sms_inter, 0) + nvl(amount_sms_roaming, 0) > 0 then 1 end) RATED_voice_sms_COUNt
            sum(case when nvl(VOIX_COMBO, 0) + nvl(VOIX_PUR, 0) + nvl(SMS_COMBO, 0) + nvl(SMS_PUR, 0) > 0 then 1 else 0 end )  RATED_voice_sms_COUNt
        FROM 
        (
            SELECT
                MSISDN SERVED_PARTY_MSISDN
                , PERIOD transaction_date
                , (
                    CASE WHEN (NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0)) < 100
                    THEN BDLE_COST*(NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0))/100
                    ELSE 0 END
                ) DATA_COMBO
                , (
                    CASE WHEN (NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0)) = 100
                    THEN BDLE_COST*(NVL(COEFF_DATA, 0) + NVL(COEFF_ROAMING_DATA, 0))/100
                    ELSE 0 END
                ) DATA_PUR
                , (
                    CASE WHEN (NVL(COEFF_ONNET, 0)+NVL(COEFF_OFFNET, 0)+NVL(COEFF_INTER, 0)+NVL(COEFF_ROAMING_VOIX, 0)) < 100
                    THEN BDLE_COST*(NVL(COEFF_ONNET, 0) + NVL(COEFF_OFFNET, 0) + NVL(COEFF_INTER, 0) + NVL(COEFF_ROAMING_VOIX, 0))/100
                    ELSE 0 END
                ) VOIX_COMBO
                , (
                    CASE WHEN (NVL(COEFF_ONNET, 0)+NVL(COEFF_OFFNET, 0)+NVL(COEFF_INTER, 0)+NVL(COEFF_ROAMING_VOIX, 0)) = 100
                    THEN BDLE_COST*(NVL(COEFF_ONNET, 0) + NVL(COEFF_OFFNET, 0) + NVL(COEFF_INTER, 0) + NVL(COEFF_ROAMING_VOIX, 0))/100
                    ELSE 0 END
                ) VOIX_PUR
                , (
                    CASE WHEN (NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0)) < 100
                    THEN BDLE_COST*(NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0))/100
                    ELSE 0 END
                ) SMS_COMBO
                , (
                    CASE WHEN (NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0)) = 100
                    THEN BDLE_COST*(NVL(COEF_SMS, 0) + NVL(COEFF_ROAMING_SMS, 0))/100
                    ELSE 0 END
                ) SMS_PUR
            FROM
            (
                SELECT
                    MSISDN,
                    BDLE_COST,
                    BDLE_NAME,
                    PERIOD
                FROM MON.SPARK_FT_CBM_BUNDLE_SUBS_DAILY
                WHERE PERIOD = '###SLICE_VALUE###'
            ) U00
            LEFT JOIN DIM.DT_CBM_REF_SOUSCRIPTION_PRICE U01
            ON UPPER(TRIM(U00.BDLE_NAME))= UPPER(TRIM(U01.BDLE_NAME))
        ) T
        left join 
        (
            SELECT
                nvl(F10.MSISDN, F11.MSISDN) MSISDN,
                UPPER(NVL(F11.location_ci, F10.location_ci)) location_ci
            FROM
            (
                SELECT
                    MSISDN,
                    MAX(location_ci) location_ci
                FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
                WHERE EVENT_DATE = '###SLICE_VALUE###'
                GROUP BY MSISDN
            ) F10
            FULL JOIN
            (
                SELECT
                    MSISDN,
                    MAX(location_ci) location_ci
                FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
                WHERE EVENT_DATE = '###SLICE_VALUE###'
                GROUP BY MSISDN
            ) F11
            ON F10.MSISDN = F11.MSISDN
        ) U
        ON T.SERVED_PARTY_MSISDN = U.MSISDN 
        where location_ci is not null
        GROUP BY transaction_date, location_ci
    ) G right join dim.dt_ci_lac_site_nuran vdci 
    -- on upper(trim((G.SITE_NAME))) = upper(trim(vdci.SITE_NAME))
    on LPAD(G.location_ci, 5, 0) = lpad(vdci.CI, 5, 0) 
) Y