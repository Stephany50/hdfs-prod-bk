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
        transaction_date EVENT_DATE, vdci.CI CI,
        SUM(amount_data) AS rated_data,
        sum(nvl(amount_voice_onnet, 0) + nvl(amount_voice_offnet, 0) + nvl(amount_voice_inter, 0) + nvl(amount_voice_roaming, 0)+
        nvl(amount_sms_onnet, 0)+nvl(amount_sms_offnet, 0) + nvl(amount_sms_inter, 0) + nvl(amount_sms_roaming, 0)) as rated_voice_sms,
        sum(case when amount_data > 0 then 1 end) AS RATED_data_COUNt,
        sum(case when nvl(amount_voice_onnet, 0) + nvl(amount_voice_offnet, 0) + nvl(amount_voice_inter, 0) + nvl(amount_voice_roaming, 0)+
        nvl(amount_sms_onnet, 0)+nvl(amount_sms_offnet, 0) + nvl(amount_sms_inter, 0) + nvl(amount_sms_roaming, 0) > 0 then 1 end) RATED_voice_sms_COUNt
    FROM 
    (
        select *
        from MON.SPARK_FT_SUBSCRIPTION a
        WHERE transaction_date = '###SLICE_VALUE###' 
    ) T
    left join 
    (
        SELECT *
        FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
        WHERE EVENT_DATE = '###SLICE_VALUE###'
    ) U
    ON T.SERVED_PARTY_MSISDN = U.MSISDN
    left join dim.dt_ci_lac_site_nuran vdci 
    on LPAD(U.location_ci, 5, 0) = lpad(vdci.CI, 5, 0) 
    where vdci.CI is not null
    GROUP BY transaction_date, vdci.CI
) Y