SELECT
    REPLACE(EVENT_DATE, ';', '-') event_date,
    REPLACE(ACCESS_KEY, ';', '-') access_key,
    REPLACE(OSP_STATUS, ';', '-') osp_status,
    REPLACE(ACTIVATION_DATE, ';', '-') activation_date,
    REPLACE(DEACTIVATION_DATE, ';', '-') deactivation_date,
    REPLACE(PROFILE, ';', '-') profile,
    (CASE WHEN UPPER(TRIM(administrative_region)) IN ('NORD-OUEST', 'SUD-OUEST') THEN 'EN' ELSE REPLACE(LANG, ';', '-') END) lang,
    REPLACE(MAIN_CREDIT, ';', '-') main_credit,
    REPLACE(PROMO_CREDIT, ';', '-') promo_credit,
    REPLACE(OSP_CONTRACT_TYPE, ';', '-') osp_contract_type,
    REPLACE(CURRENT_STATUS_1, ';', '-') current_status_1,
    REPLACE(DATE_FORMAT(STATE_DATETIME, 'yyyy-MM-dd HH:mm:ss'), ';', '-') state_datetime,
    REPLACE(LANG, ';', '-') in_lang
FROM 
(select * from MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = "###SLICE_VALUE###") A
left join
(select msisdn, administrative_region FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY where event_date = "###SLICE_VALUE###") B
on A.access_key = B.msisdn