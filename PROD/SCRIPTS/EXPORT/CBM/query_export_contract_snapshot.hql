SELECT
    REPLACE(EVENT_DATE, ';', '-') event_date,
    REPLACE(ACCESS_KEY, ';', '-') access_key,
    REPLACE(OSP_STATUS, ';', '-') osp_status,
    REPLACE(ACTIVATION_DATE, ';', '-') activation_date,
    REPLACE(DEACTIVATION_DATE, ';', '-') deactivation_date,
    REPLACE(PROFILE, ';', '-') profile,
    REPLACE(LANG, ';', '-') lang,
    REPLACE(MAIN_CREDIT, ';', '-') main_credit,
    REPLACE(PROMO_CREDIT, ';', '-') promo_credit,
    REPLACE(OSP_CONTRACT_TYPE, ';', '-') osp_contract_type,
    REPLACE(CURRENT_STATUS_1, ';', '-') current_status_1,
    REPLACE(DATE_FORMAT(STATE_DATETIME, 'yyyy-MM-dd HH:mm:ss'), ';', '-') state_datetime
FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
WHERE EVENT_DATE = "###SLICE_VALUE###"