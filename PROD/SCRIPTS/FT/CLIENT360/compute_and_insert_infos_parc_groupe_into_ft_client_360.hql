SELECT
    NVL(a.MSISDN, b.MSISDN) AS MSISDN,
    CONTRACT_TYPE,
    COMMERCIAL_OFFER,
    ACTIVATION_DATE,
    DEACTIVATION_DATE,
    LANG,
    OSP_STATUS,
    IMSI,
    GRP_LAST_OG_CALL,
    GRP_LAST_IC_CALL,
    GRP_REMAIN_CREDIT_MAIN,
    GRP_REMAIN_CREDIT_PROMO,
    GRP_GP_STATUS
FROM
(
    -- infos generales
    SELECT ACCESS_KEY MSISDN,
        OSP_CONTRACT_TYPE CONTRACT_TYPE,
        COMMERCIAL_OFFER,
        ACTIVATION_DATE,
        DEACTIVATION_DATE,
        LANG,
        OSP_STATUS,
        MAIN_IMSI IMSI
    FROM MON.FT_CONTRACT_SNAPSHOT
    WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') + 1
        AND OSP_STATUS IN ('ACTIVE', 'INACTIVE')
) a
FULL JOIN
(
    -- infos groupe
    SELECT
        MSISDN,
        OG_CALL GRP_LAST_OG_CALL,
        IC_CALL_4 GRP_LAST_IC_CALL,
        REMAIN_CREDIT_MAIN GRP_REMAIN_CREDIT_MAIN,
        REMAIN_CREDIT_PROMO GRP_REMAIN_CREDIT_PROMO,
        GP_STATUS GRP_GP_STATUS
    FROM MON.FT_ACCOUNT_ACTIVITY
    WHERE EVENT_DATE = TO_DATE(s_slice_value, 'yyyymmdd') + 1
) b  ON a.MSISDN = b.MSISDN