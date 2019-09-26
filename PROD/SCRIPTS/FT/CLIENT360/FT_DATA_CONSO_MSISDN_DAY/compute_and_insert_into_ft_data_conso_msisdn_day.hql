-- @TRAITEMENT :: RECUPERER LES DONNÉES  ET  INSERER LES DONNÉES AGREGÉES <mon.FT_DATA_CONSO_MSISDN_DAY>
-- EVENT_DATE, MSISDN, FORMULE, CONSO, SMS_COUNT, TEL_COUNT, TEL_DURATION, BILLED_SMS_COUNT, BILLED_TEL_COUNT, BILLED_TEL_DURATION
INSERT INTO mon.FT_DATA_CONSO_MSISDN_DAY
(MSISDN,    COMMERCIAL_OFFER,    BYTES_SENT,    BYTES_RECEIVED, MMS_COUNT,    MAIN_RATED_AMOUNT,    PROMO_RATED_AMOUNT,    SERVED_PARTY_IMSI,    SERVED_PARTY_IMEI
,    BYTES_USED_IN_BUNDLE,    BYTES_USED_OUT_BUNDLE,    BYTES_USED_IN_BUNDLE_ROAMING,    BYTES_USED_OUT_BUNDLE_ROAMING,    BUNDLE_MMS_USED_VOLUME--,    BYTES_USED_OUT_BUNDLE_LOCAL
,    MAIN_RATED_AMOUNT_ROAMING,    PROMO_RATED_AMOUNT_ROAMING, GOS_DEBIT_COUNT, GOS_SESSION_COUNT, GOS_REFUND_COUNT, GOS_DEBIT_AMOUNT, GOS_SESSION_AMOUNT, GOS_REFUND_AMOUNT
,    BUNDLE_BYTES_REMAINING_VOLUME, BUNDLE_MMS_REMAINING_VOLUME,    SOURCE_TABLE,    OPERATOR_CODE,    INSERT_DATE,EVENT_DATE)
SELECT
     a.CHARGED_PARTY_MSISDN  MSISDN
    , MAX (a.SERVED_PARTY_OFFER) COMMERCIAL_OFFER
    , SUM(NVL(BYTES_SENT, 0)) BYTES_SENT
    , SUM(NVL(BYTES_RECEIVED, 0)) BYTES_RECEIVED
    , SUM(CASE WHEN NVL(BUNDLE_MMS_USED_VOLUME, 0) > 0 THEN 1 ELSE 0 END) MMS_COUNT
    , SUM(NVL(MAIN_COST, 0)) MAIN_RATED_AMOUNT
    , SUM(NVL(PROMO_COST, 0)) PROMO_RATED_AMOUNT
    , MAX(SERVED_PARTY_IMSI) SERVED_PARTY_IMSI
    , MAX(SERVED_PARTY_IMEI) SERVED_PARTY_IMEI
    , SUM(NVL(BUNDLE_BYTES_USED_VOLUME, 0)) BYTES_USED_IN_BUNDLE
    , SUM(NVL(BYTES_SENT, 0) + NVL(BYTES_RECEIVED, 0) - NVL(BUNDLE_BYTES_USED_VOLUME, 0)) BYTES_USED_OUT_BUNDLE
    , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END)BYTES_USED_IN_BUNDLE_ROAMING
    , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BYTES_SENT, 0) + NVL(BYTES_RECEIVED, 0) - NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END) BYTES_USED_OUT_BUNDLE_ROAMING
    , SUM(NVL(BUNDLE_MMS_USED_VOLUME, 0))BUNDLE_MMS_USED_VOLUME
    --, SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 0 THEN NVL(BYTES_SENT, 0) + NVL(BYTES_RECEIVED, 0) - NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END) BYTES_USED_OUT_BUNDLE_LOCAL
    , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(MAIN_COST, 0) ELSE 0 END)MAIN_RATED_AMOUNT_ROAMING
    , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(PROMO_COST, 0) ELSE 0 END) PROMO_RATED_AMOUNT_ROAMING
    , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%DEBIT%' THEN 1 ELSE 0 END) GOS_DEBIT_COUNT
    , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%SESSION%' THEN 1 ELSE 0 END) GOS_SESSION_COUNT
    , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%REFUND%' THEN 1 ELSE 0 END) GOS_REFUND_COUNT
    , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%DEBIT%' THEN NVL(MAIN_COST, 0) ELSE 0 END) GOS_DEBIT_AMOUNT
    , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%SESSION%' THEN NVL(MAIN_COST, 0) ELSE 0 END) GOS_SESSION_AMOUNT
    , SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%REFUND%' THEN NVL(MAIN_COST, 0) ELSE 0 END) GOS_REFUND_AMOUNT
    , SUM(NVL(BUNDLE_BYTES_REMAINING_VOLUME, 0)) BUNDLE_BYTES_REMAINING_VOLUME
    , SUM(NVL(BUNDLE_MMS_REMAINING_VOLUME, 0)) BUNDLE_MMS_REMAINING_VOLUME
    , 'FT_CRA_GPRS' SRC_TABLE
    , OPERATOR_CODE
    , CURRENT_TIMESTAMP INSERT_DATE
    , to_date(SESSION_DATE) EVENT_DATE
FROM backup_dwh.FT_CRA_GPRS20190923 a
--WHERE SESSION_DATE = '2019-09-23'
GROUP BY
    to_date(SESSION_DATE)
    , a.CHARGED_PARTY_MSISDN
    , 'FT_CRA_GPRS'
    , OPERATOR_CODE
    , CURRENT_TIMESTAMP
