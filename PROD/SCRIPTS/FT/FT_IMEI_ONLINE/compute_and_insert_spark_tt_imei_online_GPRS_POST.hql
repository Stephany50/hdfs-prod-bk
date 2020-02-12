INSERT INTO TMP.TT_FT_CRA_GPRS_POST_IMEI_ONLINE
SELECT
      SERVED_PARTY_IMEI IMEI
    , SERVED_PARTY_IMSI IMSI
    , SERVED_PARTY_MSISDN MSISDN
    , 'GPRS_POST|' SRC_TABLE
    , SUM(1) nbre
    , CURRENT_TIMESTAMP INSERT_DATE
    , SESSION_DATE SDATE
FROM MON.SPARK_FT_CRA_GPRS_POST
WHERE
      SESSION_DATE = '###SLICE_VALUE###'
      AND SERVED_PARTY_IMEI IS NOT NULL

GROUP BY SESSION_DATE
    , SERVED_PARTY_IMEI
    , SERVED_PARTY_IMSI
    , SERVED_PARTY_MSISDN