INSERT INTO TMP.TT_FT_CRA_GPRS_IMEI_ONLINE
SELECT
     SERVED_PARTY_IMEI IMEI
    , SERVED_PARTY_IMSI IMSI
    , SERVED_PARTY_MSISDN MSISDN
    , SUM(1) nbre
    , 'GPRS|' SRC_TABLE
    , CURRENT_TIMESTAMP INSERT_DATE
    , SESSION_DATE SDATE
FROM MON.SPARK_FT_CRA_GPRS
WHERE
      SESSION_DATE = '###SLICE_VALUE###'
      AND SERVED_PARTY_IMEI IS NOT NULL
GROUP BY SESSION_DATE
    , SERVED_PARTY_IMEI
    , SERVED_PARTY_IMSI
    , SERVED_PARTY_MSISDN