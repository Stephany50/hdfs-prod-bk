INSERT  INTO TMP.TT_FT_MSC_IMEI_ONLINE  D
SELECT
    SERVED_IMEI IMEI,
    SERVED_IMSI IMSI,
    SERVED_MSISDN MSISDN,
    SUM(1) NBRE,
    , 'MSC|' SRC_TABLE
    , CURRENT_TIMESTAMP INSERT_DATE
    TRANSACTION_DATE SDATE
FROM MON.SPARK_FT_MSC_TRANSACTION
WHERE
      TRANSACTION_DATE = '###SLICE_VALUE###'
      AND SERVED_IMEI IS NOT NULL
GROUP BY
    TRANSACTION_DATE,
    SERVED_IMEI,
    SERVED_IMSI,
    SERVED_MSISDN