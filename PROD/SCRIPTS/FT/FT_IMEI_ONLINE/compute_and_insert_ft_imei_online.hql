insert into MON.FT_IMEI_ONLINE PARTITION(SDATE)
SELECT
    IMEI,
    IMSI,
    MSISDN ,
    SRC_TABLE,
    TRANSACTION_COUNT,
    INSERT_DATE,
    SDATE
    FROM TMP.TT_FT_IMEI_ONLINE

