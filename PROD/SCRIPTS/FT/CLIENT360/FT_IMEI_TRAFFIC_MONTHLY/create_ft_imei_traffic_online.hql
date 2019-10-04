create table TMP.FT_IMEI_ONLINE
(
    IMEI              VARCHAR(50),
    IMSI              VARCHAR(50),
    MSISDN            VARCHAR(50),
    SRC_TABLE         VARCHAR(100),
    INSERT_DATE       TIMESTAMP,
    TRANSACTION_COUNT BIGINT
)
PARTITIONED BY (SDATE  DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")

create table backup_dwh.FT_IMEI_ONLINE
(
    SDATE             VARCHAR(100),
    IMEI              VARCHAR(50),
    IMSI              VARCHAR(50),
    MSISDN            VARCHAR(50),
    SRC_TABLE         VARCHAR(100),
    INSERT_DATE       VARCHAR(100),
    TRANSACTION_COUNT VARCHAR(100)
) stored as orc



INSERT INTO TMP.FT_IMEI_ONLINE
SELECT
   IMEI,
    IMSI,
    MSISDN,
    SRC_TABLE,
    INSERT_DATE,
    TRANSACTION_COUNT,
    to_date(SDATE) SDATE
FROM backup_dwh.FT_IMEI_ONLINE
-- auto-generated definition


