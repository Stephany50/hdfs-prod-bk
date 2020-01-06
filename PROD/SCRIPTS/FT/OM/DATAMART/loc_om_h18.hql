
-- LAST SUBSCRIBER'S MOST ACTIVE LOCATION (SITE)
TRUNCATE TABLE MON.LOC_OM_H18;

CREATE TABLE MON.LOC_OM_H18 (
    MSISDN STRING,
    SITE_NAME STRING,
    TOWNNAME STRING,
    ADMINISTRATIVE_REGION STRING,
    COMMERCIAL_REGION STRING,
    LAST_LOCATION_DAY TIMESTAMP
)
STORED AS ORC

INSERT INTO  MON.LOC_OM_H18
SELECT
  a.MSISDN,
  a.SITE_NAME,
  a.TOWNNAME,
  a.ADMINISTRATIVE_REGION,
  a.COMMERCIAL_REGION,
  a.LAST_LOCATION_DAY
FROM (
    SELECT
        a.*,
        ROW_NUMBER() OVER (PARTITION  BY msisdn ORDER BY LAST_LOCATION_DAY DESC, insert_date desc) RN
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY a
    WHERE EVENT_DATE = '2019-11-19'
)a WHERE RN=1