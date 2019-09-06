CREATE TABLE TMP.TT_CLIENT_LAST_SITE_DAY (
    MSISDN VARCHAR(25),
    SITE_NAME VARCHAR(50),
    TOWNNAME VARCHAR(50),
    ADMINISTRATIVE_REGION VARCHAR(50),
   COMMERCIAL_REGION VARCHAR(50),
    LAST_LOCATION_DAY TIMESTAMP,
    OPERATOR_CODE VARCHAR(20),
    INSERT_DATE TIMESTAMP,
    EVENT_DATE DATE
)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;