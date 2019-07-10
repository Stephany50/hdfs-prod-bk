CREATE TABLE MON.TT_FT_CBM_DOLA_DAILY(
 EVENT_DATE       DATE,
 MSISDN           VARCHAR(40),
 ACTIVATION_DATE  DATE,
 DOLA             DATE,
 DOLIA            DATE,
 ACTIVITY_TYPE    VARCHAR(80),
 RGSX             VARCHAR(80),
 REGION           VARCHAR(80),
 TOWN             VARCHAR(80),
 QUARTER          VARCHAR(80),
 INSERT_DATE      TIMESTAMP
)COMMENT 'MON.TT_FT_CBM_DOLA_DAILY Table'
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
    