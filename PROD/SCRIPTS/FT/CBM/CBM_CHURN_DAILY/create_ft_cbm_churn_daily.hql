CREATE TABLE MON.FT_CBM_CHURN_DAILY
(
  MSISDN                      VARCHAR(25),
  LOST_TYPE                   VARCHAR(10),
  SITE                        VARCHAR(50),
  VILLE                       VARCHAR(50),
  REGION                      VARCHAR(50),
  DATE_DERNIERE_LOCALISATION  TIMESTAMP,
  ACTIVATION_DATE             TIMESTAMP,
  INSERT_DATE                 TIMESTAMP
)
COMMENT 'FT_CBM_CHURN_DAILY - FT'
  PARTITIONED BY (EVENT_DATE    DATE)
  STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
