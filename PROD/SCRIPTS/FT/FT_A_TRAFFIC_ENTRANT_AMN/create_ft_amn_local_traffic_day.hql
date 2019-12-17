CREATE TABLE MON.ft_amn_local_traffic_day
(

  SITE_NAME    VARCHAR2(100),
  DURATION     NUMBER,
  SMS_COUNT    NUMBER,
  INSERT_DATE  DATE
) COMMENT 'FT_AMN_LOCAL_TRAFFIC_DAY table'
PARTITIONED BY (EVENT_DATE   DATE)
TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")