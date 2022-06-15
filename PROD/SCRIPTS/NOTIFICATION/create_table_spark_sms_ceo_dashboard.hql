
CREATE TABLE IF NOT EXISTS MON.SPARK_SMS_CEO_DASHBOARD(
    MSISDN STRING,
    SMS STRING,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


CREATE TABLE IF NOT EXISTS MON.SPARK_SMS_CEO_DASHBOARD_BACKUP(
    MSISDN STRING,
    SMS STRING,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


INSERT INTO DIM.SPARK_DT_SMSNOTIFICATION_RECIPIENT  SELECT '699942700','SMSCEODASHBOARD', CURRENT_DATE(), 'NKONGYUM Prosper AKWO', 'DRS', 'YES'