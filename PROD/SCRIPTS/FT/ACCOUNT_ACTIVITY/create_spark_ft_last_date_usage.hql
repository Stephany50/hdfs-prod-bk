
-- create table table MON.SPARK_TMP_LAST_DATE_USAGE_PRE
desc MON.SPARK_TMP_LAST_DATE_USAGE_PRE;
CREATE TABLE MON.SPARK_TMP_LAST_DATE_USAGE_PRE( 
MSISDN  VARCHAR(60),
last_date_telco  DATE, 
last_date_out_voice_sms  DATE, 
last_date_out_voice  DATE, 
last_date_out_sms  DATE, 
last_date_data  DATE, 
last_date_inc_voice_sms DATE, 
last_date_inc_voice  DATE, 
last_date_inc_sms  DATE, 
last_date_subscription  DATE, 
last_date_om  DATE
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

		
-- create table table MON.SPARK_FT_LAST_DATE_USAGE
desc MON.SPARK_FT_LAST_DATE_USAGE;
SHOW PARTITIONS MON.SPARK_FT_LAST_DATE_USAGE;
SELECT EVENT_DATE, INSERT_DATE, COUNT(*) from MON.SPARK_FT_LAST_DATE_USAGE where event_date >= "2022-09-13" group by 1, 2 order by 1, 2;
CREATE TABLE MON.SPARK_FT_LAST_DATE_USAGE( 
  MSISDN  VARCHAR(60),
  last_date_telco  DATE, 
  last_date_out_voice_sms  DATE, 
  last_date_out_voice  DATE, 
  last_date_out_sms  DATE, 
  last_date_data  DATE, 
  last_date_inc_voice_sms DATE, 
  last_date_inc_voice  DATE, 
  last_date_inc_sms  DATE, 
  last_date_subscription  DATE, 
  last_date_om  DATE, 
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
