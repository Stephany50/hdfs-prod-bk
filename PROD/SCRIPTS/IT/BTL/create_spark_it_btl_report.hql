CREATE TABLE cdr.spark_it_btl_report
(
msisdn              VARCHAR(400),
transaction_date       date,
type_forfait    VARCHAR(1000),
msisdn_vendeur              VARCHAR(400),
prix decimal(17, 2),
ipp VARCHAR(1000),
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

CREATE EXTERNAL TABLE cdr.tt_btl_report (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  msisdn              VARCHAR(400),
  transaction_date       date,
  type_forfait    VARCHAR(1000),
  msisdn_vendeur              VARCHAR(400)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/BTL/REPORT'
TBLPROPERTIES ('serialization.null.format'='')

ALTER TABLE cdr.tt_btl_report ADD COLUMNS (prix decimal(17, 2))
ALTER TABLE cdr.tt_btl_report ADD COLUMNS (ipp VARCHAR(1000))

CREATE EXTERNAL TABLE cdr.tt_btl_report_hour (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  msisdn              VARCHAR(400),
  transaction_date       date,
  type_forfait    VARCHAR(1000),
  msisdn_vendeur              VARCHAR(400),
  prix decimal(17, 2),
  ipp VARCHAR(1000),
  transaction_time varchar(100),
  hour_period varchar(100)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/REPORT_HOUR'
TBLPROPERTIES ('serialization.null.format'='')

CREATE TABLE cdr.spark_it_btl_report_hour
(
  msisdn              VARCHAR(400),
  transaction_date       date,
  type_forfait    VARCHAR(1000),
  msisdn_vendeur              VARCHAR(400),
  prix decimal(17, 2),
  ipp VARCHAR(1000),
  transaction_time varchar(100),
  hour_period varchar(100),
  ORIGINAL_FILE_NAME VARCHAR(50),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');