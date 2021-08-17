create table MON.SPARK_FT_SOS_CREDIT (
loan_id               varchar(400),
loan_date             date,
msisdn                varchar(50),
loan_service          varchar(20),
loan_amount           decimal(17,2),
service_fee           decimal(17,2),
Region_administrative varchar(200),
insert_date           timestamp
)
PARTITIONED BY (EVENT_DATE date)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

