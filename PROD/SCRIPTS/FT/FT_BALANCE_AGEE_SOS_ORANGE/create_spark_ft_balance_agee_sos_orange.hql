create table MON.SPARK_FT_BALANCE_AGEE_SOS_ORANGE (
msisdn               varchar(40),
sos_type             varchar(40),
region_administrative varchar(50),
statut          varchar(50),
Age           INT,
INSERT_DATE DATE
)
PARTITIONED BY (EVENT_DATE date)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')



