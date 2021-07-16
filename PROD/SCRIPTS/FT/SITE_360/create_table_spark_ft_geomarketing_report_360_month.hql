CREATE TABLE MON.SPARK_FT_GEOMARKETING_REPORT_360_MONTH
(
    
SITE_NAME VARCHAR(1000),
TOWN VARCHAR(1000),
REGION_ADM VARCHAR(1000),
REGION_COMMERCIAL VARCHAR(1000),
KPI_NAME VARCHAR(400),
KPI_VALUE DECIMAL(17, 2),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_MONTH VARCHAR(50))
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')