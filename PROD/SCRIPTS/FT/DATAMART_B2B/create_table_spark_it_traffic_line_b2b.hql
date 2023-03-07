CREATE TABLE CDR.SPARK_IT_TRAFFIC_LINE_B2B
(
    OrderNumber VARCHAR(200),
    Customer_Order_Number VARCHAR(500),
    Customer_Name VARCHAR(1000),
    Event_Name VARCHAR(200),
    Service_Number VARCHAR(100),
    Order_State VARCHAR(100),
    Created_Date TIMESTAMP,
    Completed_Date TIMESTAMP,
    Accept_Channel  VARCHAR(100),
    Created_By VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(200),
    INSERT_DATE TIMESTAMP

)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_TRAFFIC_LINE_B2B
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    OrderNumber VARCHAR(200),
    Customer_Order_Number VARCHAR(500),
    Customer_Name VARCHAR(1000),
    Event_Name VARCHAR(200),
    Service_Number VARCHAR(100),
    Order_State VARCHAR(100),
    Created_Date VARCHAR(100),
    Completed_Date VARCHAR(100),
    Accept_Channel  VARCHAR(100),
    Created_By VARCHAR(200)
)COMMENT 'CDR SPARK_TT_TRAFFIC_LINE_B2B'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
LOCATION '/PROD/TT/STAT_TOOLS/DATAMARTB2B/ORDER_DETAIL_DAILY'
TBLPROPERTIES ('serialization.null.format'='');