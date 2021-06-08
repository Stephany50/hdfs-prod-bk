CREATE TABLE CDR.SPARK_IT_SELL_CUST_DAILY
(
    Ticket_Reference VARCHAR(200),
    Article_reference VARCHAR(200),
    Description_of_the_article VARCHAR(200),
    Customer_Code VARCHAR(200),
    Customer_Name VARCHAR(200),
    Organization_Code VARCHAR(200),
    Staff_CUID VARCHAR(200),
    Staff_Name VARCHAR(200),
    Quantity_Sold VARCHAR(200),
    Quantity_Delivered VARCHAR(200),
    Amount_of_the_sale VARCHAR(200),
    Ticket_number VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (transaction_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_SELL_CUST_DAILY
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    Date_of_the_transaction DATE,
    Ticket_Reference VARCHAR(200),
    Article_reference VARCHAR(200),
    Description_of_the_article VARCHAR(200),
    Customer_Code VARCHAR(200),
    Customer_Name VARCHAR(200),
    Organization_Code VARCHAR(200),
    Staff_CUID VARCHAR(200),
    Staff_Name VARCHAR(200),
    Quantity_Sold VARCHAR(200),
    Quantity_Delivered VARCHAR(200),
    Amount_of_the_sale VARCHAR(200),
    Ticket_number VARCHAR(200)
)COMMENT 'CDR SPARK_TT_DAILY_EQUIMENT_SOLD'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
LOCATION '/PROD/TT/STAT_TOOLS/SELL_CUST_DAILY'
TBLPROPERTIES ('serialization.null.format'='');