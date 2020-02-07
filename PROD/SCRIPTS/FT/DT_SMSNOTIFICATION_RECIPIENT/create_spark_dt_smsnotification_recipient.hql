CREATE TABLE DIM.SPARK_DT_SMSNOTIFICATION_RECIPIENT (
    msisdn varchar(12),
    type varchar(50),
    inserted_date string,
    full_name varchar(200),
    direction varchar(200) ,
    actif varchar(10)
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


