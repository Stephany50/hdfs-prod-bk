CREATE TABLE DIM.SPARK_REF_SOUSCRIPTION_PRICE (
   ipp_name varchar(250),
    ipp_code    INT,
    ipp_amount    INT ,
    create_date    string
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

