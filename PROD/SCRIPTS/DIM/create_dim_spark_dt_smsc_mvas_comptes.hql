
CREATE TABLE DIM.SPARK_DT_SMSC_MVAS_COMPTES (
    username_account varchar(20),
    application_name varchar(20),
    traffic_type varchar(10),
    connexion_point varchar(20),
    description varchar(40),
    insert_date timestamp,
    connexion_protocol varchar(20),
    application_detail varchar(50),
    application_type varchar(60)
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
