CREATE TABLE MON.SPARK_FT_ORANGEPRO_PU_ACTE_GESTION (
    id bigint, 
    username VARCHAR(250), 
    name VARCHAR(250), 
    account_numbers VARCHAR(250), 
    enterprise_name VARCHAR(250), 
    email VARCHAR(250), 
    social_reason VARCHAR(250), 
    last_connection TIMESTAMP,
    action VARCHAR(200), 
    details VARCHAR(500),
    management_act VARCHAR(50),
    insert_date TIMESTAMP)
PARTITIONED BY (EVENT_DATE DATE) 
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 

CREATE TABLE MON.SPARK_FT_ORANGEPRO_PU_KPI (
    social_reason VARCHAR(250),
    action VARCHAR(200),
    mgmt_act_ticket_count int,
    KPI_type VARCHAR(200),
    insert_date TIMESTAMP)
PARTITIONED BY (EVENT_DATE DATE) 
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY'); 