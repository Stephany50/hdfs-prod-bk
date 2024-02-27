CREATE TABLE CDR.SPARK_IT_DIMELO_AGENTS (
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    id varchar(10000),
    firstname varchar(10000),
    lastname varchar(10000),
    nickname varchar(10000),
    gender varchar(10000),
    email varchar(10000),
    identities varchar(10000),
    role varchar(10000),
    teams varchar(10000),
    categories varchar(10000),
    signatures varchar(10000),
    foreign_id varchar(10000),
    foreign_jwt_id varchar(10000),
    foreign_saml_id varchar(10000),
    enabled varchar(10000),
    invitation_accepted varchar(10000),
    rc_user_id varchar(10000),
    no_password varchar(10000),
    ORIGINAL_FILE_NAME VARCHAR(1000),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;



CREATE EXTERNAL TABLE CDR.SPARK_TT_DIMELO_AGENTS (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    created_at varchar(10000),
    updated_at varchar(10000),
    id varchar(10000),
    firstname varchar(10000),
    lastname varchar(10000),
    nickname varchar(10000),
    gender varchar(10000),
    email varchar(10000),
    identities varchar(10000),
    role varchar(10000),
    teams varchar(10000),
    categories varchar(10000),
    signatures varchar(10000),
    foreign_id varchar(10000),
    foreign_jwt_id varchar(10000),
    foreign_saml_id varchar(10000),
    enabled varchar(10000),
    invitation_accepted varchar(10000),
    rc_user_id varchar(10000),
    no_password varchar(10000)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/DIMELO/AGENTS'
TBLPROPERTIES ('serialization.null.format'='');