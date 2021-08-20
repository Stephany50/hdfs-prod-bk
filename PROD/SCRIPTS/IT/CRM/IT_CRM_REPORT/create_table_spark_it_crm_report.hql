CREATE TABLE cdr.spark_it_crm_reporting 
(
TicketNumber              VARCHAR(400),
onc_Numeroappelant       VARCHAR(400),
categorie           VARCHAR(400),
Date_Interaction         VARCHAR(400),
typarticle              VARCHAR(400),
article               VARCHAR(400),
motif      VARCHAR(400),
Agent         VARCHAR(400),
CUID_AGENT     VARCHAR(400),
comment_interact    VARCHAR(400),
description_ticket     VARCHAR(400),
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');