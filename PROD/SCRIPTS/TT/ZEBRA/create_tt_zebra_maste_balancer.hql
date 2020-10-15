---***********************************************************---
------------    TT ZEBRA_MASTER  -------------------
---- ARNOLD CHUENFFO 06-02-2019
---***********************************************************---

CREATE EXTERNAL TABLE CDR.tt_zebra_master_balance (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    CHANNEL_USER_ID VARCHAR(200),
    USER_NAME VARCHAR(200),
    MOBILE_NUMBER VARCHAR(200),
    CATEGORY VARCHAR(200),
    MOBILE_NUMBER_1 VARCHAR(200),
    GEOGRAPHICAL_DOMAIN VARCHAR(200),
    PRODUCT VARCHAR(200),
    PARENT_USER_NAME VARCHAR(200),
    OWNER_USER_NAME VARCHAR(200),
    AVAILABLE_BALANCE VARCHAR(200),
    AGENT_BALANCE VARCHAR(200),Bienvenido a Quicksearch

    USER_STATUS VARCHAR(200),
    MODIFIED_ON VARCHAR(200)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZEBRA/ZEB_MASTER_SNAPSHOT'
TBLPROPERTIES ('serialization.null.format'='')
;

