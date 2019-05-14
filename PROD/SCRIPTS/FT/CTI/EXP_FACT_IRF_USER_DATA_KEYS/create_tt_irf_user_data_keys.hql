
CREATE EXTERNAL TABLE IF NOT EXISTS CTI.FT_IRF_USER_DATA_KEYS (
   INTERACTION_RESOURCE_ID          VARCHAR(100),       
   START_DATE_TIME_KEY              VARCHAR(100),       
   TENANT_KEY                       VARCHAR(100),       
   INTERACTION_DESCRIPTOR_KEY       VARCHAR(100),       
   CUSTOM_KEY_1                     VARCHAR(100),       
   CUSTOM_KEY_2                     VARCHAR(100),       
   CUSTOM_KEY_3                     VARCHAR(100),       
   CUSTOM_KEY_4                     VARCHAR(100),       
   CUSTOM_KEY_5                     VARCHAR(100),       
   ORIGINAL_FILE_NAME               VARCHAR(50),
   ORIGINAL_FILE_SIZE INT,
   ORIGINAL_FILE_LINE_COUNT INT
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;
