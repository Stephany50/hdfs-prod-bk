create table TMP.SQ_FT_A_SUBSCRIBER_SUMMARY
(
DATECODE                   DATE,          
NETWORK_DOMAIN             VARCHAR(40) , 
NETWORK_TECHNOLOGY         VARCHAR(30)  ,
SUBSCRIBER_CATEGORY        VARCHAR(30) , 
CUSTOMER_ID                VARCHAR(100), 
SUBSCRIBER_TYPE            VARCHAR(40) , 
COMMERCIAL_OFFER           VARCHAR(50) , 
ACCOUNT_STATUS             VARCHAR(15)  ,
LOCK_STATUS                VARCHAR(20)  ,
ACTIVATION_MONTH           VARCHAR(6)   ,
CITYZONE                   VARCHAR(20)  ,
USAGE_TYPE                 VARCHAR(10)  ,
TOTAL_COUNT                BIGINT  ,      
TOTAL_ACTIVATION           BIGINT   ,     
TOTAL_DEACTIVATION         BIGINT   ,     
TOTAL_EXPIRATION           BIGINT   ,     
TOTAL_PROVISIONNED         BIGINT   ,     
TOTAL_MAIN_CREDIT          DOUBLE   ,
TOTAL_PROMO_CREDIT         DOUBLE   ,
TOTAL_SMS_CREDIT           DOUBLE  ,
TOTAL_DATA_CREDIT          DOUBLE  ,
SOURCE                     VARCHAR(30)  ,
REFRESH_DATE               TIMESTAMP     ,
PROFILE_NAME               VARCHAR(100) ,
PROCESS_NAME               VARCHAR(40)  
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


DATECODE,NETWORK_DOMAIN,NETWORK_TECHNOLOGY,SUBSCRIBER_CATEGORY,CUSTOMER_ID,SUBSCRIBER_TYPE,COMMERCIAL_OFFER,ACCOUNT_STATUS,LOCK_STATUS,ACTIVATION_MONTH,CITYZONE,USAGE_TYPE,TOTAL_COUNT,TOTAL_ACTIVATION,TOTAL_DEACTIVATION,TOTAL_EXPIRATION,TOTAL_PROVISIONNED,TOTAL_MAIN_CREDIT,TOTAL_PROMO_CREDIT,TOTAL_SMS_CREDIT,TOTAL_DATA_CREDIT,SOURCE,REFRESH_DATE,PROFILE_NAME,PROCESS_NAME