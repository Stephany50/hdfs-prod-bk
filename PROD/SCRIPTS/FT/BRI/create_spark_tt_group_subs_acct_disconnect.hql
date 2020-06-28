CREATE TABLE MON.SPARK_TT_GROUP_SUBS_ACCT_DISCONNECT (

    MSISDN                  VARCHAR(25),
    OG_CALL                 DATE,
    IC_CALL_1               DATE,
    IC_CALL_2               DATE,
    IC_CALL_3               DATE,
    IC_CALL_4               DATE,
    STATUS                  VARCHAR(4),
    GP_STATUS               VARCHAR(5),
    GP_STATUS_DATE          DATE,
    GP_FIRST_ACTIVE_DATE    DATE,
    ACTIVATION_DATE         DATE,
    RESILIATION_DATE        DATE,
    PROVISION_DATE          DATE,
    FORMULE                 VARCHAR(50),
    PLATFORM_STATUS         VARCHAR(20),
    REMAIN_CREDIT_MAIN      DOUBLE,
    REMAIN_CREDIT_PROMO     DOUBLE,
    LANGUAGE_ACC            VARCHAR(15),
    SRC_TABLE               VARCHAR(40),
    CONTRACT_ID             DOUBLE,
    CUSTOMER_ID             DOUBLE,
    ACCOUNT_ID              VARCHAR(25),
    LOGIN                   VARCHAR(25),
    ICC_COMM_OFFER          VARCHAR(50),
    BSCS_COMM_OFFER         VARCHAR(100),
    BSCS_STATUS             VARCHAR(20),
    OSP_ACCOUNT_TYPE        VARCHAR(15),
    CUST_GROUP              VARCHAR(50),
    CUST_BILLCYCLE          VARCHAR(50),
    BSCS_STATUS_DATE        DATE,
    INACTIVITY_BEGIN_DATE   DATE,
    COMGP_STATUS            VARCHAR(10),
    COMGP_STATUS_DATE       DATE,
    COMGP_FIRST_ACTIVE_DATE DATE,
    INSERT_DATE TIMESTAMP

) COMMENT 'SPARK_TT_GROUP_SUBS_ACCT_DISCONNECT table'
PARTITIONED BY (EVENT_DATE DATE)
CLUSTERED BY(MSISDN) INTO 64 BUCKETS STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');