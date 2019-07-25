-- auto-generated definition
create table MON.FT_DATA_DA_USAGE_DAILY_TMP
(
    MSISDN                 VARCHAR(50),
    DA_NAME                VARCHAR(4000),
    DA_UNIT                VARCHAR(4000),
    DA_TYPE                VARCHAR(4000),
    POSITIVE_CHARGE_AMOUNT DOUBLE,
    NEGATIVE_CHARGE_AMOUNT DOUBLE,
    OPERATOR_CODE          VARCHAR(30),
    CONTRACT_TYPE          VARCHAR(50),
    COMMERCIAL_PROFILE     VARCHAR(50),
    TARIFF_PLAN            VARCHAR(50),
    TYPE_OF_MEASUREMENT    VARCHAR(150),
    SERVICE_TYPE           VARCHAR(50),
    SERVICE_ZONE           VARCHAR(150),
    SDP_GOS_SERVICE        VARCHAR(150),
    SOURCE_TABLE           VARCHAR(50),
    INSERT_DATE            TIMESTAMP
)
PARTITIONED BY (SESSION_DATE DATE)
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")