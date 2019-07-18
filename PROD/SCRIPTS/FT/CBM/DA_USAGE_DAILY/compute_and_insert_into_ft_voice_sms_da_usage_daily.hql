create table MON.FT_VOICE_SMS_DA_USAGE_DAILY
(
    MSISDN                 VARCHAR(50),
    DA_NAME                VARCHAR(4000),
    DA_UNIT                VARCHAR(4000),
    DA_TYPE                VARCHAR(4000),
    POSITIVE_CHARGE_AMOUNT DOUBLE,
    NEGATIVE_CHARGE_AMOUNT DOUBLE,
    OPERATOR_CODE          VARCHAR(30),
    COMMERCIAL_PROFILE     VARCHAR(50),
    TARIFF_PLAN            VARCHAR(50),
    OTHER_PARTY_ZONE       VARCHAR(50),
    DESTINATION            VARCHAR(50),
    USAGE                  VARCHAR(50),
    SOURCE_TABLE           VARCHAR(50),
    INSERT_DATE            TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")