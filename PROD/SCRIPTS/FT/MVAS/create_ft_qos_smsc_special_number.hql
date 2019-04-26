CREATE TABLE MON.FT_QOS_SMSC_SPECIAL_NUMBER (
    SRC_TABLE    VARCHAR(30),
    SRC_NUM    VARCHAR(30),
    DEST_NUM    VARCHAR(30),
    TRANSACTION_DATE    DATE,
    ENTRY_DATE    DATE,
    SM_STATE    VARCHAR(20),
    DEST_OPERATOR    VARCHAR(40),
    SRC_OPERATOR    VARCHAR(40),
    SRC_MSC_OPERATOR    VARCHAR(40),
    DEST_MSC_OPERATOR    VARCHAR(40),
    SRC_EI_TYPE    VARCHAR(25),
    DEST_EI_TYPE    VARCHAR(25),
    NOTIF_IND    VARCHAR(40),
    DELIVERY_SLICEID    VARCHAR(40),
    CRA_COUNT    INT,
    INSERT_DATE    TIMESTAMP,
    BILLS_PLATFORM    VARCHAR(25),
    SRC_EI    VARCHAR(50),
    DEST_EI    VARCHAR(50),
    BILL_DATE    DATE
)
COMMENT 'qos smsc special number - FT'
PARTITIONED BY (STATE_DATE DATE)
STORED AS ORC TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")
