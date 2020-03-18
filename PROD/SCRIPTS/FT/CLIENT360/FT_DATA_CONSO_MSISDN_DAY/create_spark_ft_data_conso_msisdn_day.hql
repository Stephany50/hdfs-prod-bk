create table MON.SPARK_FT_DATA_CONSO_MSISDN_DAY
(
    MSISDN                        VARCHAR(50),
    COMMERCIAL_OFFER              VARCHAR(250),
    BYTES_SENT                    BIGINT,
    BYTES_RECEIVED                BIGINT,
    MMS_COUNT                     BIGINT,
    MAIN_RATED_AMOUNT             DOUBLE,
    PROMO_RATED_AMOUNT            DOUBLE,
    SERVED_PARTY_IMSI             VARCHAR(50),
    SERVED_PARTY_IMEI             VARCHAR(50),
    BYTES_USED_IN_BUNDLE          BIGINT,
    BYTES_USED_OUT_BUNDLE         BIGINT,
    BYTES_USED_IN_BUNDLE_ROAMING  BIGINT,
    BYTES_USED_OUT_BUNDLE_ROAMING BIGINT,
    BUNDLE_MMS_USED_VOLUME        BIGINT,
    MAIN_RATED_AMOUNT_ROAMING     DOUBLE,
    PROMO_RATED_AMOUNT_ROAMING    DOUBLE,
    GOS_DEBIT_COUNT               BIGINT,
    GOS_SESSION_COUNT             BIGINT,
    GOS_REFUND_COUNT              BIGINT,
    GOS_DEBIT_AMOUNT              DOUBLE,
    GOS_SESSION_AMOUNT            DOUBLE,
    GOS_REFUND_AMOUNT             DOUBLE,
    BUNDLE_BYTES_REMAINING_VOLUME BIGINT,
    BUNDLE_MMS_REMAINING_VOLUME   BIGINT,
    SOURCE_TABLE                  VARCHAR(250),
    OPERATOR_CODE                 VARCHAR(250),
    INSERT_DATE                   TIMESTAMP,
    THROTTLING_VOL                BIGINT
)
PARTITIONED BY (EVENT_DATE  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


