CREATE TABLE MON.SPARK_FT_ACQ_SNAP_MONTH
(
    msisdn VARCHAR(100),
    FIRST_DAY_REFILL    DATE,
    FIRST_DATE_REFILL_AMOUNT    DECIMAL(17, 2),
    REFILL_AMOUNT   DECIMAL(17, 2),
    conso_amount    DECIMAL(17, 2),
    depot_amount    DECIMAL(17, 2),
    souscriptiondata_amount DECIMAL(17, 2),
    transaction_AMOUNT  DECIMAL(17, 2),
    FIRST_MONTH_REFILL  DECIMAL(17, 2),
    FIRST_MONTH_CONSO   DECIMAL(17, 2),
    FIRST_MONTH_DEPOT   DECIMAL(17, 2),
    FIRST_MONTH_SOUSCRIPTIONDATA    DECIMAL(17, 2),
    FIRST_MONTH_TRANSACTION     DECIMAL(17, 2),
    SECOND_MONTH_REFILL     DECIMAL(17, 2),
    SECOND_MONTH_CONSO      DECIMAL(17, 2),
    SECOND_MONTH_DEPOT      DECIMAL(17, 2),
    SECOND_MONTH_SOUSCRIPTIONDATA       DECIMAL(17, 2),
    SECOND_MONTH_TRANSACTION        DECIMAL(17, 2),
    THIRD_MONTH_REFILL      DECIMAL(17, 2),
    THIRD_MONTH_CONSO       DECIMAL(17, 2),
    THIRD_MONTH_DEPOT       DECIMAL(17, 2),
    THIRD_MONTH_SOUSCRIPTIONDATA        DECIMAL(17, 2),
    THIRD_MONTH_TRANSACTION         DECIMAL(17, 2),
    INSERT_DATE timestamp
)
PARTITIONED BY (EVENT_MONTH VARCHAR(50), ACTIVATION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')