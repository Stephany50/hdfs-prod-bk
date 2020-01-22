create table MON.SPARK_TT_EMERGENCY_CREDIT_ACTIVITY
(
    MSISDN                     VARCHAR(50),
    ACCOUNT_STATUS             VARCHAR(250),
    ACCOUNT_PROFILE            VARCHAR(250),
    ACTIVATION_DATE            TIMESTAMP,
    PROVISION_DATE             TIMESTAMP,
    RESILIATION_DATE           TIMESTAMP,
    MAIN_REMAINING_CREDIT      DOUBLE,
    PROMO_REMAINING_CREDIT     INT,
    TOTAL_REIMBOURSMENT_AMOUNT DOUBLE,
    TOTAL_REIMBOURSMENT_COUNT  INT,
    TOTAL_LOAN_AMOUNT          DOUBLE,
    TOTAL_LOAN_COUNT           INT,
    TOTAL_PAYED_FEE_AMOUNT     DOUBLE,
    TOTAL_PAYED_FEE_COUNT      INT,
    DAILY_REIMBOURSMENT_AMOUNT DOUBLE,
    DAILY_REIMBOURSMENT_COUNT  INT,
    DAILY_LOAN_AMOUNT          DOUBLE,
    DAILY_LOAN_COUNT           INT,
    DAILY_PAYED_FEE_AMOUNT     DOUBLE,
    DAILY_PAYED_FEE_COUNT      INT,
    LOAN_AMOUNT                DOUBLE,
    LOAN_TO_PAY                DOUBLE,
    LOAN_DATE                  TIMESTAMP,
    LAST_PAYMENT_DATE          TIMESTAMP,
    DUE_FEE_AMOUNT             DOUBLE,
    ACTIVITY_BEGIN_DATE        TIMESTAMP,
    SOURCE_PLATFORM            VARCHAR(250),
    SOURCE_DATA                VARCHAR(250),
    INSERT_DATE                TIMESTAMP,
    EVENT_DATE                 DATE

)STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')