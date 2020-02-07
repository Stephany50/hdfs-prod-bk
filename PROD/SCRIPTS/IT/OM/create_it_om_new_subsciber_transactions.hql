CREATE TABLE CDR.SPARK_IT_OM_NEW_SUBSCRIBER_TRANSACTIONS

(
    DATE_NOW DATE,
    MSISDN	VARCHAR(15),
    USER_ID	VARCHAR(20),
    MOIS INT,
    DATE_CREATION_CPTE	TIMESTAMP,
    DATE_DERNIERE_ACTIVITE_OM TIMESTAMP,
    NB_JR_ACTIVITE INT,
    TRANSFER_STATUS VARCHAR(100),
    SERVICE_CHARGE_RECEIVED BIGINT,
    TRANSACTION_AMOUNT BIGINT,
    TRANSFER_ID VARCHAR(50),
    SERVICE_TYPE VARCHAR(100),




)



STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

