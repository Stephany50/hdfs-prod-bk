CREATE TABLE MON.SPARK_TF_DASHBRD_CMO_DATA_REVM
(
  ADMINISTRATIVE_REGION           VARCHAR(60),
  COMMERCIAL_REGION               VARCHAR(60),
  TOWNNAME                        VARCHAR(60),
  SITE_NAME                       VARCHAR(60),
  CONSTRUCTOR_HANDSET             VARCHAR(60),
  DATA_COMPATIBLE_HANDSET         VARCHAR(60),
  TOTAL_DATA_DOER                 DOUBLE,
  TOTAL_REV_PROVIDER              DOUBLE,
  TOTAL_ROAM_REV_PROVIDER         DOUBLE,
  TOTAL_GOSSDP_REV_PROVIDER       DOUBLE,
  TOTAL_BYTE_RECEIVER             DOUBLE,
  TOTAL_BYTE_SENDER               DOUBLE,
  TOTAL_BYTE_USER_IN_BUNDLE       DOUBLE,
  TOTAL_BYTE_USER_OUT_BUNDLE      DOUBLE,
  TOTAL_BYTE_USER_IN_BUNDLEROAM   DOUBLE,
  TOTAL_BYTE_USER_OUT_BUNDLEROAM  DOUBLE,
  TOTAL_MMS_USER                  DOUBLE,
  TOTAL_MMS_USER_IN_BUNDLE        DOUBLE,
  MAIN_CONSO                      DOUBLE,
  PROMO_CONSO                     DOUBLE,
  ROAM_MAIN_CONSO                 DOUBLE,
  ROAM_PROMO_CONSO                DOUBLE,
  BYTES_RECEIVED                  DOUBLE,
  BYTES_SENT                      DOUBLE,
  BYTES_USED_IN_BUNDLE            DOUBLE,
  BYTES_USED_OUT_BUNDLE           DOUBLE,
  BYTES_USED_IN_BUNDLE_ROAMING    DOUBLE,
  BYTES_USED_OUT_BUNDLE_ROAMING   DOUBLE,
  MMS_COUNT                       DOUBLE,
  BUNDLE_MMS_USED_VOLUME          DOUBLE,
  GOS_DEBIT_COUNT                 DOUBLE,
  GOS_DEBIT_AMOUNT                DOUBLE,
  INSERT_DATE                     TIMESTAMP
) COMMENT 'TF_DASHBRD_CMO_DATA_REVM table '
    PARTITIONED BY (EVENT_MONTH VARCHAR(10)
    STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")
