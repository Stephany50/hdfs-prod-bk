CREATE   TABLE   MON.SPARK_FT_ACTIVATION_BY_IDENTIF_DAY   (
   TABLE_NAME   VARCHAR(40),
   NQ_SDATE   TIMESTAMP,
   HEURE   VARCHAR(2),
   USAGE_APPEL   VARCHAR(40),
   TYPE_APPEL   VARCHAR(56),
   TYPE_ABONNE   VARCHAR(56),
   TYPE_HEURE   VARCHAR(56),
   REC_TYPE   VARCHAR(40),
   CALLER_SUBR   VARCHAR(40),
   PARTNER_ID   VARCHAR(40),
   PARTNER_GT   VARCHAR(40),
   CRA_SRC   VARCHAR(20),
   BASIC_SERV   VARCHAR(40),
   PARTNER_GT_LEN   BIGINT,
   PARTNER_ID_LEN   BIGINT,
   CALLER_SUBR_LEN   BIGINT,
   PARTNER_ID_PREFIX   VARCHAR(15),
   PARTNER_GT_PREFIX   VARCHAR(15),
   TRUNCK_OUT   VARCHAR(50),
   TRUNCK_IN   VARCHAR(50),
   DURATION   INT,
   CRA_COUNT   BIGINT,
   INSERTED_DATE   TIMESTAMP,
   CALLEDNUMBER   VARCHAR(40),
   CALLINGNUMBER   VARCHAR(40),
   SERVICECENTRE   VARCHAR(40),
   SERVICECENTRE_PREFIX   VARCHAR(10),
   SERVICECENTRE_LEN   INT,
   OPERATOR_CODE   VARCHAR(25),
   MSC_LOCATION   VARCHAR(200)

)   COMMENT   'FT AG INTERCO'
PARTITIONED   BY   (SDATE   DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
