CREATE TABLE TMP.TT_STK_HIERACHY
(
  MSISDN                    VARCHAR(20),
  CATEGORY_CODE             VARCHAR(15),
  CATEGORY_NAME             VARCHAR(50),
  GEOGRAPHICAL_DOMAIN_CODE  VARCHAR(20),
  GEOGRAPHICAL_DOMAIN_NAME  VARCHAR(50),
  CHANNEL_USER_NAME         VARCHAR(80),
  PARENT                    VARCHAR(20),
  GRDPARENT                 VARCHAR(20),
  ACTIV_BEGIN_DATE          DATE
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')