
CREATE TABLE TMP.TT_DIM_STK_HIERACHY
(
  MSISDN                    VARCHAR(20),
  CATEGORY_CODE             VARCHAR(15),
  CATEGORY_NAME             VARCHAR(50),
  GEOGRAPHICAL_DOMAIN_CODE  VARCHAR(20),
  GEOGRAPHICAL_DOMAIN_NAME  VARCHAR(50),
  CHANNEL_USER_NAME         VARCHAR(80),
  PARENT                    VARCHAR(20),
  GRDPARENT                 VARCHAR(20),
  ACTIV_BEGIN_DATE          DATE,
  ACTIV_END_DATE            DATE,
  STATUS                    VARCHAR(50),
  LAST_UPDATE               DATE,
  TMP_STATUS                VARCHAR(50),
  LAST_EVENT_DATE           DATE,
  PARTNER_NAME              VARCHAR(100),
  TDW                       VARCHAR(50),
  TDW_COCI                  VARCHAR(50),
  COMMENTAIRE               VARCHAR(100),
  ACTIV_END_TMP             DATE
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')