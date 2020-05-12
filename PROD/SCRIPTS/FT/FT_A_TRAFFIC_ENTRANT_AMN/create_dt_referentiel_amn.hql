CREATE  TABLE DIM.DT_CI_LAC_SITE_AMN (
  CELLNAME  VARCHAR(400),
  LAC  VARCHAR(400),
  CI  VARCHAR(400),
  NUMERO_SITE  VARCHAR(400),
  ANNEE_MES  VARCHAR(100),
  MOIS_MES  VARCHAR(100),
  REGION  VARCHAR(400),
  CODESITE  VARCHAR(400),
  SITE_NAME  VARCHAR(400),
  BSC  VARCHAR(400),
  LOCALITE  VARCHAR(400),
  LONGITUDE  VARCHAR(400),
  LATITUDE  VARCHAR(400),
  PROGRAMME  VARCHAR(400),
  DETAILS  VARCHAR(400)
)
COMMENT 'external tables-TT'
 STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');