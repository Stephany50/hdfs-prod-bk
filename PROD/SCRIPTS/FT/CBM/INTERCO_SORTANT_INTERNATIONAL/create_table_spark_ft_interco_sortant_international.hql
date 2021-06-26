---***********************************************************---
------------ CREATE IT Table- INTERCO SORTANT INTERNATIONALE -------------------
---***********************************************************---

CREATE TABLE MON.SPARK_FT_INTERCO_SORTANT_INTERNATIONAL (
  MSISDN VARCHAR(250),
  DESTINATION VARCHAR(250),
  MOU_INTER VARCHAR(250),
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
