CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_ZTE_PROD_SPEC_EXTRACT (
  PROD_SPEC_ID	INT,
  PROD_SPEC_TYPE	VARCHAR (200),
  PROD_SPEC_NAME	VARCHAR (200),
  COMMENTS	VARCHAR (200),
  STD_CODE	VARCHAR (200),
  EFF_DATE	VARCHAR (20),
  EXP_DATE	VARCHAR (20),
  STATE	VARCHAR (200),
  STATE_DATE	VARCHAR (20),
  SP_ID	INT,
  ORIGINAL_FILE_NAME VARCHAR(100),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'Prod Spec Extract tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/EXTRACT_PROD_SPEC'
