CREATE EXTERNAL TABLE CDR.tt_log (

  FILENAME  VARCHAR(200),
  FILESIZE  BIGINT,
  FILECOUNT  BIGINT,
  FILETYPE  VARCHAR(200),
  MERGED_FILENAME  VARCHAR(200),
  FLUXTYPE  VARCHAR(50),
  PROVENANCE  VARCHAR(50),
  STATUS  VARCHAR(50),
  LOG_DATETIME  BIGINT,
  FLOWFILE_ATTR  VARCHAR(2000))
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/NIFI_LOGS'
TBLPROPERTIES ('serialization.null.format'='')
;