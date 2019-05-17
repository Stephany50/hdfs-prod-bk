CREATE EXTERNAL TABLE IF NOT EXISTS CTI.tt_media_type (

  MEDIA_TYPE_KEY        VARCHAR(10),
  MEDIA_NAME            VARCHAR(64),
  MEDIA_NAME_CODE       VARCHAR(32),
  IS_ONLINE             VARCHAR(64),
  CREATE_AUDIT_KEY      VARCHAR(32),
  UPDATE_AUDIT_KEY      VARCHAR(19),
  ORIGINAL_FILE_NAME    VARCHAR(200),
  ORIGINAL_FILE_SIZE    INT,
  ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CTI external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_MEDIA_TYPE'
TBLPROPERTIES ('serialization.null.format'='')
;