CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_ZTE_PROFILE (
    profile_id varchar(20), 
    profile_name varchar(150), 
    price_plan_code varchar(20), 
    std_code varchar(50), 
    original_file_name varchar(50),
    ORIGINAL_FILE_SIZE int,
    ORIGINAL_FILE_LINE_COUNT int 
  )
COMMENT 'Profile mapping data External Table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/PROFILE'
TBLPROPERTIES ('serialization.null.format'='')
;
