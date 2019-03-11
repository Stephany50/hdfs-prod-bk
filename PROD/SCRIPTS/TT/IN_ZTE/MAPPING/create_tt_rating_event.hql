CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_RATING_EVENT (
    rating_event_id varchar(20),
    rating_event_name varchar(255),
    rating_event_zone varchar(255),
    rating_event_operator varchar(255),
    rating_event_service varchar(255),
    rating_event_specific_tarif varchar(25),
    rating_event_spec_tarif_desc varchar(150),
    original_file_name varchar(50),
    ORIGINAL_FILE_SIZE int,
    ORIGINAL_FILE_LINE_COUNT int
  )
COMMENT 'Rating_Event mapping data External Table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/RATING_EVENT'
TBLPROPERTIES ('serialization.null.format'='')
