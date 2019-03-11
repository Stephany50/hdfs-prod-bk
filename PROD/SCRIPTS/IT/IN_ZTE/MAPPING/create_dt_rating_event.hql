CREATE TABLE IF NOT EXISTS DIM.DT_RATING_EVENT (
    rating_event_id varchar(20),
    rating_event_name varchar(255),
    rating_event_zone varchar(255),
    rating_event_operator varchar(255),
    rating_event_service varchar(255),
    rating_event_specific_tarif varchar(25),
    rating_event_spec_tarif_desc varchar(150),
    INSERT_DATE TIMESTAMP,
    original_file_name varchar(50),
    ORIGINAL_FILE_DATE DATE
  )STORED AS ORC
TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864");
