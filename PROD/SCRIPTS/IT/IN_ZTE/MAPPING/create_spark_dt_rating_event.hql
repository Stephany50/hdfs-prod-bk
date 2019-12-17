CREATE TABLE DIM.SPARK_DT_RATING_EVENT
(
    rating_event_id              varchar(20),
    rating_event_name            varchar(255),
    rating_event_zone            varchar(255),
    rating_event_operator        varchar(255),
    rating_event_service         varchar(255),
    rating_event_specific_tarif  varchar(25),
    rating_event_spec_tarif_desc varchar(150),
    INSERT_DATE                  TIMESTAMP,
    original_file_name           varchar(50)

)   PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
 STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')