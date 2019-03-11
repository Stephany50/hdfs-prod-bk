

create external table cdr.tt_log (
  filename varchar(200),
  merged_filename varchar(200),
  fluxtype varchar(50),
  provenance varchar(50),
  status varchar(50),
  log_datetime bigint,
  flowfile_attr varchar(2000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/PROD/TT/NIFI_LOGS'
