
CREATE TABLE cdr.it_log (
  filename varchar(200),
  filesize bigint,
  filecount bigint,
  filetype varchar(200),
  merged_filename varchar(200),
  fluxtype varchar(50),
  provenance varchar(50),
  status varchar(50),
  log_datetime TIMESTAMP,
  flowfile_attr varchar(2000),
  insert_date TIMESTAMP
)
PARTITIONED BY (LOG_DATE DATE)
CLUSTERED BY(fluxtype,provenance, status) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")

