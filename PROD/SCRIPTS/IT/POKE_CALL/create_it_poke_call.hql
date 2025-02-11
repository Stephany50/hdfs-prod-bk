
CREATE TABLE CDR.IT_POKE_CALL (
  CARDNUMBER VARCHAR(30),
  SERVICEKEY INT,
  CALLINGNUMBER VARCHAR(30),
  CALLEDNUMBER VARCHAR(30),
  SETUPTIME TIMESTAMP,
  TERMINATETIME TIMESTAMP,
  RESULT INT,
  CAUSEVALUE INT,
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_DATE  DATE,
  INSERT_DATE TIMESTAMP,
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT
)
PARTITIONED BY (SETUPDATE DATE)
CLUSTERED BY(CARDNUMBER) INTO 2 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")

