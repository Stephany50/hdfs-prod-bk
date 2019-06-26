
   CREATE TABLE CTI.FT_AGENT_ACTIVITY

   (
   EMPLOYE VARCHAR(255),

	FIRST_NAME VARCHAR(255),

	LAST_NAME VARCHAR(255),

	START_TIME_NQ TIMESTAMP,

	END_TIME TIMESTAMP,

	TYPE_STATE VARCHAR(32),

	NAME_STATE VARCHAR(32),

	REASON_VALUE VARCHAR(255),

	TOTAL_DURATION INT,

	INSERT_DATE TIMESTAMP,

	START_TIME_15MIN_SLICE TIMESTAMP
   )
COMMENT 'FT_AGENT_ACTIVITY - FT'
PARTITIONED BY (START_TIME    DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
