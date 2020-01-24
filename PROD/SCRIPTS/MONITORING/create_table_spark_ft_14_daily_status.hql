CREATE TABLE MON.SPARK_14_FT_DAILY_STATUS
(
table_name INT,
jour14 INT,
jour13  INT,
jour12 INT,
jour11 INT,
jour10 INT,
jour09 INT,
jour08 INT,
jour07 INT,
jour06 INT,
jour05 INT,
jour04 INT,
jour03 INT,
jour02 INT,
jour01 INT,
INSERT_DATE TIMESTAMP

)
COMMENT ''SPARK_14_FT_DAILY_STATUS - FT_A''
PARTITIONED BY (EVENT_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES (''PARQUET.COMPRESS''=''SNAPPY'');