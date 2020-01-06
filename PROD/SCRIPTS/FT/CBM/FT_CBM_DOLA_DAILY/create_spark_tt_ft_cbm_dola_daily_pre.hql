CREATE TABLE TMP.SPARK_TT_FT_CBM_DOLA_DAILY_PRE  (
	EVENT_DATE DATE, 
    MSISDN VARCHAR(40),
    DOLA DATE, 
    ACTIVITY_TYPE VARCHAR(80)
    ) COMMENT 'MON.TT_FT_CBM_DOLA_DAILY_PRE Table'
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
    