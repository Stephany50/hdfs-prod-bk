******** Tables TT *************************

CREATE EXTERNAL TABLE CDR.TT_DMC_GMK_2G (
PERIOD_START_TIME timestamp,
bsc_name varchar(200),
bcf_name varchar(200),
bts_name varchar(200),
dn varchar(200),
tch_availability_ratio varchar(200),
2g_nur_perweek varchar(200),
2g_nur_perday varchar(200),
erlang_traffic_carried_2g varchar(200),
grp_2g_sdcch_blocking_new1 varchar(200),
tch_blocking_rate varchar(200),
grp_2g_call_blocking varchar(200),
grp_2g_rxqualitydl varchar(200),
dl_egprs_rlc_payload varchar(200),
grp_2g_cssr_new1 varchar(200),
grp_2g_dcr_new1 varchar(200),
grp_2g_drop_cs_new varchar(200),
grp_2g_sdcch_drop_new1 varchar(200),
dl_egprs_rlc_throughput varchar(200),
ul_egprs_rlc_throughput varchar(200),
ho_ratio_dl_quality varchar(200),
ho_ratio_ul_quality varchar(200),
call_success_rate_voice_2g varchar(200),
ora_cssr_cs_new_2306 varchar(200),
ora_2g_rxqual_dl varchar(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE   DATE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/GEOMARKETING/DMC_GMK_2G'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables IT *************************
CREATE TABLE CDR.SPARK_IT_DMC_GMK_2G (
PERIOD_START_TIME timestamp,
bsc_name varchar(200),
bcf_name varchar(200),
bts_name varchar(200),
dn varchar(200),
tch_availability_ratio varchar(200),
2g_nur_perweek varchar(200),
2g_nur_perday varchar(200),
erlang_traffic_carried_2g varchar(200),
grp_2g_sdcch_blocking_new1 varchar(200),
tch_blocking_rate varchar(200),
grp_2g_call_blocking varchar(200),
grp_2g_rxqualitydl varchar(200),
dl_egprs_rlc_payload varchar(200),
grp_2g_cssr_new1 varchar(200),
grp_2g_dcr_new1 varchar(200),
grp_2g_drop_cs_new varchar(200),
grp_2g_sdcch_drop_new1 varchar(200),
dl_egprs_rlc_throughput varchar(200),
ul_egprs_rlc_throughput varchar(200),
ho_ratio_dl_quality varchar(200),
ho_ratio_ul_quality varchar(200),
call_success_rate_voice_2g varchar(200),
ora_cssr_cs_new_2306 varchar(200),
ora_2g_rxqual_dl varchar(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE   DATE
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');