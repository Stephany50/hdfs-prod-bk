******** Tables TT *************************

CREATE EXTERNAL TABLE CDR.TT_DMC_GMK_4G (
PERIOD_START_TIME timestamp,
mrbts_sbts_name   varchar(200),
lnbts_name   varchar(200),
lncel_name   varchar(200),
dn   varchar(200),
cell_avail_excl_blu   varchar(200),
cell_avail   varchar(200),
4g_nur   varchar(200),
ora_4g_call_drop_wo_volte_new   varchar(200),
ora_4g_cssr_without_volte_new   varchar(200),
ora_lte_dl_user_throughput_new   varchar(200),
ora_lte_ul_user_throughput_new   varchar(200),
trafficnewdlsran1   varchar(200),
lte_dl_ul_traffic_volume_new_mbyte   varchar(200),
4g_lte_dl_traffic_volume_new_mbites   varchar(200),
ora_4g_erab_setup_sr_new   varchar(200),
sran_oskc_average_number_of_ue_queued_dl_new   varchar(200),
perc_dl_prb_util   varchar(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE   DATE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/GEOMARKETING/DMC_GMK_4G'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables IT *************************
CREATE TABLE CDR.SPARK_IT_DMC_GMK_4G (
PERIOD_START_TIME timestamp,
mrbts_sbts_name   varchar(200),
lnbts_name   varchar(200),
lncel_name   varchar(200),
dn   varchar(200),
cell_avail_excl_blu   varchar(200),
cell_avail   varchar(200),
4g_nur   varchar(200),
ora_4g_call_drop_wo_volte_new   varchar(200),
ora_4g_cssr_without_volte_new   varchar(200),
ora_lte_dl_user_throughput_new   varchar(200),
ora_lte_ul_user_throughput_new   varchar(200),
trafficnewdlsran1   varchar(200),
lte_dl_ul_traffic_volume_new_mbyte   varchar(200),
4g_lte_dl_traffic_volume_new_mbites   varchar(200),
ora_4g_erab_setup_sr_new   varchar(200),
sran_oskc_average_number_of_ue_queued_dl_new   varchar(200),
perc_dl_prb_util   varchar(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE   DATE
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');