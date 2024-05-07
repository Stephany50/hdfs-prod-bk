******** Tables TT *************************

CREATE EXTERNAL TABLE CDR.TT_DMC_GMK_3G (
PERIOD_START_TIME timestamp,
plmn_name   varchar(200),
rnc_name   varchar(200),
wbts_name   varchar(200),
wbts_id   varchar(200),
wcel_name   varchar(200),
wcel_id   varchar(200),
dn   varchar(200),
grp_cell_availability   varchar(200),
3g_nur   varchar(200),
grp_traffic_speech_erlangs   varchar(200),
grp_total_data_traffic_vol_dlul   varchar(200),
grp_3g_drop_call_cs   varchar(200),
ora_cssr_cs_cellpch_urapchnew   varchar(200),
ora_cssr_ps_cellpch_urapchnew   varchar(200),
ora_hsupa_user_throughput_new   varchar(200),
ora_hsdpa_user_throughput_new   varchar(200),
grp_number_of_users_queued   varchar(200),
cs_drop_nb   varchar(200),
hs_dsch_credit_reductions_due_to_frame_loss   varchar(200),
ora_3g_traffic_speech   varchar(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE   DATE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/GEOMARKETING/DMC_GMK_3G'
TBLPROPERTIES ('serialization.null.format'='');


******** Tables IT *************************
CREATE TABLE CDR.SPARK_IT_DMC_GMK_3G (
PERIOD_START_TIME timestamp,
plmn_name   varchar(200),
rnc_name   varchar(200),
wbts_name   varchar(200),
wbts_id   varchar(200),
wcel_name   varchar(200),
wcel_id   varchar(200),
dn   varchar(200),
grp_cell_availability   varchar(200),
3g_nur   varchar(200),
grp_traffic_speech_erlangs   varchar(200),
grp_total_data_traffic_vol_dlul   varchar(200),
grp_3g_drop_call_cs   varchar(200),
ora_cssr_cs_cellpch_urapchnew   varchar(200),
ora_cssr_ps_cellpch_urapchnew   varchar(200),
ora_hsupa_user_throughput_new   varchar(200),
ora_hsdpa_user_throughput_new   varchar(200),
grp_number_of_users_queued   varchar(200),
cs_drop_nb   varchar(200),
hs_dsch_credit_reductions_due_to_frame_loss   varchar(200),
ora_3g_traffic_speech   varchar(200),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE   DATE
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');