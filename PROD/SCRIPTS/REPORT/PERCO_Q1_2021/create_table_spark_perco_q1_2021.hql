create TABLE mon.spark_ft_perco_q1_2021
(
    site_name varchar(200),
    region_name varchar(200),
    users_backend_daily bigint,
    users_backend_mtd bigint,
    takers_best_deal_offer_daily bigint,
    takers_best_deal_offer_mtd bigint,
    takers_best_deal_voice_offer_daily bigint,
    takers_best_deal_voice_offer_mtd bigint,
    takers_best_deal_data_offer_daily bigint,
    takers_best_deal_data_offer_mtd bigint,
    takers_best_deal_combo_offer_daily bigint,
    takers_best_deal_combo_offer_mtd bigint,
    takers_myway_plus_daily bigint,
    takers_myway_plus_mtd bigint,
    takers_myway_plus_voice_offer_daily bigint,
    takers_myway_plus_voice_offer_mtd bigint,
    takers_myway_plus_data_offer_daily bigint,
    takers_myway_plus_data_offer_mtd bigint,
    takers_myway_plus_combo_offer_daily bigint,
    takers_myway_plus_combo_offer_mtd bigint,
    takers_myway_plus_via_om_daily bigint,
    takers_myway_plus_via_om_mtd bigint,
    subscriptions_best_deal_offer_daily bigint,
    subscriptions_best_deal_voice_offer_daily bigint,
    subscriptions_best_deal_data_offer_daily bigint,
    subscriptions_best_deal_combo_offer_daily bigint,
    subscriptions_myway_plus_daily bigint,
    subscriptions_myway_plus_voice_offer_daily bigint,
    subscriptions_myway_plus_data_offer_daily bigint,
    subscriptions_myway_plus_combo_offer_daily bigint,
    subscriptions_myway_plus_via_om_daily bigint,
    revenu_best_deal_offer_daily decimal(20,3),
    revenu_best_deal_voice_offer_daily decimal(20,3),
    revenu_best_deal_data_offer_daily decimal(20,3),
    revenu_myway_plus_daily decimal(20,3),
    revenu_myway_plus_voice_offer_daily decimal(20,3),
    revenu_myway_plus_data_offer_daily decimal(20,3),
    revenu_myway_plus_via_om_daily decimal(20,3),
    takers_best_deal_offer_daily_perco bigint,
    takers_best_deal_offer_mtd_perco bigint,
    takers_best_deal_voice_offer_daily_perco bigint,
    takers_best_deal_voice_offer_mtd_perco bigint,
    takers_best_deal_data_offer_daily_perco bigint,
    takers_best_deal_data_offer_mtd_perco bigint,
    takers_best_deal_combo_offer_daily_perco bigint,
    takers_best_deal_combo_offer_mtd_perco bigint,
    subscriptions_best_deal_offer_daily_perco bigint,
    subscriptions_best_deal_voice_offer_daily_perco bigint,
    subscriptions_best_deal_data_offer_daily_perco bigint,
    subscriptions_best_deal_combo_offer_daily_perco bigint,
    revenu_best_deal_offer_daily_perco decimal(20,3),
    revenu_best_deal_voice_offer_daily_perco decimal(20,3),
    revenu_best_deal_data_offer_daily_perco decimal(20,3),
    insert_date timestamp
)
COMMENT 'Table for PERCO Q1 2021'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');



--Staging table in DataLake
CREATE TABLE TMP.SQ_FT_PERCO_Q1_2021
(
    site_name varchar(200),
    region_name varchar(200),
    users_backend_dly decimal(20,3),
    users_backend_mtd decimal(20,3),
    tkrs_bst_dl_ofr_dly decimal(20,3),
    tkrs_bst_dl_ofr_mtd decimal(20,3),
    tkrs_bst_dl_voice_ofr_dly decimal(20,3),
    tkrs_bst_dl_voice_ofr_mtd decimal(20,3),
    tkrs_bst_dl_data_ofr_dly decimal(20,3),
    tkrs_bst_dl_data_ofr_mtd decimal(20,3),
    tkrs_bst_dl_combo_ofr_dly decimal(20,3),
    tkrs_bst_dl_combo_ofr_mtd decimal(20,3),
    tkrs_mw_pls_dly decimal(20,3),
    tkrs_mw_pls_mtd decimal(20,3),
    tkrs_mw_pls_voice_ofr_dly decimal(20,3),
    tkrs_mw_pls_voice_ofr_mtd decimal(20,3),
    tkrs_mw_pls_data_ofr_dly decimal(20,3),
    tkrs_mw_pls_data_ofr_mtd decimal(20,3),
    tkrs_mw_pls_combo_ofr_dly decimal(20,3),
    tkrs_mw_pls_combo_ofr_mtd decimal(20,3),
    tkrs_mw_pls_via_om_dly decimal(20,3),
    tkrs_mw_pls_via_om_mtd decimal(20,3),
    subs_bst_dl_ofr_dly decimal(20,3),
    subs_bst_dl_voice_ofr_dly decimal(20,3),
    subs_bst_dl_data_ofr_dly decimal(20,3),
    subs_bst_dl_combo_ofr_dly decimal(20,3),
    subs_mw_pls_dly decimal(20,3),
    subs_mw_pls_voice_ofr_dly decimal(20,3),
    subs_mw_pls_data_ofr_dly decimal(20,3),
    subs_mw_pls_combo_ofr_dly decimal(20,3),
    subs_mw_pls_via_om_dly decimal(20,3),
    rvn_bst_dl_ofr_dly decimal(20,3),
    rvn_bst_dl_voice_ofr_dly decimal(20,3),
    rvn_bst_dl_data_ofr_dly decimal(20,3),
    rvn_mw_pls_dly decimal(20,3),
    rvn_mw_pls_voice_ofr_dly decimal(20,3),
    rvn_mw_pls_data_ofr_dly decimal(20,3),
    rvn_mw_pls_via_om_dly decimal(20,3),
    tkrs_bst_dl_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_voice_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_voice_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_data_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_data_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_combo_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_combo_ofr_mtd_prc decimal(20,3),
    subs_bst_dl_ofr_dly_prc decimal(20,3),
    subs_bst_dl_voice_ofr_dly_prc decimal(20,3),
    subs_bst_dl_data_ofr_dly_prc decimal(20,3),
    subs_bst_dl_combo_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_voice_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_data_ofr_dly_prc decimal(20,3),
    insert_date timestamp,
    event_date date
);



--Staging table in DWH
CREATE TABLE MON.SQ_FT_PERCO_Q1_2021
(
    site_name varchar(200),
    region_name varchar(200),
    users_backend_dly decimal(20,3),
    users_backend_mtd decimal(20,3),
    tkrs_bst_dl_ofr_dly decimal(20,3),
    tkrs_bst_dl_ofr_mtd decimal(20,3),
    tkrs_bst_dl_voice_ofr_dly decimal(20,3),
    tkrs_bst_dl_voice_ofr_mtd decimal(20,3),
    tkrs_bst_dl_data_ofr_dly decimal(20,3),
    tkrs_bst_dl_data_ofr_mtd decimal(20,3),
    tkrs_bst_dl_combo_ofr_dly decimal(20,3),
    tkrs_bst_dl_combo_ofr_mtd decimal(20,3),
    tkrs_mw_pls_dly decimal(20,3),
    tkrs_mw_pls_mtd decimal(20,3),
    tkrs_mw_pls_voice_ofr_dly decimal(20,3),
    tkrs_mw_pls_voice_ofr_mtd decimal(20,3),
    tkrs_mw_pls_data_ofr_dly decimal(20,3),
    tkrs_mw_pls_data_ofr_mtd decimal(20,3),
    tkrs_mw_pls_combo_ofr_dly decimal(20,3),
    tkrs_mw_pls_combo_ofr_mtd decimal(20,3),
    tkrs_mw_pls_via_om_dly decimal(20,3),
    tkrs_mw_pls_via_om_mtd decimal(20,3),
    subs_bst_dl_ofr_dly decimal(20,3),
    subs_bst_dl_voice_ofr_dly decimal(20,3),
    subs_bst_dl_data_ofr_dly decimal(20,3),
    subs_bst_dl_combo_ofr_dly decimal(20,3),
    subs_mw_pls_dly decimal(20,3),
    subs_mw_pls_voice_ofr_dly decimal(20,3),
    subs_mw_pls_data_ofr_dly decimal(20,3),
    subs_mw_pls_combo_ofr_dly decimal(20,3),
    subs_mw_pls_via_om_dly decimal(20,3),
    rvn_bst_dl_ofr_dly decimal(20,3),
    rvn_bst_dl_voice_ofr_dly decimal(20,3),
    rvn_bst_dl_data_ofr_dly decimal(20,3),
    rvn_mw_pls_dly decimal(20,3),
    rvn_mw_pls_voice_ofr_dly decimal(20,3),
    rvn_mw_pls_data_ofr_dly decimal(20,3),
    rvn_mw_pls_via_om_dly decimal(20,3),
    tkrs_bst_dl_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_voice_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_voice_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_data_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_data_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_combo_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_combo_ofr_mtd_prc decimal(20,3),
    subs_bst_dl_ofr_dly_prc decimal(20,3),
    subs_bst_dl_voice_ofr_dly_prc decimal(20,3),
    subs_bst_dl_data_ofr_dly_prc decimal(20,3),
    subs_bst_dl_combo_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_voice_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_data_ofr_dly_prc decimal(20,3),
    insert_date timestamp,
    event_date date
)


--final table in DWH
CREATE TABLE MON.TMP_FT_PERCO_Q1_2021
(
    site_name varchar(200),
    region_name varchar(200),
    users_backend_dly decimal(20,3),
    users_backend_mtd decimal(20,3),
    tkrs_bst_dl_ofr_dly decimal(20,3),
    tkrs_bst_dl_ofr_mtd decimal(20,3),
    tkrs_bst_dl_voice_ofr_dly decimal(20,3),
    tkrs_bst_dl_voice_ofr_mtd decimal(20,3),
    tkrs_bst_dl_data_ofr_dly decimal(20,3),
    tkrs_bst_dl_data_ofr_mtd decimal(20,3),
    tkrs_bst_dl_combo_ofr_dly decimal(20,3),
    tkrs_bst_dl_combo_ofr_mtd decimal(20,3),
    tkrs_mw_pls_dly decimal(20,3),
    tkrs_mw_pls_mtd decimal(20,3),
    tkrs_mw_pls_voice_ofr_dly decimal(20,3),
    tkrs_mw_pls_voice_ofr_mtd decimal(20,3),
    tkrs_mw_pls_data_ofr_dly decimal(20,3),
    tkrs_mw_pls_data_ofr_mtd decimal(20,3),
    tkrs_mw_pls_combo_ofr_dly decimal(20,3),
    tkrs_mw_pls_combo_ofr_mtd decimal(20,3),
    tkrs_mw_pls_via_om_dly decimal(20,3),
    tkrs_mw_pls_via_om_mtd decimal(20,3),
    subs_bst_dl_ofr_dly decimal(20,3),
    subs_bst_dl_voice_ofr_dly decimal(20,3),
    subs_bst_dl_data_ofr_dly decimal(20,3),
    subs_bst_dl_combo_ofr_dly decimal(20,3),
    subs_mw_pls_dly decimal(20,3),
    subs_mw_pls_voice_ofr_dly decimal(20,3),
    subs_mw_pls_data_ofr_dly decimal(20,3),
    subs_mw_pls_combo_ofr_dly decimal(20,3),
    subs_mw_pls_via_om_dly decimal(20,3),
    rvn_bst_dl_ofr_dly decimal(20,3),
    rvn_bst_dl_voice_ofr_dly decimal(20,3),
    rvn_bst_dl_data_ofr_dly decimal(20,3),
    rvn_mw_pls_dly decimal(20,3),
    rvn_mw_pls_voice_ofr_dly decimal(20,3),
    rvn_mw_pls_data_ofr_dly decimal(20,3),
    rvn_mw_pls_via_om_dly decimal(20,3),
    tkrs_bst_dl_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_voice_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_voice_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_data_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_data_ofr_mtd_prc decimal(20,3),
    tkrs_bst_dl_combo_ofr_dly_prc decimal(20,3),
    tkrs_bst_dl_combo_ofr_mtd_prc decimal(20,3),
    subs_bst_dl_ofr_dly_prc decimal(20,3),
    subs_bst_dl_voice_ofr_dly_prc decimal(20,3),
    subs_bst_dl_data_ofr_dly_prc decimal(20,3),
    subs_bst_dl_combo_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_voice_ofr_dly_prc decimal(20,3),
    rvn_bst_dl_data_ofr_dly_prc decimal(20,3),
    insert_date timestamp,
    event_date date
)




DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.TMP_FT_PERCO_Q1_2021';
  MIN_DATE_PARTITION := '20210101';
  MAX_DATE_PARTITION := '20210601';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_PERCO_Q1_2021';
  PART_PARTITION_NAME := 'FT_PERCO_Q1_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_MIG_64K';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;