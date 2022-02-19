CREATE TABLE SPOOL.SPOOL_RECYCLAGE_OM
(
    MSISDN VARCHAR(100),
    prod_state_date VARCHAR(100),
    prod_state_name VARCHAR(100),
    osp_account_type VARCHAR(100),
    est_present_om VARCHAR(100),
    duree_inactivite_om VARCHAR(100),
    om_balance VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
