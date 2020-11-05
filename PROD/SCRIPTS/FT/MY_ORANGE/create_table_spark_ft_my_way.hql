CREATE TABLE MON.SPARK_FT_MY_WAY (
    takers_global_daily bigint,
    takers_global_weekly bigint,
    takers_global_mtd bigint,
    takers_myway_plus_daily bigint,
    takers_myway_plus_weekly bigint,
    takers_myway_plus_mtd bigint,
    takers_myway_ussd_daily bigint,
    takers_myway_ussd_weekly bigint,
    takers_myway_ussd_mtd bigint,
    subs_global_daily bigint,
    subs_global_weekly bigint,
    subs_globlal_mtd bigint,
    subs_myway_plus_daily bigint,
    subs_myway_plus_weekly bigint,
    subs_myway_plus_mtd bigint,
    subs_myway_ussd_daily bigint,
    subs_myway_ussd_weekly bigint,
    subs_myway_ussd_mtd bigint,
    revenu_global_daily double,
    revenu_global_weekly double,
    revenu_globlal_mtd double,
    revenu_myway_plus_daily double,
    revenu_myway_plus_weekly double,
    revenu_myway_plus_mtd double,
    revenu_myway_ussd_daily double,
    revenu_myway_ussd_weekly double,
    revenu_myway_ussd_mtd double,
    insert_date TIMESTAMP
)
COMMENT 'Table for My Way KPIs'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
