CREATE TABLE MON.SPARK_FT_MY_ORANGE (
    site_name varchar(200),
    users_backend_daily bigint,
    users_backend_since_perco_start bigint,
    new_users_backend_daily bigint,
    new_users_backend_since_perco_start bigint,
    new_users_backend_daily_percentage decimal(17, 2),
    new_users_backend_since_perco_start_percentage decimal(17, 2),
    welcome_pack_takers_global_daily bigint,
    welcome_pack_takers_global_since_perco_start bigint,
    welcome_pack_takers_new_backend_daily bigint,
    welcome_pack_takers_new_backend_since_perco_start bigint,
    welcome_pack_takers_new_backend_daily_percentage decimal(17, 2),
    welcome_pack_takers_new_backend_since_perco_start_percentage decimal(17, 2),
    welcome_pack_data_usage_daily decimal(17, 2),
    welcome_pack_data_usage_since_perco_start decimal(17, 2),
    myway_plus_takers_backend_daily bigint,
    myway_plus_takers_backend_since_perco_start bigint,
    myway_plus_subs_backend_daily bigint,
    myway_plus_subs_backend_since_perco_start bigint,
    myway_plus_revenu_backend_daily decimal(17, 2),
    myway_plus_revenu_backend_since_perco_start decimal(17, 2),
    revenu_global_backend_daily decimal(17, 2),
    revenu_global_backend_since_perco_start decimal(17, 2),
    revenu_data_backend_daily decimal(17, 2),
    revenu_data_backend_since_perco_start decimal(17, 2),
    revenu_voix_backend_daily decimal(17, 2),
    revenu_voix_backend_since_perco_start decimal(17, 2),
    usage_voix_backend_daily decimal(20, 2),
    usage_voix_backend_since_perco_start decimal(20, 2),
    usage_data_backend_daily decimal(20, 2),
    usage_data_backend_since_perco_start decimal(20, 2),
    revenu_global_subs_myorange_daily decimal(17, 2),
    revenu_global_subs_myorange_since_perco_start decimal(17, 2),
    revenu_data_subs_myorange_daily decimal(17, 2),
    revenu_data_subs_myorange_since_perco_start decimal(17, 2),
    revenu_voix_subs_myorange_daily decimal(17, 2),
    revenu_voix_subs_myorange_since_perco_start decimal(17, 2),
    active_om_users_daily bigint,
    active_om_users_since_perco_start bigint,

    users_backend_not_appeared_before_perco_daily bigint,
    users_backend_not_appeared_before_perco_since_perco_start bigint,
    users_backend_appeared_three_months_before_perco_daily bigint,
    users_backend_appeared_three_months_before_perco_since_perco_start bigint,
    users_backend_appeared_not_three_months_before_perco_daily bigint,
    users_backend_appeared_not_three_months_before_perco_since_perco_start bigint,
    welcome_pack_takers_not_appeared_before_perco_daily bigint,
    welcome_pack_takers_not_appeared_before_perco_since_perco_start bigint,
    welcome_pack_takers_appeared_three_months_before_perco_daily bigint,
    welcome_pack_takers_appeared_three_months_before_perco_since_perco_start bigint,
    welcome_pack_takers_appeared_not_three_months_before_perco_daily bigint,
    welcome_pack_takers_appeared_not_three_months_before_perco_since_perco_start bigint,
    myway_plus_takers_backend_not_appeared_before_perco_daily bigint,
    myway_plus_takers_backend_not_appeared_before_perco_since_perco_start bigint,
    myway_plus_takers_backend_appeared_three_months_before_perco_daily bigint,
    myway_plus_takers_backend_appeared_three_months_before_perco_since_perco_start bigint,
    myway_plus_takers_backend_appeared_not_three_months_before_perco_daily bigint,
    myway_plus_takers_backend_appeared_not_three_months_before_perco_since_perco_start bigint,
    users_backend_migrator_daily bigint,
    users_backend_migrator_since_perco_start bigint,
    welcome_pack_takers_migrator_daily bigint,
    welcome_pack_takers_migrator_since_perco_start bigint,
    myway_plus_takers_backend_migrator_daily bigint,
    myway_plus_takers_backend_migrator_since_perco_start bigint,

    insert_date TIMESTAMP
)
COMMENT 'Table for My Orange KPIs'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
