create table tmp.perco_q1_2021_incrementals
(
    site_name varchar(1000),
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
    subscriptions_myway_plus_via_om_daily bigint
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
