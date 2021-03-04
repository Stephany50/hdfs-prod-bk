create table tmp.perco_q1_2021_daily_takers
(
    msisdn varchar(100),
    is_takers_best_deal_offer_daily bigint,
    is_takers_best_deal_voice_offer_daily bigint,
    is_takers_best_deal_data_offer_daily bigint,
    is_takers_best_deal_combo_offer_daily bigint,
    is_takers_myway_plus_daily bigint,
    is_takers_myway_plus_voice_offer_daily bigint,
    is_takers_myway_plus_data_offer_daily bigint,
    is_takers_myway_plus_combo_offer_daily bigint
)
COMMENT 'Table for takers of the day in this perco'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
