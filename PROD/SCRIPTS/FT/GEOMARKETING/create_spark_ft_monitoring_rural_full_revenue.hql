CREATE TABLE MON.SPARK_FT_MONITORING_RURAL_FULL_REVENUE (
    site_name varchar(200),
    revenu_voix_paygo decimal(19, 3),
    revenu_sms_paygo decimal(19, 3),
    revenu_data_paygo decimal(19, 3),
    revenu_voix_subs decimal(19, 3),
    revenu_sms_subs decimal(19, 3),
    revenu_data_subs decimal(19, 3),
    revenu_credit_compte_desactive decimal(19, 3),
    EMERGENCY_DATA decimal(19, 3),
    revenu_vas_retail decimal(19, 3),
    revenu_p2p_voix decimal(19, 3),
    revenu_p2p_data decimal(19, 3),
    revenu_data_roaming decimal(19, 3),
    parc_group decimal(19, 3),
    charged_base decimal(19, 3),
    data_users decimal(19, 3),
    gross_add decimal(19, 3),
    gross_add_data decimal(19, 3),
    gross_add_om decimal(19, 3),
    call_box decimal(19, 3),
    pos_om decimal(19, 3),
    famoco decimal(19, 3),
    insert_date timestamp
)
COMMENT 'SPARK_FT_MONITORING_RURAL_FULL_REVENUE'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
