create table MON.SPARK_FT_SPHERE_TRAFFIC_MONTH (
served_msisdn              varchar(40),
other_party                varchar(40),
duree_sortant              bigint,
nbre_tel_sortant           bigint,
duree_entrant              bigint,
nbre_tel_entrant           bigint,
nbre_sms_sortant           bigint,
nbre_sms_entrant           bigint,
dernier_jour_tel_entrant   date,
dernier_jour_tel_sortant   date,
dernier_jour_sms_entrant   date,
dernier_jour_sms_sortant   date,
premier_jour_tel_entrant   date,
premier_jour_tel_sortant   date,
premier_jour_sms_entrant   date,
premier_jour_sms_sortant   date,
operator_dest              string,
insert_date                timestamp
) COMMENT 'MON_FT_SPHERE_TRAFFIC_MONTH'
PARTITIONED BY (EVENT_MONTH VARCHAR(10))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

