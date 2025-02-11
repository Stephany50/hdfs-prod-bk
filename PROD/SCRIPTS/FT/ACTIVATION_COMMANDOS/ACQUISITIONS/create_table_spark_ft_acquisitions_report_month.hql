CREATE TABLE MON.SPARK_FT_ACQUISITIONS_REPORT_MONTH
(
    iccid VARCHAR(400),
    msisdn_identified VARCHAR(400),
    msisdn_identificator VARCHAR(400),
    cni VARCHAR(400),
    noms_prenoms VARCHAR(400),
    contract_type VARCHAR(400),
    identification_date date,
    est_snappe VARCHAR(20),
    statut_back_office VARCHAR(200),
    statut_back_office_date date,
    activation_date date,
    LAST_REFRESH_DATE date,
    est_fake VARCHAR(20),
	FIRST_DATE_REFILL date,
	FIRST_DATE_REFILL_AMOUNT decimal(17, 2),
	pps_first_dial_date date,
	FIRST_MONTH_REFILL decimal(17, 2),
	SECOND_MONTH_REFILL decimal(17, 2),
	THIRD_MONTH_REFILL decimal(17, 2),
	FIRST_MONTH_CONSO decimal(17, 2),
	SECOND_MONTH_CONSO decimal(17, 2),
	THIRD_MONTH_CONSO decimal(17, 2),
    FIRST_MONTH_DEPOT decimal(17, 2),
	SECOND_MONTH_DEPOT decimal(17, 2),
	THIRD_MONTH_DEPOT decimal(17, 2),
	FIRST_MONTH_SOUSCRIPTIONDATA decimal(17, 2),
	SECOND_MONTH_SOUSCRIPTIONDATA decimal(17, 2),
	THIRD_MONTH_SOUSCRIPTIONDATA decimal(17, 2),
	FIRST_MONTH_TRANSACTION decimal(17, 2),
    SECOND_MONTH_TRANSACTION decimal(17, 2),
    THIRD_MONTH_TRANSACTION decimal(17, 2),
	INSCRIPTEUR_OM VARCHAR(200),
	DATE_INSCRIPTION_OM DATE,
	est_bypass varchar(50),
	categorie_site VARCHAR(200),
	site_name VARCHAR(400),
    INSERT_DATE timestamp
)
PARTITIONED BY (EVENT_MONTH VARCHAR(100))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
