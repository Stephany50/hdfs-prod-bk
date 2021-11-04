create table mon.spark_ft_sos_credit_non_conformes
(
    loan_id varchar(200),
    loan_date timestamp,
    loan_amount decimal(17, 2),
    msisdn varchar(50),
    activation_date date,
    profil varchar(400),
    main_credit decimal(17, 2),
    avg_recharge_last_three_month decimal(17, 2), 
    plafond decimal(17, 2),
    last_loan_date_sos_credit timestamp,
    last_payback_date_sos_credit timestamp,
    last_loan_amount_sos_credit decimal(17, 2),
    last_payback_amount_sos_credit decimal(17, 2),
    last_loan_id_sos_credit varchar(200),
    last_payback_id_sos_credit varchar(200),
    second_last_loan_date_sos_credit timestamp,
    second_last_payback_date_sos_credit timestamp,
    second_last_loan_amount_sos_credit decimal(17, 2),
    second_last_payback_amount_sos_credit decimal(17, 2),
    second_last_loan_id_sos_credit varchar(200),
    second_last_payback_id_sos_credit varchar(200),
    last_loan_date_sos_data timestamp,
    last_loan_amount_sos_data decimal(17, 2),
    last_payback_date_sos_data timestamp,
    last_payback_amount_sos_data decimal(17, 2),
    last_loan_id_sos_data varchar(200),
    last_loan_date_sos_voix timestamp,
    last_loan_amount_sos_voix decimal(17, 2),
    last_payback_date_sos_voix timestamp,
    last_payback_amount_sos_voix decimal(17, 2),
    last_loan_id_sos_voix varchar(200),
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUECOMPRESS'='SNAPPY')
